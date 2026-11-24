"""Redis side of the frame bus: one stream per camera announcing every frame in its ring.

Stream ``frames:<camera>``, one entry per frame (the maia-ring FrameRef fields). Pipelines
read it with :class:`FrameStream`:

    frames = FrameStream("gate-1")
    for frame in frames.follow():          # newest frame each time, skipping what it missed
        boxes = client.infer("yolo", frame)  # by reference: the worker reads the ring in place

    for frame in frames.follow(group="counting", consumer="a"):  # replicas share the frames
        ...

Other keys: ``ctl:<camera>`` (commands to a camera's process), ``clips`` (finished clips),
``gw:stats:<camera>`` (live stats), ``gw:cameras`` (the camera registry).
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator

import redis
from maia_ring import Frame, FrameRef, RingPool, StaleFrame

FRAMES = "frames:{}"
CONTROL = "ctl:{}"
CLIPS = "clips"
STATS = "gw:stats:{}"
REGISTRY = "gw:cameras"
REPLY = "gw:reply:{}"
STREAM_MAXLEN = 1000


def connect(url: str) -> redis.Redis:
    return redis.Redis.from_url(url, decode_responses=True)


def ref_fields(ref: FrameRef) -> dict[str, str]:
    d = ref.to_dict()
    d["shape"] = ",".join(map(str, ref.shape))
    return {k: str(v) for k, v in d.items()}


def ref_from_fields(f: dict[str, str]) -> FrameRef:
    return FrameRef(
        ring=f["ring"],
        instance=int(f["instance"]),
        slot=int(f["slot"]),
        frame_index=int(f["frame_index"]),
        seq=int(f["seq"]),
        shape=tuple(int(x) for x in f["shape"].split(",")),
        pixfmt=f["pixfmt"],
        timestamp_ns=int(f["timestamp_ns"]),
        dtype=f.get("dtype", "uint8"),
    )


class FramePublisher:
    def __init__(self, r: redis.Redis, camera_id: str, maxlen: int = STREAM_MAXLEN) -> None:
        self.r, self.key, self.maxlen = r, FRAMES.format(camera_id), maxlen

    def publish(self, ref: FrameRef) -> None:
        self.r.xadd(self.key, ref_fields(ref), maxlen=self.maxlen, approximate=True)


class FrameStream:
    """Read a camera's frames. Yields maia-ring ``Frame`` objects (views by default: no copy;
    check ``frame.still_valid()`` after using one) or, with ``read=False``, just ``FrameRef``s
    to forward to a worker."""

    def __init__(self, camera_id: str, redis_url: str = "redis://localhost:6379/0", r: redis.Redis | None = None):
        self.camera_id = camera_id
        self.key = FRAMES.format(camera_id)
        self.r = r or connect(redis_url)
        self.rings = RingPool()
        self.missed = 0  # frames that were overwritten before they could be read
        self.stale = 0

    def _resolve(self, fields: dict[str, str], read: bool, copy: bool) -> Frame | FrameRef | None:
        ref = ref_from_fields(fields)
        if not read:
            return ref
        try:
            return self.rings.get(ref, copy=copy)
        except (StaleFrame, FileNotFoundError):
            self.stale += 1
            return None

    def latest(self, copy: bool = True, read: bool = True) -> Frame | FrameRef | None:
        for _, fields in self.r.xrevrange(self.key, count=3):
            item = self._resolve(fields, read, copy)
            if item is not None:
                return item
        return None

    def follow(
        self,
        every: bool = False,
        group: str | None = None,
        consumer: str | None = None,
        copy: bool = False,
        read: bool = True,
        block_ms: int = 2000,
        idle_timeout_s: float | None = None,
    ) -> Iterator[Frame | FrameRef]:
        """Yield frames as they arrive.

        - default: the newest frame each time; frames that arrived meanwhile are skipped.
        - ``every=True``: every frame in order; frames already overwritten in the ring are
          counted in ``missed`` instead.
        - ``group=...``: a Redis consumer group, so replicas of one pipeline split the frames.

        The stream ends when the camera does (a file played once: ``ended``; a source that
        cannot work: ``failed``), or after ``idle_timeout_s`` without frames.
        """
        last_id = "$"
        if group:
            try:
                self.r.xgroup_create(self.key, group, id="$", mkstream=True)
            except redis.ResponseError as exc:
                if "BUSYGROUP" not in str(exc):
                    raise
            consumer = consumer or f"c{time.time_ns()}"
        idle_since = time.monotonic()
        while True:
            if group:
                batch = self.r.xreadgroup(group, consumer, {self.key: ">"}, count=32, block=block_ms)
            else:
                batch = self.r.xread({self.key: last_id}, count=None if every else 1000, block=block_ms)
            entries = batch[0][1] if batch else []
            if not entries:
                if idle_timeout_s is not None and time.monotonic() - idle_since > idle_timeout_s:
                    return
                if self.camera_state() in ("ended", "failed"):
                    return
                continue
            idle_since = time.monotonic()
            last_id = entries[-1][0]
            if group:
                self.r.xack(self.key, group, *[e[0] for e in entries])
            chosen = entries if (every or group) else entries[-1:]
            if not (every or group):
                self.missed += len(entries) - 1
            for _, fields in chosen:
                item = self._resolve(fields, read, copy)
                if item is None:
                    self.missed += 1
                    continue
                yield item

    def camera_state(self) -> str | None:
        """The camera's state as its ingest process last reported it (None if unknown)."""
        raw = self.r.get(STATS.format(self.camera_id))
        return json.loads(raw).get("state") if raw else None

    def close(self) -> None:
        self.rings.close()


def send_command(r: redis.Redis, camera_id: str, cmd: str, wait_s: float = 0.0, **args) -> dict | None:
    """Send a command to a camera's ingest process; optionally wait for its reply."""
    request_id = f"{time.time_ns()}"
    r.xadd(CONTROL.format(camera_id), {"cmd": cmd, "id": request_id, "args": json.dumps(args)}, maxlen=100)
    if wait_s <= 0:
        return None
    reply = r.blpop([REPLY.format(request_id)], timeout=wait_s)
    return json.loads(reply[1]) if reply else None
