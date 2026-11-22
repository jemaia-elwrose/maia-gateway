"""Event clips cut from the encoded stream, without re-encoding.

The ingest keeps the last ``pre_s`` seconds of encoded packets, always starting at a key
frame. An event opens a clip from that buffer, keeps collecting for ``post_s`` seconds, and
then muxes the packets into an MP4. An event that arrives while a clip is still open
extends it instead of starting a second file.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from collections import deque
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path

from .gst import Gst
from .pipeline import Packet

log = logging.getLogger("maia_gateway.clips")
NONE = Gst.CLOCK_TIME_NONE


class PacketBuffer:
    """The newest ``pre_s`` seconds of packets, trimmed so it always starts at a key frame."""

    def __init__(self, pre_s: float) -> None:
        self.pre_ns = int(pre_s * 1e9)
        self.packets: deque[Packet] = deque()

    def append(self, packet: Packet) -> None:
        if not self.packets and not packet.key:
            return  # nothing useful until the first key frame
        self.packets.append(packet)
        cutoff = packet.arrival_ns - self.pre_ns
        while True:  # drop whole GOPs while the next GOP still covers the window
            nxt = next((i for i, p in enumerate(self.packets) if i and p.key), None)
            if nxt is None or self.packets[nxt].arrival_ns > cutoff:
                break
            for _ in range(nxt):
                self.packets.popleft()

    def since(self, start_ns: int) -> list[Packet]:
        """Packets from the last key frame at or before ``start_ns`` (or the oldest one)."""
        packets = list(self.packets)
        first = 0
        for i, p in enumerate(packets):
            if p.key and p.arrival_ns <= start_ns:
                first = i
        return packets[first:]


@dataclass
class Clip:
    id: str
    camera: str
    path: str
    events: list[dict]
    start_ns: int
    end_ns: int
    packets: int = 0
    bytes: int = 0
    status: str = "recording"  # recording | done | failed
    error: str | None = None

    def sidecar(self) -> dict:
        return asdict(self)


@dataclass
class _Open:
    clip: Clip
    packets: list[Packet] = field(default_factory=list)


class ClipRecorder:
    """Feed it every packet (``add``); call ``event`` to request a clip."""

    def __init__(
        self,
        camera_id: str,
        directory: str | Path,
        pre_s: float,
        post_s: float,
        caps: Callable[[], str | None],
        on_done: Callable[[Clip], None] | None = None,
    ) -> None:
        self.camera_id = camera_id
        self.directory = Path(directory) / camera_id
        self.post_ns = int(post_s * 1e9)
        self.pre_ns = int(pre_s * 1e9)
        self.buffer = PacketBuffer(pre_s)
        self.caps = caps
        self.on_done = on_done or (lambda clip: None)
        self._open: _Open | None = None
        self._lock = threading.Lock()

    def add(self, packet: Packet) -> None:
        with self._lock:
            self.buffer.append(packet)
            job = self._open
            if job is None:
                return
            job.packets.append(packet)
            if packet.arrival_ns < job.clip.end_ns:
                return
            self._open = None
        threading.Thread(target=self._finish, args=(job,), name=f"clip-{job.clip.id}", daemon=True).start()

    def event(self, at_ns: int | None = None, label: str = "", **meta) -> Clip:
        """Request a clip around ``at_ns`` (wall clock, default now). Returns the clip being
        recorded; it is written to disk ``post_s`` seconds later."""
        at_ns = at_ns or time.time_ns()
        record = {"at_ns": at_ns, "label": label, **meta}
        with self._lock:
            if self._open is not None and at_ns - self.pre_ns <= self._open.clip.end_ns:
                clip = self._open.clip
                clip.end_ns = max(clip.end_ns, at_ns + self.post_ns)
                clip.events.append(record)
                return clip
            packets = self.buffer.since(at_ns - self.pre_ns)
            start = packets[0].arrival_ns if packets else at_ns
            stamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime(at_ns / 1e9))
            clip_id = f"{stamp}-{uuid.uuid4().hex[:6]}"
            clip = Clip(
                id=clip_id,
                camera=self.camera_id,
                path=str(self.directory / f"{clip_id}.mp4"),
                events=[record],
                start_ns=start,
                end_ns=at_ns + self.post_ns,
            )
            self._open = _Open(clip, packets)
        return clip

    def flush(self) -> None:
        """Write any open clip now (camera stopping)."""
        with self._lock:
            job, self._open = self._open, None
        if job is not None:
            self._finish(job)

    def _finish(self, job: _Open) -> None:
        clip = job.clip
        self.directory.mkdir(parents=True, exist_ok=True)
        try:
            if not job.packets:
                raise RuntimeError("no video was received for this clip")
            caps = self.caps()
            if caps is None:
                raise RuntimeError("stream format unknown")
            write_mp4(job.packets, caps, clip.path)
            clip.packets = len(job.packets)
            clip.bytes = Path(clip.path).stat().st_size
            clip.status = "done"
        except Exception as exc:
            clip.status, clip.error = "failed", str(exc)
            log.error("[%s] clip %s failed: %s", self.camera_id, clip.id, exc)
        Path(clip.path).with_suffix(".json").write_text(json.dumps(clip.sidecar(), indent=2))
        self.on_done(clip)


def write_mp4(packets: list[Packet], caps: str, path: str, timeout_s: float = 30.0) -> None:
    """Mux encoded access units into an MP4 (no re-encode)."""
    codec = "h265" if "x-h265" in caps else "h264"
    pipeline = Gst.parse_launch(
        f"appsrc name=src format=time ! {codec}parse ! mp4mux faststart=true ! filesink name=out"
    )
    src = pipeline.get_by_name("src")
    src.set_property("caps", Gst.Caps.from_string(caps))
    pipeline.get_by_name("out").set_property("location", path)
    pipeline.set_state(Gst.State.PLAYING)
    base = _first_valid(packets, "dts") or _first_valid(packets, "pts")
    for i, p in enumerate(packets):
        buf = Gst.Buffer.new_wrapped(p.data)
        if base is not None and p.pts != NONE:
            buf.pts = p.pts - base
            buf.dts = (p.dts - base) if p.dts != NONE else buf.pts
        else:  # no stream timestamps: fall back to arrival times
            buf.pts = buf.dts = p.arrival_ns - packets[0].arrival_ns
        buf.duration = p.duration
        if not p.key:
            buf.set_flags(Gst.BufferFlags.DELTA_UNIT)
        if src.emit("push-buffer", buf) != Gst.FlowReturn.OK:
            raise RuntimeError(f"muxer refused packet {i}")
    src.emit("end-of-stream")
    msg = pipeline.get_bus().timed_pop_filtered(int(timeout_s * 1e9), Gst.MessageType.EOS | Gst.MessageType.ERROR)
    pipeline.set_state(Gst.State.NULL)
    if msg is None:
        raise RuntimeError("muxing timed out")
    if msg.type == Gst.MessageType.ERROR:
        raise RuntimeError(msg.parse_error()[0].message)


def _first_valid(packets: list[Packet], attr: str) -> int | None:
    return next((getattr(p, attr) for p in packets if getattr(p, attr) != NONE), None)
