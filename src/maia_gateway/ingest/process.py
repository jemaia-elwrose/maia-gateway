"""Entry point of a camera's own process: pipeline + Redis + clips + control commands.

One process per camera, so a camera that crashes or stalls never takes the others down;
the supervisor restarts it.
"""

from __future__ import annotations

import json
import logging
import threading
import time

from .. import bus
from ..config import CameraProfile

log = logging.getLogger("maia_gateway.camera")

EXIT_OK, EXIT_FAILED = 0, 1


def run_camera(camera_id: str, profile_json: str, url: str, redis_url: str, clips_dir: str, gpu: int | None) -> int:
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s %(levelname)s [{camera_id}] %(message)s")
    from .clips import ClipRecorder  # imported here: GStreamer loads in the camera process only
    from .pipeline import Ingest

    profile = CameraProfile.model_validate_json(profile_json)
    r = bus.connect(redis_url)
    publisher = bus.FramePublisher(r, camera_id)
    recorder: ClipRecorder | None = None

    def clip_done(clip) -> None:
        r.xadd(bus.CLIPS, {"camera": camera_id, "id": clip.id, "status": clip.status}, maxlen=10000)

    def on_packet(packet) -> None:
        recorder.add(packet)

    ingest = Ingest(camera_id, profile, url=url, publish=publisher.publish, on_packet=on_packet, gpu=gpu)
    recorder = ClipRecorder(
        camera_id,
        clips_dir,
        profile.clips.pre_s,
        profile.clips.post_s,
        caps=lambda: ingest.caps_string,
        on_done=clip_done,
    )
    stop = threading.Event()

    def control() -> None:
        key, last = bus.CONTROL.format(camera_id), "$"
        while not stop.is_set():
            try:
                batch = r.xread({key: last}, block=1000)
            except Exception as exc:  # Redis restarting: keep the video going
                log.warning("control channel: %s", exc)
                time.sleep(1)
                continue
            for entry_id, fields in batch[0][1] if batch else []:
                last = entry_id
                args = json.loads(fields.get("args") or "{}")
                reply: dict = {"ok": True}
                if fields.get("cmd") == "clip":
                    clip = recorder.event(at_ns=args.pop("at_ns", None), label=args.pop("label", ""), **args)
                    reply.update(clip_id=clip.id, start_ns=clip.start_ns, end_ns=clip.end_ns)
                else:
                    reply = {"ok": False, "error": f"unknown command {fields.get('cmd')}"}
                r.rpush(bus.REPLY.format(fields["id"]), json.dumps(reply))
                r.expire(bus.REPLY.format(fields["id"]), 30)

    def report() -> None:
        while not stop.is_set():
            try:
                r.set(bus.STATS.format(camera_id), json.dumps(ingest.stats.as_dict()), ex=10)
            except Exception:
                pass
            stop.wait(1.0)

    threads = [threading.Thread(target=f, daemon=True) for f in (control, report)]
    for t in threads:
        t.start()
    state = ingest.run()
    stop.set()
    recorder.flush()
    try:
        r.set(bus.STATS.format(camera_id), json.dumps(ingest.stats.as_dict()), ex=60)
    except Exception:
        pass
    log.info("camera stopped: %s%s", state, f" ({ingest.stats.error})" if ingest.stats.error else "")
    return EXIT_OK if state in ("stopped", "ended") else EXIT_FAILED
