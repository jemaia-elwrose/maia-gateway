"""A minimal analytics pipeline on top of the gateway: read a camera's newest frame from the
frame bus, run YOLO on it through maia-infer *by reference* (the worker reads the frame
in place, no pixels are copied), and raise an event, which cuts a clip, when a person
appears.

    docker compose up                                   # the gateway stack (or maia-gateway serve)
    maia-infer broker &  maia-infer worker yolo &       # on the same machine
    python examples/detect_from_bus.py --camera sim-1 --label person

Replace the event logic with your own use case; the reading and inference stay the same.
"""

import argparse
import time

import httpx
from maia_infer import Client, StaleFrameError, detections

from maia_gateway.bus import FrameStream


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--camera", default="sim-1")
    ap.add_argument("--label", default="person", help="class that raises an event")
    ap.add_argument("--redis", default="redis://localhost:6379/0")
    ap.add_argument("--api", default="http://localhost:8080")
    ap.add_argument("--infer", default="tcp://localhost:5555")
    ap.add_argument("--cooldown", type=float, default=30, help="seconds between events")
    args = ap.parse_args()

    frames = FrameStream(args.camera, args.redis)
    client = Client(args.infer, timeout_ms=3000)
    api = httpx.Client(base_url=args.api, timeout=5)
    last_event, n, t0 = 0.0, 0, time.time()

    for frame in frames.follow():  # newest frame each time: a slow model skips, never lags
        try:
            res = client.infer("yolo", frame, meta={"conf": 0.4})
        except StaleFrameError:
            continue  # the ring moved on while the frame was queued: take the next one
        found = [d for d in detections(res) if d["name"] == args.label]
        n += 1
        if found and time.time() - last_event > args.cooldown:
            event = api.post(
                f"/cameras/{args.camera}/events",
                json={"label": args.label, "at_ns": frame.meta.timestamp_ns, "meta": {"count": len(found)}},
            ).json()
            print(f"{len(found)} x {args.label} -> clip {event.get('clip_id')}")
            last_event = time.time()
        if n % 50 == 0:
            print(f"{n / (time.time() - t0):.1f} frames/s analysed, {frames.missed} skipped to stay live")


if __name__ == "__main__":
    main()
