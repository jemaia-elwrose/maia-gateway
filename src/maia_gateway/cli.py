"""maia-gateway command line.

maia-gateway serve --config cameras.yml        # API on :8080, cameras from the file
maia-gateway cameras                           # list cameras (through the API)
maia-gateway cameras add gate-1 rtsp://10.0.0.21/stream1 --fps 5
maia-gateway cameras rm gate-1
maia-gateway event gate-1 --label person       # cut a clip around now
maia-gateway decoders                          # what this machine can decode with
maia-gateway bench                             # decode throughput per decoder
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time

import httpx


def _api(args) -> httpx.Client:
    headers = {"X-API-Key": os.environ["MAIA_API_KEY"]} if os.environ.get("MAIA_API_KEY") else {}
    return httpx.Client(base_url=args.api, headers=headers, timeout=15)


def _show(res: httpx.Response) -> None:
    if res.status_code >= 400:
        sys.exit(f"{res.status_code}: {res.json().get('detail', res.text) if res.content else ''}")
    if res.content:
        print(json.dumps(res.json(), indent=2))


class _Counter:
    def __init__(self) -> None:
        self.n = 0

    def __call__(self, *args) -> None:
        self.n += 1


def bench(seconds: float, size: str, codecs: list[str]) -> list[dict]:
    """Decode throughput: frames/s each available decoder sustains on a synthetic stream."""
    from .ingest import decoders
    from .ingest.gst import Gst
    from .ingest.pipeline import TEST_ENCODERS

    width, height = (int(x) for x in size.split("x"))
    rows = []
    encoder = next((e for e in TEST_ENCODERS if decoders.available(e)), None)
    if encoder is None:
        sys.exit("bench needs an H.264 encoder (x264enc, openh264enc or vtenc_h264) to make test video")
    # 1. encode a test clip once, so only decoding is timed
    path = f"/tmp/maia-bench-{width}x{height}.h264"
    if not os.path.exists(path):
        enc = Gst.parse_launch(
            f"videotestsrc num-buffers=300 pattern=smpte ! video/x-raw,width={width},height={height},framerate=30/1 "
            f"! videoconvert ! {encoder} ! h264parse ! video/x-h264,stream-format=byte-stream "
            f"! filesink location={path}"
        )
        enc.set_state(Gst.State.PLAYING)
        enc.get_bus().timed_pop_filtered(120 * Gst.SECOND, Gst.MessageType.EOS | Gst.MessageType.ERROR)
        enc.set_state(Gst.State.NULL)
    for kind in decoders.AUTO_ORDER:
        for codec in codecs:
            element = next((e for e in decoders.DECODERS[kind].get(codec, []) if decoders.available(e)), None)
            if element is None or codec != "h264":
                continue
            pipe = Gst.parse_launch(
                f"multifilesrc location={path} loop=true ! h264parse ! {element} ! videoconvert ! "
                f"video/x-raw,format=BGR ! fakesink name=sink sync=false signal-handoffs=true"
            )
            counter = _Counter()
            pipe.get_by_name("sink").connect("handoff", counter)
            pipe.set_state(Gst.State.PLAYING)
            time.sleep(1.0)  # warm up
            start, t0 = counter.n, time.perf_counter()
            time.sleep(seconds)
            fps = (counter.n - start) / (time.perf_counter() - t0)
            pipe.set_state(Gst.State.NULL)
            rows.append({"decoder": element, "kind": kind, "codec": codec, "size": size, "fps": round(fps, 1)})
    return rows


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(
        prog="maia-gateway", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--api", default=os.environ.get("MAIA_API", "http://localhost:8080"))
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("serve", help="run the gateway (API + camera processes)")
    s.add_argument("--config", help="YAML file with cameras to add or update on start")
    s.add_argument("--host", default="0.0.0.0")
    s.add_argument("--port", type=int, default=8080)
    c = sub.add_parser("cameras", help="list, add or remove cameras")
    csub = c.add_subparsers(dest="action")
    add = csub.add_parser("add")
    add.add_argument("id")
    add.add_argument("source")
    add.add_argument("--fps", type=float, default=5)
    add.add_argument("--mode", choices=["fps", "iframes", "motion"], default="fps")
    add.add_argument("--resize", help="WxH, e.g. 1280x720")
    rm = csub.add_parser("rm")
    rm.add_argument("id")
    e = sub.add_parser("event", help="raise an event: cuts a clip around now")
    e.add_argument("id")
    e.add_argument("--label", default="manual")
    sub.add_parser("decoders", help="decoders available on this machine")
    b = sub.add_parser("bench", help="decode throughput per available decoder")
    b.add_argument("--seconds", type=float, default=5)
    b.add_argument("--size", default="1920x1080")
    args = ap.parse_args(argv)

    if args.cmd == "serve":
        import uvicorn

        from .api.app import create_app
        from .config import GatewayConfig

        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
        cameras = GatewayConfig.load(args.config).cameras if args.config else None
        uvicorn.run(create_app(cameras=cameras), host=args.host, port=args.port, log_level="info")
    elif args.cmd == "cameras":
        with _api(args) as api:
            if args.action == "add":
                profile: dict = {"source": args.source, "sampling": {"mode": args.mode, "fps": args.fps}}
                if args.resize:
                    profile["steps"] = [{"resize": [int(x) for x in args.resize.split("x")]}]
                _show(api.post("/cameras", json={"id": args.id, "profile": profile}))
            elif args.action == "rm":
                _show(api.delete(f"/cameras/{args.id}"))
            else:
                res = api.get("/cameras")
                if res.status_code >= 400:
                    _show(res)
                print(f"{'camera':<16}{'state':<12}{'decoder':<14}{'size':<11}{'fps':>6}{'gop s':>7}  source")
                for cam in res.json():
                    st = cam["stats"]
                    size = f"{st.get('width')}x{st.get('height')}" if st.get("width") else "-"
                    print(
                        f"{cam['id']:<16}{cam['state']:<12}{st.get('decoder') or '-':<14}{size:<11}"
                        f"{st.get('fps_out') or 0:>6}{st.get('gop_s') or '-':>7}  {cam['source']}"
                    )
    elif args.cmd == "event":
        with _api(args) as api:
            _show(api.post(f"/cameras/{args.id}/events", json={"label": args.label}))
    elif args.cmd == "decoders":
        from .ingest.decoders import report

        print(json.dumps(report(), indent=2))
    elif args.cmd == "bench":
        rows = bench(args.seconds, args.size, ["h264"])
        print(f"{'decoder':<14}{'kind':<8}{'size':<11}{'frames/s':>9}")
        for r in rows:
            print(f"{r['decoder']:<14}{r['kind']:<8}{r['size']:<11}{r['fps']:>9}")


if __name__ == "__main__":
    main()
