"""Prove that frames really travel by reference: gateway -> pipeline -> maia-infer worker.

Three separate processes besides the gateway: a maia-infer broker, a worker that reports
what it received, and this script reading a camera's frames from the frame bus. For each
frame it sends one request *by reference* and one *with the pixels*, then compares:

- what the worker got (a read-only view of shared memory vs. an array it received);
- that both describe the same pixels (a checksum computed by the worker and by this script);
- how many bytes went over the wire, and the round trip.

    maia-gateway serve --config examples/cameras-local.yml      # or docker compose up
    python examples/check_by_reference.py --camera synth-1       # needs maia-infer installed

Workers in Docker read host rings only with MAIA_IPC=host (maia-infer's compose file);
otherwise the client falls back to pixels, and this script shows that too.
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import statistics
import time

import numpy as np

FRONT, BACK = "tcp://127.0.0.1:5591", "tcp://127.0.0.1:5592"


def checksum(a: np.ndarray) -> int:
    return int(a[::16, ::16].astype(np.int64).sum())


def run_broker() -> None:
    from maia_infer import Broker

    Broker(FRONT, BACK.replace("127.0.0.1", "*")).run()


def run_worker() -> None:
    from maia_infer import Result, Worker

    class Inspect(Worker):
        service = "inspect"

        def predict(self, request):
            a = request.arrays[0]
            return Result(
                meta={
                    "by_reference": bool(request.frame_refs),
                    "read_only": not a.flags.writeable,
                    "owns_memory": bool(a.flags.owndata),
                    "shape": list(a.shape),
                    "checksum": checksum(a),
                    "worker_pid": os.getpid(),
                }
            )

    Inspect(broker=BACK).run()


def wire_bytes(request) -> int:
    return sum(len(memoryview(part).cast("B")) for part in request.encode())


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--camera", default="synth-1")
    ap.add_argument("--redis", default="redis://localhost:6379/0")
    ap.add_argument("-n", type=int, default=20, help="frames to check")
    args = ap.parse_args()

    from maia_infer import Client, StaleFrameError
    from maia_infer import protocol as p

    from maia_gateway.bus import FrameStream

    ctx = mp.get_context("spawn")
    procs = [ctx.Process(target=run_broker, daemon=True), ctx.Process(target=run_worker, daemon=True)]
    for proc in procs:
        proc.start()
    time.sleep(1.5)
    client = Client(FRONT, timeout_ms=5000)
    frames = FrameStream(args.camera, args.redis)
    rows = {"reference": [], "pixels": []}
    print(f"camera {args.camera}: this script pid {os.getpid()}, worker pid {procs[1].pid}\n")
    try:
        for frame in frames.follow(idle_timeout_s=10):
            mine = checksum(frame.array)
            try:
                t0 = time.perf_counter()
                by_ref = client.infer("inspect", frame)
                t1 = time.perf_counter()
                pixels = np.ascontiguousarray(frame.array)  # what a client without the ring sends
                by_px = client.infer("inspect", pixels)
                t2 = time.perf_counter()
            except StaleFrameError:
                continue
            if not frame.still_valid():
                continue
            ref_request = p.Request(service="inspect", frame_refs=[frame.ref.to_dict()])
            rows["reference"].append((by_ref.meta, (t1 - t0) * 1000, wire_bytes(ref_request), mine))
            rows["pixels"].append((by_px.meta, (t2 - t1) * 1000, wire_bytes(p.Request("inspect", [pixels])), mine))
            if len(rows["pixels"]) >= args.n:
                break
    finally:
        for proc in procs:
            proc.terminate()
    if not rows["pixels"]:
        raise SystemExit(f"no frames from camera {args.camera}: is the gateway running and the camera up?")

    print(f"{'sent as':<11}{'worker got':<26}{'same pixels':<13}{'bytes sent':>12}{'p50 ms':>9}")
    for mode, data in rows.items():
        metas = [m for m, *_ in data]
        same = all(m["checksum"] == mine for m, _, _, mine in data)
        got = "read-only shared view" if all(m["read_only"] and not m["owns_memory"] for m in metas) else (
            "its own copy" if all(not m["by_reference"] for m in metas) else "mixed")  # fmt: skip
        p50 = statistics.median(ms for _, ms, _, _ in data)
        print(f"{mode:<11}{got:<26}{'yes' if same else 'NO':<13}{data[0][2]:>12,}{p50:>9.2f}")
    shape = rows["pixels"][0][0]["shape"]
    by_ref = rows["reference"][0][0]["by_reference"]
    print(f"\n{len(rows['pixels'])} frames of {shape}; worker read them by reference: {by_ref}")
    if not by_ref:
        print("The worker could not open the ring and the client resent pixels (another machine, or a")
        print("container without shared memory). Same machine and same IPC namespace: references.")


if __name__ == "__main__":
    main()
