"""SKETCH (not tested against a Triton server yet): send ring frames to Triton without copying.

Triton can read a request's input straight from a shared-memory region the client has
registered. A maia-ring ring *is* such a region: its slots are page-aligned, so a frame
is just (ring segment, byte offset, byte size). The client registers the camera's ring
once, then each request names a slot instead of carrying pixels.

Requirements:
- Triton on the same machine, sharing its IPC namespace (``--ipc=host``).
- A model whose input is the raw frame: uint8, HxWx3 (BGR). Put YOLO's resize and
  normalisation in a Triton ensemble (Python backend or DALI) in front of the network.
- ``pip install tritonclient[grpc]``

    python examples/triton_shm_sketch.py --camera sim-1 --model yolo_ensemble
"""

import argparse

import tritonclient.grpc as grpcclient
from maia_ring import RingReader, StaleFrame

from maia_gateway.bus import FrameStream
from maia_gateway.config import ring_name


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--camera", default="sim-1")
    ap.add_argument("--model", default="yolo_ensemble")
    ap.add_argument("--input", default="IMAGE")
    ap.add_argument("--triton", default="localhost:8001")
    args = ap.parse_args()

    triton = grpcclient.InferenceServerClient(args.triton)
    ring = RingReader(ring_name(args.camera))
    g = ring.geometry
    region = f"ring-{args.camera}-{ring.instance}"
    # the whole segment, once; every slot is then an offset into it
    triton.register_system_shared_memory(region, f"/{ring.name}", g.total_bytes)

    for ref in FrameStream(args.camera).follow(read=False):  # references only: nothing is read here
        if ref.instance != ring.instance:
            break  # the gateway restarted the camera: re-run to register the new ring
        inp = grpcclient.InferInput(args.input, list(ref.shape), "UINT8")
        inp.set_shared_memory(region, g.frame_bytes, offset=g.slot_offset(ref.slot))
        result = triton.infer(args.model, [inp])
        try:
            ring.get(ref, copy=False)  # the slot still holds this frame (seqlock check)?
        except StaleFrame:
            continue  # overwritten while Triton read it: discard this result
        print(ref.frame_index, {o.name: result.as_numpy(o.name).shape for o in result.get_response().outputs})

    triton.unregister_system_shared_memory(region)


if __name__ == "__main__":
    main()
