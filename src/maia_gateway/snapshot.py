"""JPEG encoding of a frame from a ring, with GStreamer's jpegenc (no extra imaging library)."""

from __future__ import annotations

import threading

import numpy as np

_FORMATS = {"bgr": "BGR", "rgb": "RGB", "gray": "GRAY8"}
_lock = threading.Lock()


def _padded_rows(frame: np.ndarray) -> np.ndarray:
    """GStreamer raw video rows are padded to 4 bytes (a 606-pixel BGR row takes 1820, not 1818)."""
    rows = frame.reshape(frame.shape[0], -1)
    pad = -rows.shape[1] % 4
    return np.ascontiguousarray(np.pad(rows, ((0, 0), (0, pad))) if pad else rows)


def jpeg(frame: np.ndarray, pixfmt: str, quality: int = 85) -> bytes:
    from .ingest.gst import Gst

    h, w = frame.shape[:2]
    caps = f"video/x-raw,format={_FORMATS[pixfmt]},width={w},height={h},framerate=0/1"
    with _lock:
        pipeline = Gst.parse_launch(
            f"appsrc name=src caps={caps} ! videoconvert ! jpegenc quality={quality} ! appsink name=out sync=false"
        )
        src, out = pipeline.get_by_name("src"), pipeline.get_by_name("out")
        pipeline.set_state(Gst.State.PLAYING)
        src.emit("push-buffer", Gst.Buffer.new_wrapped(_padded_rows(frame).tobytes()))
        src.emit("end-of-stream")
        sample = out.emit("try-pull-sample", 5 * Gst.SECOND)
        pipeline.set_state(Gst.State.NULL)
    if sample is None:
        raise RuntimeError("JPEG encoding failed")
    buf = sample.get_buffer()
    return buf.extract_dup(0, buf.get_size())
