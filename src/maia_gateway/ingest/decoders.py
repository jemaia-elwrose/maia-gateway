"""Pick a hardware decoder when the machine has one, else the CPU decoder.

GStreamer only registers NVIDIA's nvcodec elements when a usable GPU and driver are present,
and Apple's VideoToolbox elements only on macOS, so "is the element there" is the test.
"""

from __future__ import annotations

from .gst import Gst

DECODERS = {
    # preference -> codec -> candidate elements, best first
    "nvidia": {"h264": ["nvh264dec"], "h265": ["nvh265dec"]},
    "apple": {"h264": ["vtdec_hw", "vtdec"], "h265": ["vtdec_hw", "vtdec"]},
    "cpu": {"h264": ["avdec_h264", "openh264dec"], "h265": ["avdec_h265"]},
}
AUTO_ORDER = ["nvidia", "apple", "cpu"]


class NoDecoder(RuntimeError):
    pass


def available(element: str) -> bool:
    return Gst.ElementFactory.find(element) is not None


def choose(codec: str, preference: str = "auto") -> tuple[str, str]:
    """(kind, element name) for ``codec`` ("h264" or "h265")."""
    kinds = AUTO_ORDER if preference == "auto" else [preference]
    for kind in kinds:
        for element in DECODERS[kind].get(codec, []):
            if available(element):
                return kind, element
    wanted = "any decoder" if preference == "auto" else f"a {preference} decoder"
    raise NoDecoder(f"no {codec} decoder: this machine has {wanted} for it? install the GStreamer plugins")


def report() -> dict[str, dict[str, str | None]]:
    """What this machine can decode with, for the API and the docs."""
    out: dict[str, dict[str, str | None]] = {}
    for kind in AUTO_ORDER:
        out[kind] = {}
        for codec, elements in DECODERS[kind].items():
            out[kind][codec] = next((e for e in elements if available(e)), None)
    return out
