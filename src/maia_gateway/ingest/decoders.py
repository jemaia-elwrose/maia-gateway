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


def nvidia_element(codec: str, gpu: int | None) -> str:
    """NVIDIA's plugin registers one decoder per GPU: nvh264dec is GPU 0, nvh264device1dec
    GPU 1, and so on (the device is fixed per element; it cannot be set as a property)."""
    return f"nv{codec}dec" if not gpu else f"nv{codec}device{gpu}dec"


# fields the parser in front of every decoder converts or recomputes, so they never decide
_CONVERTED = ("stream-format", "alignment", "codec_data", "parsed", "level")


def accepts(element: str, caps: Gst.Caps | None) -> bool:
    """Whether ``element`` can decode a stream with these caps. Hardware decoders list the
    profiles they support (NVDEC and many others: no 4:4:4, some no 10-bit), so a stream they
    cannot decode is caught before the pipeline starts instead of failing to negotiate."""
    factory = Gst.ElementFactory.find(element)
    if factory is None:
        return False
    if caps is None or caps.is_any():
        return True
    stream = Gst.Caps.new_empty()
    for i in range(caps.get_size()):
        structure = caps.get_structure(i).copy()  # (PyGObject hands out copies anyway)
        for name in _CONVERTED:
            structure.remove_field(name)
        stream.append_structure(structure)
    for template in factory.get_static_pad_templates():
        if template.direction == Gst.PadDirection.SINK:
            return template.get_caps().can_intersect(stream)
    return True


def choose(
    codec: str, preference: str = "auto", gpu: int | None = None, caps: Gst.Caps | None = None
) -> tuple[str, str, list[str]]:
    """(kind, element, notes) for ``codec`` ("h264" or "h265"), on GPU ``gpu`` for NVIDIA.
    With ``caps``, decoders that cannot handle the stream's profile are skipped (noted)."""
    kinds = AUTO_ORDER if preference == "auto" else [preference]
    notes: list[str] = []
    profile = caps.get_structure(0).get_string("profile") if caps is not None and caps.get_size() else None
    for kind in kinds:
        candidates = DECODERS[kind].get(codec, [])
        if kind == "nvidia":
            candidates = [nvidia_element(codec, gpu)]
        for element in candidates:
            if not available(element):
                continue
            if not accepts(element, caps):
                notes.append(f"{element} cannot decode {codec} profile {profile or '?'}")
                continue
            return kind, element, notes
    wanted = "decoder" if preference == "auto" else f"{preference} decoder"
    detail = f" ({'; '.join(notes)})" if notes else ""
    raise NoDecoder(f"no {wanted} for this {codec} stream{detail}")


def report() -> dict[str, dict[str, str | None]]:
    """What this machine can decode with, for the API and the docs."""
    out: dict[str, dict[str, str | None]] = {}
    for kind in AUTO_ORDER:
        out[kind] = {}
        for codec, elements in DECODERS[kind].items():
            out[kind][codec] = next((e for e in elements if available(e)), None)
    return out
