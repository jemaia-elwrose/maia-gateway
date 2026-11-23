import pytest
from pydantic import ValidationError

from maia_gateway.config import CameraProfile, Crop, GatewayConfig, Resize, check_camera_id, ring_name


def test_minimal_profile_gets_defaults():
    p = CameraProfile(source="rtsp://10.0.0.21/stream1")
    assert p.sampling.mode == "fps" and p.sampling.fps == 5 and p.ring.slots == 32 and p.relayed


def test_steps_parse_and_are_found_by_kind():
    p = CameraProfile(source="test://ball", steps=[{"crop": [0, 0.3, 1, 1]}, {"resize": [1280, 720]}])
    assert p.step(Crop).crop == (0, 0.3, 1, 1) and p.step(Resize).resize == (1280, 720)


@pytest.mark.parametrize(
    "bad",
    [
        {"source": "http://x"},  # not a supported scheme
        {"source": "test://x", "steps": [{"crop": [0.5, 0, 0.4, 1]}]},  # left > right
        {"source": "test://x", "steps": [{"resize": [1280, 720]}, {"resize": [640, 360]}]},  # twice
        {"source": "test://x", "steps": [{"rotate": 45}]},
        {"source": "test://x", "sampling": {"mode": "sometimes"}},
        {"source": "test://x", "sampling": {"fps": 0}},
        {"source": "test://x", "typo": 1},  # unknown keys are errors, not silently ignored
    ],
)
def test_invalid_profiles_are_rejected(bad):
    with pytest.raises(ValidationError):
        CameraProfile.model_validate(bad)


def test_file_and_test_sources_are_never_relayed():
    assert not CameraProfile(source="file:///v.mp4").relayed
    assert not CameraProfile(source="rtsp://x/y", relay=False).relayed


def test_camera_ids_fit_ring_names():
    assert len(ring_name(check_camera_id("a" * 23))) <= 30
    for bad in ["Gate", "a" * 24, "-x", "a_b", ""]:
        with pytest.raises(ValueError):
            check_camera_id(bad)


def test_yaml_config(tmp_path):
    f = tmp_path / "cams.yml"
    f.write_text("cameras:\n  gate-1:\n    source: rtsp://10.0.0.21/s\n    sampling: {mode: iframes}\n")
    assert GatewayConfig.load(f).cameras["gate-1"].sampling.mode == "iframes"
    f.write_text("cameras:\n  Gate_1:\n    source: rtsp://10.0.0.21/s\n")
    with pytest.raises(ValidationError):
        GatewayConfig.load(f)


def test_nvidia_decoder_per_gpu():
    pytest.importorskip("gi")
    from maia_gateway.ingest.decoders import nvidia_element

    # NVIDIA's plugin fixes the GPU per element; the device cannot be set as a property
    assert [nvidia_element("h264", g) for g in (None, 0, 1)] == ["nvh264dec", "nvh264dec", "nvh264device1dec"]
    assert nvidia_element("h265", 3) == "nvh265device3dec"
