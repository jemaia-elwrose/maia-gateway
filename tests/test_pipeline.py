"""The ingest pipeline on synthetic cameras: sampling, image steps, file sources, clips."""

import os
import subprocess
import threading
import time

import pytest
from conftest import needs_gst

pytestmark = needs_gst


def run(profile: dict, seconds: float, camera="t"):
    """Run a camera for ``seconds``; return its stats, the refs it published and the last frame."""
    from maia_ring import RingReader

    from maia_gateway.config import CameraProfile
    from maia_gateway.ingest.pipeline import Ingest

    camera = f"{camera}{os.getpid() % 10000}"
    refs, packets = [], []
    ingest = Ingest(camera, CameraProfile.model_validate(profile), publish=refs.append, on_packet=packets.append)
    t = threading.Thread(target=ingest.run)
    t.start()
    time.sleep(seconds)
    last = None
    if ingest.ring is not None:
        with RingReader(ingest.ring.name) as r:
            last = r.latest()
    ingest.stop()
    t.join(10)
    return ingest, refs, packets, last


def test_fps_sampling_and_gop():
    ingest, refs, packets, last = run({"source": "test://ball?size=640x360&fps=25&gop=50", "sampling": {"fps": 5}}, 3)
    s = ingest.stats
    assert 12 <= s.written <= 17, s.written  # 5 fps for 3 s
    assert s.decoded >= 3 * s.written  # every frame decoded, most dropped before conversion
    assert s.gop_s == pytest.approx(2.0, abs=0.1)
    assert last.array.shape == (360, 640, 3)
    assert [r.frame_index for r in refs] == list(range(len(refs)))


def test_fractional_fps():
    ingest, *_ = run({"source": "test://ball?size=320x240&fps=25", "sampling": {"fps": 0.5}}, 4.5)
    assert 2 <= ingest.stats.written <= 3


def test_iframes_mode_decodes_only_key_frames():
    ingest, refs, packets, last = run(
        {"source": "test://ball?size=320x240&fps=25&gop=25", "sampling": {"mode": "iframes"}}, 3.5
    )
    s = ingest.stats
    assert 3 <= s.written <= 4 and s.decoded == s.written  # one frame per 1 s GOP, nothing else decoded
    assert len(packets) > 5 * s.written  # yet every packet still reached the clip branch
    assert last.meta.is_iframe


def test_motion_mode_keeps_still_scenes_quiet_and_moving_ones_busy():
    still, *_ = run(
        {"source": "test://black?size=320x240&fps=25", "sampling": {"mode": "motion", "fps": 10, "keepalive_s": 1}},
        3,
    )
    moving, *_ = run({"source": "test://ball?size=320x240&fps=25", "sampling": {"mode": "motion", "fps": 10}}, 3)
    assert 2 <= still.stats.written <= 4  # keep-alive frames only
    assert moving.stats.written >= 20


def test_steps_rotate_crop_resize_mask_gray():
    _, _, _, rotated = run({"source": "test://ball?size=640x360", "steps": [{"rotate": 90}]}, 1.5)
    assert rotated.array.shape == (640, 360, 3)
    _, _, _, f = run(
        {
            "source": "test://smpte?size=640x360",
            "pixfmt": "gray",
            "steps": [
                {"crop": [0.25, 0.25, 0.75, 0.75]},
                {"resize": [160, 90]},
                {"privacy_mask": [[0.0, 0.0, 0.5, 0.5]]},
            ],
        },
        1.5,
    )
    assert f.array.shape == (90, 160)
    assert f.array[:45, :80].max() == 0 and f.array[45:, 80:].mean() > 30


def test_night_enhance_brightens():
    _, _, _, plain = run({"source": "test://smpte?size=320x240"}, 1.2)
    _, _, _, night = run({"source": "test://smpte?size=320x240", "steps": [{"enhance": "night"}]}, 1.2)
    assert night.array.mean() > plain.array.mean() + 5


@pytest.fixture(scope="module")
def video_file(tmp_path_factory):
    from maia_gateway.ingest.pipeline import test_source

    path = tmp_path_factory.mktemp("video") / "loop.mp4"
    src = test_source("test://ball?size=320x240&fps=25&gop=25").replace("is-live=true", "num-buffers=25")
    subprocess.run(
        ["gst-launch-1.0", "-q", *src.split(), "!", "mp4mux", "!", "filesink", f"location={path}"], check=True
    )
    return path


def test_file_source_plays_in_real_time_and_loops(video_file):
    ingest, refs, _, _ = run({"source": f"file://{video_file}", "sampling": {"fps": 10}}, 3.2)
    # the file is 1 s long: 3 s of frames means it looped, and ~10 fps means it was paced
    # like a camera instead of decoded as fast as possible
    assert 25 <= ingest.stats.written <= 34
    assert ingest.stats.state == "stopped"


def test_ring_is_removed_when_the_camera_stops():
    from maia_ring import RingReader

    ingest, *_ = run({"source": "test://ball?size=160x120"}, 1)
    with pytest.raises(FileNotFoundError):
        RingReader(f"cam-t{os.getpid() % 10000}")
    assert ingest.ring is None


def test_clip_from_the_encoded_stream(tmp_path):
    from test_clips import probe

    from maia_gateway.config import CameraProfile
    from maia_gateway.ingest.clips import ClipRecorder
    from maia_gateway.ingest.pipeline import Ingest

    done = []
    holder = {}
    ingest = Ingest(
        "clip", CameraProfile(source="test://ball?size=320x240&fps=25&gop=25"), on_packet=lambda p: holder["r"].add(p)
    )
    holder["r"] = ClipRecorder("clip", tmp_path, 1.5, 1.0, caps=lambda: ingest.caps_string, on_done=done.append)
    t = threading.Thread(target=ingest.run)
    t.start()
    time.sleep(2.5)
    clip = holder["r"].event(label="test")
    time.sleep(2.0)
    ingest.stop()
    t.join(10)
    assert done and done[0].id == clip.id and done[0].status == "done", done and done[0].error
    duration, width, height = probe(done[0].path)
    assert (width, height) == (320, 240)
    assert 2.4 <= duration <= 4.0  # 1.5 s before (from the previous key frame) + 1.0 s after
