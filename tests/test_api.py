"""The gateway end to end: API, supervisor, camera processes, Redis frame bus, clips, MediaMTX."""

import time

import pytest
from conftest import needs_gst

pytestmark = needs_gst


def wait(fn, timeout=15.0, every=0.2):
    end = time.time() + timeout
    while time.time() < end:
        value = fn()
        if value:
            return value
        time.sleep(every)
    raise TimeoutError("condition never became true")


class NoMediaMTX:
    """Stands in for MediaMTX when only file and test sources are used."""

    def ok(self):
        return False

    def add(self, *a):
        raise AssertionError("not relayed")

    def remove(self, *a):
        pass

    def ingest_url(self, camera_id):
        raise AssertionError("not relayed")

    def own_path(self, source):
        return None

    def view(self, camera_id):
        return {}


@pytest.fixture
def gateway(redis_url, tmp_path):
    from fastapi.testclient import TestClient

    from maia_gateway import bus
    from maia_gateway.api.app import create_app
    from maia_gateway.settings import Settings
    from maia_gateway.supervisor import Supervisor

    bus.connect(redis_url).flushdb()
    settings = Settings(redis_url=redis_url, clips_dir=str(tmp_path / "clips"), max_cameras=3)
    sup = Supervisor(settings, mediamtx=NoMediaMTX())
    with TestClient(create_app(settings, supervisor=sup)) as client:
        yield client, sup


def running(client, camera_id):
    cam = client.get(f"/cameras/{camera_id}").json()
    return cam if cam["state"] == "running" and cam["stats"].get("written", 0) > 2 else None


SYNTH = {"source": "test://ball?size=320x240&fps=25&gop=25", "relay": False, "clips": {"pre_s": 1, "post_s": 1}}


def test_camera_lifecycle_snapshot_and_metrics(gateway):
    client, sup = gateway
    assert client.post("/cameras", json={"id": "synth", "profile": SYNTH}).status_code == 201
    assert client.post("/cameras", json={"id": "synth", "profile": SYNTH}).status_code == 409
    cam = wait(lambda: (c := running(client, "synth")) and c["stats"]["gop_s"] and c)  # GOP needs 2 key frames
    assert cam["stats"]["decoder"] and cam["stats"]["width"] == 320 and cam["stats"]["gop_s"] == pytest.approx(1, 0.2)

    snap = client.get("/cameras/synth/snapshot")
    assert snap.status_code == 200 and snap.content[:2] == b"\xff\xd8"  # JPEG
    assert 'maia_gateway_camera_up{camera="synth"' in client.get("/metrics").text

    patched = client.patch("/cameras/synth", json={"steps": [{"resize": [160, 120]}], "pixfmt": "gray"})
    assert patched.status_code == 200
    wait(lambda: (c := running(client, "synth")) and c["stats"]["width"] == 160)
    assert client.patch("/cameras/synth", json={"sampling": {"mode": "never"}}).status_code == 422

    assert client.delete("/cameras/synth").status_code == 204
    assert client.get("/cameras/synth").status_code == 404
    assert client.get("/cameras/synth/view").status_code == 404


def test_admission_control(gateway):
    client, _ = gateway
    for i in range(3):
        assert client.post("/cameras", json={"id": f"c{i}", "profile": SYNTH}).status_code == 201
    full = client.post("/cameras", json={"id": "c3", "profile": SYNTH})
    assert full.status_code == 503 and "full" in full.json()["detail"]


def test_crashed_camera_is_restarted(gateway):
    client, sup = gateway
    client.post("/cameras", json={"id": "synth", "profile": SYNTH})
    wait(lambda: running(client, "synth"))
    sup.cameras["synth"].process.kill()  # simulate a crash
    wait(lambda: client.get("/cameras/synth").json()["restarts"] >= 1)
    cam = wait(lambda: running(client, "synth"))
    assert cam["last_exit"] == -9


def test_events_make_clips(gateway):
    from test_clips import probe

    client, _ = gateway
    client.post("/cameras", json={"id": "synth", "profile": SYNTH})
    wait(lambda: running(client, "synth"))
    time.sleep(1.5)  # fill the pre-event buffer
    event = client.post("/cameras/synth/events", json={"label": "person", "meta": {"track": 3}}).json()
    assert event["ok"] and event["clip_id"]
    clip = wait(lambda: next((c for c in client.get("/clips?camera=synth").json() if c["status"] == "done"), None))
    assert clip["id"] == event["clip_id"] and clip["events"][0]["track"] == 3
    video = client.get(f"/clips/synth/{clip['id']}.mp4")
    assert video.status_code == 200 and video.headers["content-type"] == "video/mp4"
    duration, _, _ = probe(clip["path"])
    assert 1.5 <= duration <= 3.5
    for bad in ["..%2F..%2Fetc", "synth/..%2F..%2Fsecret.mp4", "synth/x.mp4", "..%2Fsynth/" + clip["id"]]:
        assert client.get(f"/clips/{bad}").status_code == 404


def test_frame_bus_newest_every_and_groups(gateway, redis_url):
    from maia_gateway.bus import FrameStream

    client, _ = gateway
    client.post("/cameras", json={"id": "synth", "profile": {**SYNTH, "sampling": {"fps": 10}}})
    wait(lambda: running(client, "synth"))
    stream = FrameStream("synth", redis_url)
    latest = stream.latest()
    assert latest.array.shape == (240, 320, 3)

    newest = [f.ref.frame_index for f, _ in zip(stream.follow(), range(5), strict=False)]
    assert newest == sorted(newest)
    every = [f.ref.frame_index for f, _ in zip(stream.follow(every=True), range(10), strict=False)]
    assert every == list(range(every[0], every[0] + 10))  # in order, none skipped

    a, b = FrameStream("synth", redis_url), FrameStream("synth", redis_url)
    ga = a.follow(group="count", consumer="a", read=False)
    gb = b.follow(group="count", consumer="b", read=False)
    seen_a = {next(ga).frame_index for _ in range(4)}
    seen_b = {next(gb).frame_index for _ in range(4)}
    assert not seen_a & seen_b  # replicas in a group never get the same frame


def test_registry_survives_a_gateway_restart(redis_url, tmp_path):
    from fastapi.testclient import TestClient

    from maia_gateway import bus
    from maia_gateway.api.app import create_app
    from maia_gateway.settings import Settings
    from maia_gateway.supervisor import Supervisor

    bus.connect(redis_url).flushdb()
    settings = Settings(redis_url=redis_url, clips_dir=str(tmp_path))
    with TestClient(create_app(settings, supervisor=Supervisor(settings, NoMediaMTX()))) as client:
        client.post("/cameras", json={"id": "keep", "profile": SYNTH})
    with TestClient(create_app(settings, supervisor=Supervisor(settings, NoMediaMTX()))) as client:
        assert wait(lambda: running(client, "keep"))


def test_rtsp_camera_through_mediamtx(redis_url, tmp_path, mediamtx, rtsp_camera):
    """A real RTSP camera relayed by MediaMTX. RTSP frames carry no duration: this is the
    stream that made GStreamer's videorate abort, so it guards that regression too."""
    import httpx
    from fastapi.testclient import TestClient

    from maia_gateway import bus
    from maia_gateway.api.app import create_app
    from maia_gateway.settings import Settings
    from maia_gateway.supervisor import Supervisor

    bus.connect(redis_url).flushdb()
    settings = Settings(
        redis_url=redis_url, clips_dir=str(tmp_path), mediamtx_api=mediamtx["api"], mediamtx_rtsp=mediamtx["rtsp"]
    )
    with TestClient(create_app(settings, supervisor=Supervisor(settings))) as client:
        # 127.0.0.2 is this machine too, but not an address of the gateway's MediaMTX: it
        # stands in for a camera elsewhere, which MediaMTX relays
        camera = rtsp_camera.replace("127.0.0.1", "127.0.0.2")
        profile = {"source": camera, "sampling": {"fps": 5}}
        assert client.post("/cameras", json={"id": "gate-1", "profile": profile}).status_code == 201
        cam = wait(lambda: running(client, "gate-1"), timeout=20)
        assert cam["relayed"] and cam["stats"]["width"] == 1280 and cam["restarts"] == 0
        time.sleep(3)
        cam = client.get("/cameras/gate-1").json()
        assert cam["restarts"] == 0 and cam["stats"]["fps_out"] == pytest.approx(5, abs=0.6)
        assert client.get("/health").json()["mediamtx"] is True
        view = client.get("/cameras/gate-1/view").json()
        assert view["rtsp"].endswith("/gate-1")
        hls = httpx.get(f"http://127.0.0.1:{mediamtx['hls_port']}/gate-1/index.m3u8", follow_redirects=True)
        assert hls.status_code == 200 and "#EXTM3U" in hls.text  # people can watch the relayed camera
        assert client.delete("/cameras/gate-1").status_code == 204
        assert httpx.get(f"{mediamtx['api']}/v3/paths/get/gate-1").status_code == 404  # path removed too


class FlakyMediaMTX(NoMediaMTX):
    """MediaMTX that is still starting: the first attempt to create a path fails."""

    def __init__(self):
        self.calls = 0

    def add(self, camera_id, source):
        from maia_gateway.mediamtx import MediaMTXError

        self.calls += 1
        if self.calls == 1:
            raise MediaMTXError("MediaMTX API unreachable at http://mediamtx:9997: connection refused")

    def ingest_url(self, camera_id):
        return "test://ball?size=320x240&fps=25&gop=25"  # stands in for the relayed stream


def test_mediamtx_down_does_not_stop_the_gateway(redis_url, tmp_path):
    from fastapi.testclient import TestClient

    from maia_gateway import bus
    from maia_gateway.api.app import create_app
    from maia_gateway.config import CameraProfile
    from maia_gateway.settings import Settings
    from maia_gateway.supervisor import Supervisor

    bus.connect(redis_url).flushdb()
    settings = Settings(redis_url=redis_url, clips_dir=str(tmp_path))
    mtx = FlakyMediaMTX()
    config = {"relayed": CameraProfile(source="rtsp://10.0.0.21/stream1")}
    with TestClient(create_app(settings, supervisor=Supervisor(settings, mtx), cameras=config)) as client:
        cam = client.get("/cameras/relayed").json()  # the gateway started despite MediaMTX
        assert cam["state"] == "waiting-for-mediamtx" and "unreachable" in cam["error"]
        cam = wait(lambda: running(client, "relayed"))  # retried in the background, then running
        assert mtx.calls == 2 and cam["error"] is None


def test_file_published_to_mediamtx_keeps_playing_across_loops(mediamtx, tmp_path):
    """A video file watched like a camera: the encoded packets go to MediaMTX, and looping
    must not restart timestamps (HLS muxers reject that, and the stream would stall)."""
    import subprocess
    import threading

    import httpx

    from maia_gateway.config import CameraProfile
    from maia_gateway.ingest.pipeline import Ingest, test_source

    video = tmp_path / "one-second.mp4"
    src = test_source("test://ball?size=320x240&fps=25&gop=25").replace("is-live=true", "num-buffers=25")
    subprocess.run(
        ["gst-launch-1.0", "-q", *src.split(), "!", "mp4mux", "!", "filesink", f"location={video}"], check=True
    )
    profile = CameraProfile(source=f"file://{video}", publish=True, sampling={"fps": 5})
    ingest = Ingest("pub", profile, publish_url=f"{mediamtx['rtsp']}/pub")
    t = threading.Thread(target=ingest.run)
    t.start()
    hls = f"http://127.0.0.1:{mediamtx['hls_port']}/pub/index.m3u8"
    codes = []
    for _ in range(4):  # ~4 loops of the 1 s file
        time.sleep(1.2)
        codes.append(httpx.get(hls, follow_redirects=True, timeout=5).status_code)
    ingest.stop()
    t.join(10)
    assert ingest.stats.state == "stopped" and ingest.stats.written >= 15
    assert codes[-1] == 200, codes


def test_file_played_once_ends_and_is_not_restarted(gateway, tmp_path):
    import subprocess

    from maia_gateway.ingest.pipeline import test_source

    client, sup = gateway
    video = tmp_path / "once.mp4"
    src = test_source("test://ball?size=320x240&fps=25&gop=25").replace("is-live=true", "num-buffers=25")
    cmd = ["gst-launch-1.0", "-q", *src.split(), "!", "mp4mux", "!", "filesink", f"location={video}"]
    subprocess.run(cmd, check=True)
    profile = {"source": f"file://{video}", "loop": False}
    assert client.post("/cameras", json={"id": "once", "profile": profile}).status_code == 201
    cam = wait(lambda: (c := client.get("/cameras/once").json())["state"] == "ended" and c)
    time.sleep(2)  # the supervisor must leave it alone
    cam = client.get("/cameras/once").json()
    assert cam["state"] == "ended" and cam["restarts"] == 0 and cam["last_exit"] == 3

    from maia_gateway.bus import FrameStream

    stream = FrameStream("once", r=sup.r)
    t0 = time.monotonic()
    assert list(stream.follow(block_ms=200)) == []  # the camera has ended: the stream ends too
    assert time.monotonic() - t0 < 2 and stream.camera_state() == "ended"


def test_file_without_usable_video_is_failed_not_restarted(gateway, tmp_path):
    import subprocess

    client, sup = gateway
    video = tmp_path / "vp9.webm"
    cmd = [
        "gst-launch-1.0",
        "-q",
        "videotestsrc",
        "num-buffers=25",
        "!",
        "vp9enc",
        "deadline=1",
        "!",
        "webmmux",
        "!",
        "filesink",
        f"location={video}",
    ]
    subprocess.run(cmd, check=True)
    assert client.post("/cameras", json={"id": "vp9", "profile": {"source": f"file://{video}"}}).status_code == 201
    wait(lambda: client.get("/cameras/vp9").json()["state"] == "failed")
    time.sleep(2)
    cam = client.get("/cameras/vp9").json()
    assert cam["state"] == "failed" and cam["restarts"] == 0 and cam["last_exit"] == 4
    assert "video/x-vp9" in (cam.get("error") or str(cam))


@pytest.mark.parametrize("width", [640, 606, 605])
def test_snapshot_of_any_width(width):
    import numpy as np

    from maia_gateway.snapshot import jpeg

    for pixfmt, shape in (("bgr", (48, width, 3)), ("gray", (48, width))):
        data = jpeg(np.zeros(shape, np.uint8), pixfmt)
        assert data[:2] == b"\xff\xd8"  # a JPEG, even when rows need padding


def test_stream_already_on_our_mediamtx_is_read_not_relayed(redis_url, tmp_path, mediamtx, rtsp_camera):
    """A webcam published into the gateway's own MediaMTX: relaying it would replace the
    publisher's path with one that pulls from itself, and drop the webcam."""
    import httpx
    from fastapi.testclient import TestClient

    from maia_gateway import bus
    from maia_gateway.api.app import create_app
    from maia_gateway.settings import Settings
    from maia_gateway.supervisor import Supervisor

    bus.connect(redis_url).flushdb()
    settings = Settings(
        redis_url=redis_url, clips_dir=str(tmp_path), mediamtx_api=mediamtx["api"], mediamtx_rtsp=mediamtx["rtsp"]
    )
    with TestClient(create_app(settings, supervisor=Supervisor(settings))) as client:
        source = rtsp_camera.replace("127.0.0.1", "localhost")  # as a person would type it
        profile = {"source": source, "sampling": {"fps": 5}}
        assert client.post("/cameras", json={"id": "webcam", "profile": profile}).status_code == 201
        cam = wait(lambda: running(client, "webcam"), timeout=20)
        assert not cam["relayed"] and cam["viewable"] and cam["mediamtx_path"] == "upstream"
        assert client.get("/cameras/webcam/view").json()["rtsp"].endswith("/upstream")
        time.sleep(2)
        path = httpx.get(f"{mediamtx['api']}/v3/paths/get/upstream").json()
        assert path["ready"] and path["source"]["type"] == "rtspSession"  # the publisher is still there
        assert client.delete("/cameras/webcam").status_code == 204
        assert httpx.get(f"{mediamtx['api']}/v3/paths/get/upstream").status_code == 200  # left alone
