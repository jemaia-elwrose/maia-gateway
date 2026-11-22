"""Fixtures: a throwaway Redis server, a MediaMTX server and an RTSP test camera, each
skipped when the binary is not installed (CI installs all of them)."""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time
from pathlib import Path

import pytest


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_port(port: int, timeout: float = 10.0) -> None:
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.05)
    raise TimeoutError(f"nothing listening on {port}")


def has_gstreamer() -> bool:
    try:
        from maia_gateway.ingest import gst  # noqa: F401

        return True
    except ImportError:
        return False


needs_gst = pytest.mark.skipif(not has_gstreamer(), reason="GStreamer Python bindings not installed")


@pytest.fixture(scope="session")
def redis_url():
    binary = shutil.which("redis-server")
    if binary is None:
        pytest.skip("redis-server not installed")
    port = free_port()
    proc = subprocess.Popen(
        [binary, "--port", str(port), "--save", "", "--appendonly", "no"], stdout=subprocess.DEVNULL
    )
    wait_port(port)
    yield f"redis://127.0.0.1:{port}/0"
    proc.terminate()
    proc.wait(5)


@pytest.fixture(scope="session")
def mediamtx(tmp_path_factory):
    binary = os.environ.get("MEDIAMTX") or shutil.which("mediamtx")
    if binary is None or not Path(binary).exists():
        pytest.skip("mediamtx not installed (set MEDIAMTX=/path/to/mediamtx)")
    api, rtsp, hls = free_port(), free_port(), free_port()
    conf = tmp_path_factory.mktemp("mtx") / "mediamtx.yml"
    conf.write_text(
        f"logLevel: warn\napi: true\napiAddress: 127.0.0.1:{api}\nrtspAddress: :{rtsp}\n"
        f"rtmp: false\nhls: true\nhlsAddress: :{hls}\nwebrtc: false\nsrt: false\npaths:\n  all_others:\n"
    )
    proc = subprocess.Popen([binary, str(conf)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    wait_port(api)
    yield {"api": f"http://127.0.0.1:{api}", "rtsp": f"rtsp://127.0.0.1:{rtsp}", "rtsp_port": rtsp, "hls_port": hls}
    proc.terminate()
    proc.wait(5)


@pytest.fixture(scope="session")
def rtsp_camera(mediamtx):
    """A live H.264 camera published into MediaMTX at <rtsp>/upstream (720p, 25 fps, GOP 2 s)."""
    if shutil.which("gst-launch-1.0") is None:
        pytest.skip("gst-launch-1.0 not installed")
    from maia_gateway.ingest.pipeline import test_source

    pipeline = test_source("test://ball?size=1280x720&fps=25&gop=50")
    url = f"{mediamtx['rtsp']}/upstream"
    proc = subprocess.Popen(
        ["gst-launch-1.0", "-q", *pipeline.split(), "!", "rtspclientsink", f"location={url}", "protocols=tcp"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(2)
    yield url
    proc.terminate()
    proc.wait(5)
