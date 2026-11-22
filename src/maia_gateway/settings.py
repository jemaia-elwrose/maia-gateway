"""Gateway settings, from environment variables (all optional)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass
class Settings:
    redis_url: str = field(default_factory=lambda: _env("MAIA_REDIS_URL", "redis://localhost:6379/0"))
    #: MediaMTX control API and the RTSP address ingest reads relayed cameras from
    mediamtx_api: str = field(default_factory=lambda: _env("MAIA_MEDIAMTX_API", "http://localhost:9997"))
    mediamtx_rtsp: str = field(default_factory=lambda: _env("MAIA_MEDIAMTX_RTSP", "rtsp://localhost:8554"))
    #: host name people use to watch cameras (for the links in GET /cameras/{id}/view)
    public_host: str = field(default_factory=lambda: _env("MAIA_PUBLIC_HOST", "localhost"))
    clips_dir: str = field(default_factory=lambda: _env("MAIA_CLIPS_DIR", "./clips"))
    #: if set, every API call needs the header X-API-Key with this value
    api_key: str | None = field(default_factory=lambda: os.environ.get("MAIA_API_KEY") or None)
    #: admission control: cameras this gateway accepts, and NVIDIA decode sessions per GPU
    max_cameras: int = field(default_factory=lambda: int(_env("MAIA_MAX_CAMERAS", "32")))
    gpus: int = field(default_factory=lambda: int(_env("MAIA_GPUS", "0")))
    sessions_per_gpu: int = field(default_factory=lambda: int(_env("MAIA_SESSIONS_PER_GPU", "16")))
