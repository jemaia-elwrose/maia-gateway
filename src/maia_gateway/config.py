"""Camera profiles: what to ingest, how to process it and how to sample it.

cameras:
  gate-1:
    source: rtsp://user:pass@10.0.0.21/stream1
    steps:
      - crop: [0.0, 0.3, 1.0, 1.0]
      - resize: [1280, 720]
    sampling: {mode: fps, fps: 5}
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

CAMERA_ID = re.compile(r"^[a-z0-9][a-z0-9-]{0,22}$")  # ring names are "cam-<id>", at most 30 chars
Fraction = Annotated[float, Field(ge=0.0, le=1.0)]


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Rotate(_Strict):
    rotate: Literal[0, 90, 180, 270]


class Flip(_Strict):
    flip: Literal["horizontal", "vertical"]


class Crop(_Strict):
    """Region of interest as fractions of the frame: [left, top, right, bottom]."""

    crop: tuple[Fraction, Fraction, Fraction, Fraction]

    @field_validator("crop")
    @classmethod
    def _ordered(cls, v):
        if not (v[0] < v[2] and v[1] < v[3]):
            raise ValueError("crop must be [left, top, right, bottom] with left < right and top < bottom")
        return v


class Resize(_Strict):
    resize: tuple[Annotated[int, Field(ge=16, le=7680)], Annotated[int, Field(ge=16, le=4320)]]


class PrivacyMask(_Strict):
    """Rectangles blacked out in every frame, as fractions: [[left, top, right, bottom], ...]."""

    privacy_mask: list[tuple[Fraction, Fraction, Fraction, Fraction]]


class Enhance(_Strict):
    """``night``: gamma and contrast lift for dark scenes (a cheap filter, not CLAHE)."""

    enhance: Literal["off", "night"]


Step = Rotate | Flip | Crop | Resize | PrivacyMask | Enhance


class Sampling(_Strict):
    #: fps: an even rate. iframes: only key frames are decoded (cheap; rate = the camera's GOP).
    #: motion: frames whose scene changed, at most ``fps``.
    mode: Literal["fps", "iframes", "motion"] = "fps"
    fps: float = Field(5.0, gt=0, le=120)
    #: motion mode: keep a frame when at least this share of the image changed (0.002 = 0.2 %,
    #: small enough for a distant person), counting pixels whose brightness moved by more
    #: than ``motion_delta`` (0-255; higher ignores more sensor noise)
    motion_area: float = Field(0.002, gt=0, le=1)
    motion_delta: int = Field(20, ge=1, le=255)
    #: motion mode: keep one frame at least this often anyway, so readers see the camera is alive
    keepalive_s: float = Field(10.0, gt=0)


class RingConfig(_Strict):
    slots: int = Field(32, ge=2, le=1024)


class ClipConfig(_Strict):
    pre_s: float = Field(5.0, ge=0, le=60)
    post_s: float = Field(10.0, ge=0, le=300)


class CameraProfile(_Strict):
    #: rtsp://..., file:///path/video.mp4, or test://<pattern> (a synthetic H.264 camera)
    source: str
    decode: Literal["auto", "nvidia", "apple", "cpu"] = "auto"
    pixfmt: Literal["bgr", "rgb", "gray"] = "bgr"
    steps: list[Step] = Field(default_factory=list)
    sampling: Sampling = Field(default_factory=Sampling)
    ring: RingConfig = Field(default_factory=RingConfig)
    clips: ClipConfig = Field(default_factory=ClipConfig)
    #: pull the camera through MediaMTX (one connection to the camera, live view for people);
    #: when false, or for file:// and test:// sources, ingest reads the source directly
    relay: bool = True
    #: file:// and test:// sources: push the encoded stream into MediaMTX as well (no
    #: re-encode), so a video file can be watched live like a camera (WebRTC, HLS, RTSP)
    publish: bool = False
    #: file:// sources: start again at the end (true), or play once and stop (false)
    loop: bool = True

    @field_validator("source")
    @classmethod
    def _known_scheme(cls, v: str) -> str:
        if not re.match(r"^(rtsp|rtsps|file|test)://", v):
            raise ValueError("source must start with rtsp://, rtsps://, file:// or test://")
        return v

    @model_validator(mode="after")
    def _one_of_each(self):
        kinds = [type(s).__name__ for s in self.steps]
        dupes = {k for k in kinds if kinds.count(k) > 1}
        if dupes:
            raise ValueError(f"each step may appear once: {sorted(dupes)}")
        return self

    def step(self, kind: type):
        return next((s for s in self.steps if isinstance(s, kind)), None)

    @property
    def relayed(self) -> bool:
        return self.relay and self.source.startswith(("rtsp://", "rtsps://"))

    @property
    def viewable(self) -> bool:
        """Watchable through MediaMTX: relayed cameras, and published files/test sources."""
        return self.relayed or (self.publish and self.source.startswith(("file://", "test://")))


def check_camera_id(camera_id: str) -> str:
    if not CAMERA_ID.match(camera_id):
        raise ValueError("camera id: lowercase letters, digits and '-', 1-23 characters")
    return camera_id


def ring_name(camera_id: str) -> str:
    return f"cam-{camera_id}"


class GatewayConfig(_Strict):
    cameras: dict[str, CameraProfile] = Field(default_factory=dict)

    @field_validator("cameras")
    @classmethod
    def _ids(cls, v):
        for camera_id in v:
            check_camera_id(camera_id)
        return v

    @classmethod
    def load(cls, path: str | Path) -> GatewayConfig:
        return cls.model_validate(yaml.safe_load(Path(path).read_text()) or {})
