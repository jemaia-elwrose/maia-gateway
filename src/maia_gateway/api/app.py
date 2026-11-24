"""Control API: cameras, profiles, snapshots, viewing links, events and clips, health, metrics."""

from __future__ import annotations

import json
import logging
import re
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Response
from fastapi.responses import FileResponse, PlainTextResponse
from maia_ring import RingReader
from pydantic import BaseModel, Field

from .. import __version__, bus
from ..config import CAMERA_ID, CameraProfile, ring_name
from ..mediamtx import MediaMTXError
from ..settings import Settings
from ..supervisor import AdmissionError, Supervisor

log = logging.getLogger("maia_gateway.api")
CLIP_ID = re.compile(r"^\d{8}-\d{6}-[0-9a-f]{6}(\.mp4)?$")


class NewCamera(BaseModel):
    id: str
    profile: CameraProfile


class Event(BaseModel):
    label: str = ""
    #: wall-clock time of the event, ns since the epoch; default: now
    at_ns: int | None = None
    meta: dict[str, Any] = Field(default_factory=dict)


def _merge(base: dict, patch: dict) -> dict:
    out = dict(base)
    for k, v in patch.items():
        out[k] = _merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


def create_app(settings: Settings | None = None, supervisor: Supervisor | None = None, cameras=None) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        sup = supervisor or Supervisor(settings)
        app.state.sup = sup
        sup.start()
        for camera_id, profile in (cameras or {}).items():  # from --config: added or updated
            try:
                if camera_id in sup.cameras:
                    if sup.cameras[camera_id].profile != profile:
                        sup.update(camera_id, profile)
                else:
                    sup.add(camera_id, profile)
            except Exception as exc:  # one bad camera must not keep the gateway down
                log.error("camera %s from the config was not added: %s", camera_id, exc)
        yield
        sup.shutdown()

    def auth(x_api_key: str | None = Header(default=None)) -> None:
        if settings.api_key and x_api_key != settings.api_key:
            raise HTTPException(401, "missing or wrong X-API-Key")

    app = FastAPI(title="maia-gateway", version=__version__, lifespan=lifespan, dependencies=[Depends(auth)])

    def sup() -> Supervisor:
        return app.state.sup

    def camera_or_404(camera_id: str):
        if camera_id not in sup().cameras:
            raise HTTPException(404, f"no camera {camera_id}")
        return sup().cameras[camera_id]

    # ---------------------------------------------------------------- cameras
    @app.get("/cameras")
    def list_cameras() -> list[dict]:
        return [sup().status(c) for c in sorted(sup().cameras)]

    @app.post("/cameras", status_code=201)
    def add_camera(body: NewCamera) -> dict:
        try:
            sup().add(body.id, body.profile)
        except KeyError as exc:
            raise HTTPException(409, str(exc).strip("'\""))
        except (AdmissionError, MediaMTXError) as exc:
            raise HTTPException(503, str(exc))
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        return sup().status(body.id)

    @app.get("/cameras/{camera_id}")
    def get_camera(camera_id: str) -> dict:
        camera_or_404(camera_id)
        return sup().status(camera_id)

    @app.patch("/cameras/{camera_id}")
    def patch_camera(camera_id: str, patch: dict[str, Any]) -> dict:
        cam = camera_or_404(camera_id)
        try:
            profile = CameraProfile.model_validate(_merge(cam.profile.model_dump(), patch))
            sup().update(camera_id, profile)
        except (AdmissionError, MediaMTXError) as exc:
            raise HTTPException(503, str(exc))
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        return sup().status(camera_id)

    @app.delete("/cameras/{camera_id}", status_code=204)
    def delete_camera(camera_id: str) -> Response:
        camera_or_404(camera_id)
        sup().remove(camera_id)
        return Response(status_code=204)

    @app.post("/cameras/{camera_id}/restart")
    def restart_camera(camera_id: str) -> dict:
        camera_or_404(camera_id)
        sup().restart(camera_id)
        return sup().status(camera_id)

    @app.get("/cameras/{camera_id}/snapshot", response_class=Response)
    def snapshot(camera_id: str, quality: int = 85) -> Response:
        from ..snapshot import jpeg

        cam = camera_or_404(camera_id)
        try:
            with RingReader(ring_name(camera_id)) as ring:
                frame = ring.latest()
        except FileNotFoundError:
            frame = None
        if frame is None:
            raise HTTPException(503, "no frame yet")
        age_ms = (time.time_ns() - frame.meta.timestamp_ns) / 1e6
        headers = {"X-Frame-Index": str(frame.meta.frame_index), "X-Frame-Age-Ms": f"{age_ms:.0f}"}
        return Response(jpeg(frame.array, cam.profile.pixfmt, quality), media_type="image/jpeg", headers=headers)

    @app.get("/cameras/{camera_id}/view")
    def view(camera_id: str) -> dict:
        cam = camera_or_404(camera_id)
        if not cam.viewable:
            raise HTTPException(409, "live view needs an RTSP camera with relay: true, or publish: true for files")
        return sup().mtx.view(cam.mtx_path)

    # ----------------------------------------------------------------- clips
    @app.post("/cameras/{camera_id}/events", status_code=202)
    def raise_event(camera_id: str, event: Event) -> dict:
        cam = camera_or_404(camera_id)
        if not cam.running:
            raise HTTPException(409, "camera is not running")
        reply = bus.send_command(
            sup().r, camera_id, "clip", wait_s=3.0, at_ns=event.at_ns, label=event.label, **event.meta
        )
        if reply is None:
            raise HTTPException(504, "camera did not answer")
        return {"camera": camera_id, **reply, "ready_after_s": cam.profile.clips.post_s}

    def clips_dir() -> Path:
        return Path(settings.clips_dir)

    @app.get("/clips")
    def list_clips(camera: str | None = None, limit: int = 50) -> list[dict]:
        if camera is not None and not CAMERA_ID.match(camera):
            raise HTTPException(422, "bad camera id")
        files = sorted(clips_dir().glob(f"{camera or '*'}/*.json"), key=lambda p: p.name, reverse=True)
        return [json.loads(p.read_text()) for p in files[:limit]]

    @app.get("/clips/{camera_id}/{clip_id}")
    def get_clip(camera_id: str, clip_id: str):
        """Clip metadata, or the video itself with ``.mp4`` on the end."""
        if not CAMERA_ID.match(camera_id) or not CLIP_ID.match(clip_id):
            raise HTTPException(404, "no such clip")
        video = clips_dir() / camera_id / (clip_id.removesuffix(".mp4") + ".mp4")
        if clip_id.endswith(".mp4"):
            if not video.exists():
                raise HTTPException(404, "no such clip")
            return FileResponse(video, media_type="video/mp4")
        sidecar = video.with_suffix(".json")
        if not sidecar.exists():
            raise HTTPException(404, "no such clip")
        return json.loads(sidecar.read_text())

    # ---------------------------------------------------------------- health
    @app.get("/health")
    def health() -> dict:
        from ..ingest.decoders import report

        try:
            redis_ok = bool(sup().r.ping())
        except Exception:
            redis_ok = False
        cams = [sup().status(c) for c in sup().cameras]
        return {
            "version": __version__,
            "redis": redis_ok,
            "mediamtx": sup().mtx.ok(),
            "cameras": {s["id"]: s["state"] for s in cams},
            "decoders": report(),
        }

    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics() -> str:
        lines = []
        gauges = {
            "maia_gateway_camera_up": ("gauge", lambda s: 1 if s["state"] == "running" else 0),
            "maia_gateway_fps_out": ("gauge", lambda s: s["stats"].get("fps_out") or 0),
            "maia_gateway_gop_seconds": ("gauge", lambda s: s["stats"].get("gop_s") or 0),
            "maia_gateway_frames_written_total": ("counter", lambda s: s["stats"].get("written", 0)),
            "maia_gateway_frames_decoded_total": ("counter", lambda s: s["stats"].get("decoded", 0)),
            "maia_gateway_packets_total": ("counter", lambda s: s["stats"].get("packets", 0)),
            "maia_gateway_restarts": ("gauge", lambda s: s["restarts"]),
        }
        statuses = [sup().status(c) for c in sorted(sup().cameras)]
        for name, (kind, fn) in gauges.items():
            lines.append(f"# TYPE {name} {kind}")
            lines += [f'{name}{{camera="{s["id"]}",decode="{s["decode"]}"}} {fn(s)}' for s in statuses]
        return "\n".join(lines) + "\n"

    return app
