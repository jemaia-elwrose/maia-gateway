"""Runs one ingest process per camera and keeps them running.

- A camera whose process exits (stream error, stall, crash) is restarted with backoff:
  1, 2, 4 ... 60 s, reset after a minute of healthy running.
- Admission control: at most ``max_cameras``, and at most ``sessions_per_gpu`` NVIDIA
  decode sessions per GPU. Cameras go to the least-loaded GPU; when every GPU is full, an
  ``auto`` camera decodes on the CPU instead and a ``nvidia`` camera is refused.
- The camera registry lives in Redis, so cameras come back when the gateway restarts.
- MediaMTX unreachable (still starting, restarting, misconfigured) never stops the gateway:
  a relayed camera waits as ``waiting-for-mediamtx`` and its path is retried with backoff.
"""

from __future__ import annotations

import json
import logging
import multiprocessing as mp
import threading
import time
from dataclasses import dataclass, field

from . import bus
from .config import CameraProfile, check_camera_id
from .ingest.process import EXIT_ENDED, EXIT_FATAL
from .ingest.process import main as camera_main
from .mediamtx import MediaMTX, MediaMTXError
from .settings import Settings

log = logging.getLogger("maia_gateway.supervisor")
_ctx = mp.get_context("spawn")  # never fork a process that may hold GStreamer or CUDA state


class AdmissionError(RuntimeError):
    pass


@dataclass
class Camera:
    id: str
    profile: CameraProfile
    gpu: int | None = None
    decode: str = "auto"  # what the camera actually runs with (may differ from the profile)
    process: mp.Process | None = None
    started_at: float = 0.0
    restarts: int = 0
    next_start: float = 0.0
    last_exit: int | None = None
    error: str | None = None
    relay_pending: bool = False  # the MediaMTX path is not created yet
    ended: bool = False  # a file source played to its end with loop: false
    failed: bool = False  # a source that cannot work as configured (see the camera's error)
    relay_tries: int = 0
    own_path: str | None = None  # the source is this path on our MediaMTX: read it, no relay
    notes: list[str] = field(default_factory=list)

    @property
    def relayed(self) -> bool:
        return self.profile.relayed and self.own_path is None

    @property
    def viewable(self) -> bool:
        return self.profile.viewable or self.own_path is not None

    @property
    def mtx_path(self) -> str:
        return self.own_path or self.id

    @property
    def running(self) -> bool:
        return self.process is not None and self.process.is_alive()


class Supervisor:
    def __init__(self, settings: Settings, mediamtx: MediaMTX | None = None) -> None:
        self.settings = settings
        self.r = bus.connect(settings.redis_url)
        self.mtx = mediamtx or MediaMTX(
            settings.mediamtx_api, settings.mediamtx_rtsp, settings.public_host, settings.view_ports
        )
        self.cameras: dict[str, Camera] = {}
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._monitor = threading.Thread(target=self._watch, name="supervisor", daemon=True)

    # ------------------------------------------------------------ lifecycle
    def start(self) -> None:
        for camera_id, profile_json in self.r.hgetall(bus.REGISTRY).items():
            try:
                self.add(camera_id, CameraProfile.model_validate_json(profile_json), persist=False)
            except Exception as exc:
                log.error("could not restore camera %s: %s", camera_id, exc)
        self._monitor.start()

    def shutdown(self) -> None:
        """Stop every camera process; the registry is kept for the next start."""
        self._stop.set()
        with self._lock:
            for cam in self.cameras.values():
                self._stop_process(cam)

    # -------------------------------------------------------------- cameras
    def add(self, camera_id: str, profile: CameraProfile, persist: bool = True) -> Camera:
        check_camera_id(camera_id)
        with self._lock:
            if camera_id in self.cameras:
                raise KeyError(f"camera {camera_id} already exists")
            if len(self.cameras) >= self.settings.max_cameras:
                raise AdmissionError(f"this gateway is full ({self.settings.max_cameras} cameras, MAIA_MAX_CAMERAS)")
            cam = Camera(camera_id, profile)
            self._place(cam)
            if persist:
                self.r.hset(bus.REGISTRY, camera_id, profile.model_dump_json())
            self.cameras[camera_id] = cam
            if profile.source.startswith(("rtsp://", "rtsps://")):
                cam.own_path = self.mtx.own_path(profile.source)
            if cam.relayed:
                cam.relay_pending = True
                self._try_relay(cam)
            else:
                self._start_process(cam)
            return cam

    def _try_relay(self, cam: Camera) -> None:
        """Create the camera's MediaMTX path; start its pipeline once that works."""
        try:
            self.mtx.add(cam.id, cam.profile.source)
        except MediaMTXError as exc:
            cam.relay_tries += 1
            cam.error = str(exc)
            cam.next_start = time.monotonic() + min(30.0, 2.0 ** min(cam.relay_tries, 5))
            if cam.relay_tries == 1:
                log.warning("camera %s waits for MediaMTX: %s", cam.id, exc)
            return
        cam.relay_pending, cam.relay_tries, cam.error = False, 0, None
        self._start_process(cam)

    def update(self, camera_id: str, profile: CameraProfile) -> Camera:
        with self._lock:
            self.remove(camera_id, forget=False)
            return self.add(camera_id, profile)

    def remove(self, camera_id: str, forget: bool = True) -> None:
        with self._lock:
            cam = self.cameras.pop(camera_id, None)
            if cam is None:
                raise KeyError(f"no camera {camera_id}")
            self._stop_process(cam)
            if forget:
                if cam.relayed and not cam.relay_pending:
                    try:
                        self.mtx.remove(camera_id)
                    except MediaMTXError as exc:
                        log.warning("MediaMTX: %s", exc)
                self.r.hdel(bus.REGISTRY, camera_id)
                self.r.delete(bus.STATS.format(camera_id))

    def restart(self, camera_id: str) -> None:
        with self._lock:
            cam = self.cameras[camera_id]
            self._stop_process(cam)
            cam.restarts, cam.next_start, cam.ended, cam.failed = 0, 0.0, False, False
            if cam.relay_pending:
                cam.relay_tries = 0
                self._try_relay(cam)
            else:
                self._start_process(cam)

    def status(self, camera_id: str) -> dict:
        cam = self.cameras[camera_id]
        raw = self.r.get(bus.STATS.format(camera_id))
        stats = json.loads(raw) if raw else {}
        if cam.relay_pending:
            state = "waiting-for-mediamtx"
        elif cam.ended:
            state = "ended"
        elif cam.failed:
            state = "failed"
        elif cam.running:
            state = stats.get("state", "starting")
        else:
            state = "restarting" if cam.next_start else "stopped"
        return {
            "id": cam.id,
            "state": state,
            "source": cam.profile.source,
            "relayed": cam.relayed,
            "viewable": cam.viewable,
            "mediamtx_path": cam.mtx_path if cam.viewable else None,
            "decode": cam.decode,
            "gpu": cam.gpu,
            "restarts": cam.restarts,
            "last_exit": cam.last_exit,
            "notes": cam.notes + stats.get("notes", []),
            "error": cam.error if cam.relay_pending else stats.get("error"),
            "stats": stats,
            "profile": cam.profile.model_dump(),
        }

    # ------------------------------------------------------------ internals
    def _place(self, cam: Camera) -> None:
        wants_gpu = cam.profile.decode in ("auto", "nvidia")
        cam.decode = cam.profile.decode
        if not (wants_gpu and self.settings.gpus > 0):
            return
        load = {g: 0 for g in range(self.settings.gpus)}
        for other in self.cameras.values():
            if other.gpu is not None:
                load[other.gpu] += 1
        gpu, used = min(load.items(), key=lambda kv: kv[1])
        if used < self.settings.sessions_per_gpu:
            cam.gpu = gpu
        elif cam.profile.decode == "nvidia":
            raise AdmissionError(
                f"all {self.settings.gpus} GPU(s) have {self.settings.sessions_per_gpu} decode sessions"
            )
        else:
            cam.decode = "cpu"
            cam.notes.append("GPUs full: decoding on the CPU")

    def _start_process(self, cam: Camera) -> None:
        self.r.delete(bus.STATS.format(cam.id))  # the previous process's final stats
        if cam.relayed or cam.own_path:  # read through MediaMTX (works inside Docker, too)
            url = self.mtx.ingest_url(cam.mtx_path)
        else:
            url = cam.profile.source
        publishes = cam.profile.viewable and not cam.profile.relayed and not cam.own_path
        publish_url = self.mtx.ingest_url(cam.id) if publishes else None
        profile = cam.profile.model_copy(update={"decode": cam.decode})
        s = self.settings
        cam.process = _ctx.Process(
            target=camera_main,
            args=(cam.id, profile.model_dump_json(), url, s.redis_url, s.clips_dir, cam.gpu, publish_url),
            name=f"camera-{cam.id}",
        )
        cam.process.start()
        cam.started_at, cam.next_start = time.monotonic(), 0.0
        log.info("camera %s started (pid %s, decode %s%s)", cam.id, cam.process.pid, cam.decode,
                 f", gpu {cam.gpu}" if cam.gpu is not None else "")  # fmt: skip

    @staticmethod
    def _stop_process(cam: Camera, timeout: float = 5.0) -> None:
        p = cam.process
        cam.process = None
        if p is None:
            return
        if p.is_alive():
            p.terminate()  # SIGTERM: the pipeline stops cleanly and removes its ring
            p.join(timeout)
            if p.is_alive():
                p.kill()
                p.join(timeout)
        p.close()

    def _watch(self) -> None:
        while not self._stop.wait(0.5):
            with self._lock:
                now = time.monotonic()
                for cam in self.cameras.values():
                    if cam.process is not None and not cam.process.is_alive():
                        cam.last_exit = cam.process.exitcode
                        if cam.last_exit in (EXIT_ENDED, EXIT_FATAL):
                            # played once as asked, or a source that cannot work (unsupported
                            # codec): restarting would not change anything
                            cam.process.close()
                            cam.process, cam.next_start = None, 0.0
                            cam.ended, cam.failed = cam.last_exit == EXIT_ENDED, cam.last_exit == EXIT_FATAL
                            log.info("camera %s %s; not restarting", cam.id, "ended" if cam.ended else "failed")
                            continue
                        uptime = now - cam.started_at
                        cam.restarts = 0 if uptime > 60 else cam.restarts + 1
                        delay = min(60.0, 2.0 ** min(cam.restarts, 6) / 2)
                        cam.process.close()
                        cam.process, cam.next_start = None, now + delay
                        log.warning("camera %s exited (%s) after %.0fs; restart in %.0fs",
                                    cam.id, cam.last_exit, uptime, delay)  # fmt: skip
                    elif cam.process is None and cam.next_start and now >= cam.next_start:
                        if cam.relay_pending:
                            self._try_relay(cam)
                        else:
                            self._start_process(cam)
