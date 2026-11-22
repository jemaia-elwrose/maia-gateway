"""MediaMTX: the only thing that talks to cameras. The gateway adds one path per camera
through MediaMTX's control API; ingest and viewers then read that path."""

from __future__ import annotations

import httpx


class MediaMTXError(RuntimeError):
    pass


class MediaMTX:
    def __init__(self, api: str, rtsp: str, public_host: str = "localhost", timeout: float = 3.0) -> None:
        self.api = api.rstrip("/")
        self.rtsp = rtsp.rstrip("/")
        self.public_host = public_host
        self.http = httpx.Client(timeout=timeout)

    def _call(self, method: str, path: str, json: dict | None = None) -> httpx.Response:
        try:
            res = self.http.request(method, f"{self.api}{path}", json=json)
        except httpx.HTTPError as exc:
            raise MediaMTXError(f"MediaMTX API unreachable at {self.api}: {exc}") from exc
        if res.status_code >= 400:
            raise MediaMTXError(f"MediaMTX {method} {path}: {res.status_code} {res.text.strip()}")
        return res

    def ok(self) -> bool:
        try:
            self._call("GET", "/v3/paths/list")
            return True
        except MediaMTXError:
            return False

    def add(self, camera_id: str, source: str) -> None:
        conf = {"source": source, "sourceOnDemand": False, "rtspTransport": "tcp"}
        try:
            self._call("POST", f"/v3/config/paths/add/{camera_id}", conf)
        except MediaMTXError as exc:
            if "already exists" not in str(exc):
                raise
            self._call("PATCH", f"/v3/config/paths/patch/{camera_id}", conf)

    def remove(self, camera_id: str) -> None:
        try:
            self._call("DELETE", f"/v3/config/paths/delete/{camera_id}")
        except MediaMTXError as exc:
            if "not found" not in str(exc):
                raise

    def path(self, camera_id: str) -> dict | None:
        try:
            return self._call("GET", f"/v3/paths/get/{camera_id}").json()
        except MediaMTXError:
            return None

    def ingest_url(self, camera_id: str) -> str:
        return f"{self.rtsp}/{camera_id}"

    def view(self, camera_id: str) -> dict[str, str]:
        h = self.public_host
        return {
            "webrtc": f"http://{h}:8889/{camera_id}/",
            "hls": f"http://{h}:8888/{camera_id}/index.m3u8",
            "rtsp": f"rtsp://{h}:8554/{camera_id}",
        }
