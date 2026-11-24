"""MediaMTX: the only thing that talks to cameras. The gateway adds one path per camera
through MediaMTX's control API; ingest and viewers then read that path."""

from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit

import httpx


class MediaMTXError(RuntimeError):
    pass


class MediaMTX:
    def __init__(
        self,
        api: str,
        rtsp: str,
        public_host: str = "localhost",
        ports: dict[str, int] | None = None,
        timeout: float = 3.0,
    ) -> None:
        # credentials may ride in the URL (http://user:pass@mediamtx:9997) for a MediaMTX
        # user with the "api" permission; they are sent as basic auth, never logged
        parts = urlsplit(api.rstrip("/"))
        auth = (parts.username or "", parts.password or "") if parts.username else None
        host = parts.hostname + (f":{parts.port}" if parts.port else "")
        self.api = urlunsplit((parts.scheme, host, parts.path, "", ""))
        self.rtsp = rtsp.rstrip("/")
        self.public_host = public_host
        self.ports = {"rtsp": 8554, "hls": 8888, "webrtc": 8889, **(ports or {})}
        self.http = httpx.Client(timeout=timeout, auth=auth)

    def _call(self, method: str, path: str, json: dict | None = None) -> httpx.Response:
        try:
            res = self.http.request(method, f"{self.api}{path}", json=json)
        except httpx.HTTPError as exc:
            raise MediaMTXError(f"MediaMTX API unreachable at {self.api}: {exc}") from exc
        if res.status_code == 401:
            raise MediaMTXError(
                f"MediaMTX refused the API call ({method} {path}): by default it allows its API from localhost "
                "only. Allow the gateway's address under authInternalUsers (action: api) in mediamtx.yml, "
                "or give the gateway a user: MAIA_MEDIAMTX_API=http://user:pass@host:9997"
            )
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

    def own_path(self, source: str) -> str | None:
        """The path name if ``source`` is a stream on this MediaMTX already (a webcam published
        with ffmpeg, say). Relaying it would turn that path into one that pulls from itself and
        drop the publisher, so such cameras are read as they are."""
        s = urlsplit(source)
        path = s.path.strip("/")
        if s.scheme not in ("rtsp", "rtsps") or not path:
            return None
        ours = urlsplit(self.rtsp)
        hosts = {ours.hostname, "localhost", "127.0.0.1", "::1", self.public_host}
        ports = {ours.port or 554, self.ports["rtsp"]}
        return path if s.hostname in hosts and (s.port or 554) in ports else None

    def ingest_url(self, camera_id: str) -> str:
        return f"{self.rtsp}/{camera_id}"

    def view(self, camera_id: str) -> dict[str, str]:
        h, p = self.public_host, self.ports
        return {
            "webrtc": f"http://{h}:{p['webrtc']}/{camera_id}/",  # lowest latency; media over UDP/TCP 8189
            "hls_page": f"http://{h}:{p['hls']}/{camera_id}/",  # plain HTTP: works through any tunnel
            "hls": f"http://{h}:{p['hls']}/{camera_id}/index.m3u8",
            "rtsp": f"rtsp://{h}:{p['rtsp']}/{camera_id}",
        }
