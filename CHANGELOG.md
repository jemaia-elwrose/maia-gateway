# Changelog

## 0.1.1
- Docker: MediaMTX refused the gateway's API calls (its default allows the API from
  localhost only). `docker/mediamtx.yml` now allows the API from private networks; the API
  port is never published. `MAIA_MEDIAMTX_API` also accepts `http://user:pass@host:9997`.
- MediaMTX being unreachable no longer stops the gateway: relayed cameras show
  `waiting-for-mediamtx` with the reason and are retried with backoff. A camera from
  `--config` that cannot be added is logged and skipped.
- Host ports are set in docker compose (`MAIA_RTSP_PORT=7554 docker compose up`); viewing
  links follow them (`MAIA_VIEW_*_PORT`).
- `examples/cameras-local.yml` for native runs without MediaMTX (Mac hardware decoding).

## 0.1.0
- First release: camera gateway with a shared-memory frame bus for video analytics.
- Ingest: one process per camera; RTSP (relayed through MediaMTX), file and synthetic
  sources; H.264 and H.265; NVIDIA, Apple and CPU decoding chosen automatically.
- Camera profiles: rotate, flip, crop, resize, privacy masks, night enhancement; BGR, RGB
  or gray; changed live through the API.
- Sampling: fps (fractions too), I-frames only (decodes key frames only), motion-gated with
  keep-alive frames. The camera's GOP is measured and reported.
- Frame bus: maia-ring shared-memory rings plus a Redis Streams index; `FrameStream` reads
  the newest frame, every frame, or shares frames across a consumer group.
- Event clips from the encoded stream (no re-encode), overlapping events merged, JSON sidecars.
- Supervisor: restarts with backoff, stall watchdog, admission control, GPU placement,
  camera registry in Redis.
- Control API (FastAPI), CLI, Prometheus metrics, decode benchmark, Docker image for CPU and
  NVIDIA, docker compose with MediaMTX, Redis and a simulated camera.
