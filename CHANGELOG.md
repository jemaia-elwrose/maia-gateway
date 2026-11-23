# Changelog

## 0.1.3
- NVIDIA: the GPU is chosen by decoder element (`nvh264dec` for GPU 0, `nvh264device1dec`
  for GPU 1, ...). Setting `cuda-device-id` failed: the property is read-only.
- Decoders are matched to the stream's profile before use: a stream a hardware decoder
  cannot handle (NVDEC: no 4:4:4; some GPUs: no 10-bit) goes to the next decoder, with a
  note in the camera's status, instead of failing to negotiate.
- Test cameras (`test://`) always produce 4:2:0, like real cameras; left free, x264 on
  GStreamer 1.20 picked High 4:4:4, which NVDEC rejected. `?format=Y444` makes such a stream
  on purpose.
- Docker GPU: the image no longer ships GStreamer's plugin cache. It was written at build
  time without a GPU, so NVIDIA's decoders stayed hidden at run time even with one.
- `maia-gateway decoders` also shows the Python, PyGObject and GStreamer in use and where
  plugins load from; link errors name the reason and the stream format.

## 0.1.2
- Apple VideoToolbox: a parser before every decoder converts the byte-stream H.264/H.265
  (which the clip branch needs) into the format the decoder accepts; `vtdec_hw` takes AVC
  only and stalled without it.
- docker compose publishes every port on 127.0.0.1 by default; `MAIA_BIND=0.0.0.0` (with
  `MAIA_API_KEY`) exposes them. Docker's published ports bypass host firewalls, and an
  exposed API was found by internet scanners within minutes.
- Live view through tunnels: `GET /cameras/{id}/view` adds the HLS player page (plain HTTP);
  WebRTC also accepts media over TCP 8189 when UDP is blocked.
- README: macOS install with `pip install pygobject`.

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
