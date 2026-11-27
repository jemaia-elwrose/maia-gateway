# Changelog

## 0.1.6
- `examples/detect_from_bus.py` uses maia-infer's RF-DETR worker by default (`--service`,
  Apache-2.0); `--service yolo` for Ultralytics YOLO (AGPL-3.0). `[infer]` pins maia-infer 0.5.0.
- The version is written once, in `maia_gateway/__init__.py`; `pyproject.toml` reads it.

## 0.1.5
- Every event also goes to a Redis stream, `events` (camera, clip id, time, label, meta, clip
  window), for services that act on events, such as maia-vlm.
- `POST /clips/<camera>/<clip>/verdicts` attaches a verdict to a clip (also while it records);
  clips list their `verdicts`, and each goes to a `verdicts` stream. Verdicts annotate: they
  never change the clip or its events.
- `examples/detect_from_bus.py` puts the detection's box, confidence and the frame size in
  each event.

## 0.1.4
- Video files as cameras, like VST: `loop: false` plays a file once (the camera then shows
  `ended` and is not restarted); `publish: true` pushes a file or test source into MediaMTX,
  without re-encoding, so it can be watched live like a camera. `./videos` is mounted at
  `/videos` in docker compose.
- Files loop seamlessly with segment seeks, and every timestamp (rate, I-frame marks,
  clips, ring) is running time, which keeps increasing across loops: the published stream
  stays continuous and clips may span a loop. Known limit: files with B-frames hiccup in
  HLS at each loop (re-encode with `-bf 0` for viewing).
- Fix: camera processes' exit codes never reached the supervisor (multiprocessing ignores a
  target's return value); they now go through `sys.exit`.
- Clips: one clock per clip and strictly increasing decode times (B-frames, missing DTS).
- Files with audio or other extra tracks play (the tracks are ignored). A source with no
  H.264/H.265 video (a VP9 phone export, say) is marked `failed`, with the tracks found and
  an ffmpeg command to convert it, instead of restarting forever.
- Docker: rings are given to group `MAIA_RING_GID` (default 1000), so scripts on the host can
  read the frames of a gateway running as root (maia-ring 0.1.3).
- Fix: snapshots failed for frames whose rows are not a multiple of 4 bytes (e.g. 606 px wide
  BGR, a portrait video); GStreamer pads raw rows to 4 bytes.
- Fix: a camera whose source is already a stream on the gateway's own MediaMTX (a webcam
  published with ffmpeg) was relayed into a path that pulled from itself, which dropped the
  publisher. Such a source is now read as it is, and viewed on its own path.
- README: webcam test steps for macOS (download MediaMTX, then three terminals).
- `examples/check_by_reference.py`: proves frames travel by reference from the gateway to a
  maia-infer worker (325 bytes instead of 6.2 MB per 1080p frame, same pixels).

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
