# maia-gateway

**An open camera gateway for video analytics: decode each camera once, and let any pipeline
read ready frames from shared memory.**

maia-gateway does what a video management gateway such as NVIDIA VST does (camera
ingest, live viewing, event clips, a control API) with open tools on any hardware, and then
goes one step further: every decoded frame lands in a shared-memory frame bus that analytics
pipelines read without copying, and hand to [maia-infer](https://github.com/jemaia-elwrose/maia-infer)
or Triton by reference.

```mermaid
flowchart LR
    C[Cameras<br/>RTSP] --> M[MediaMTX<br/>one pull per camera]
    M --> V[Viewers<br/>WebRTC · HLS · RTSP]
    M --> I["Ingest, one process per camera<br/>decode (NVIDIA · Apple · CPU)<br/>profile steps · sampling"]
    I --> R[("Frame ring<br/>shared memory")]
    I --> X[Frame index<br/>Redis Streams]
    I --> K[Clip buffer<br/>encoded packets → MP4]
    X --> P[Pipelines<br/>consumer groups]
    R -. read in place .-> P
    P --> F[maia-infer or Triton]
    A[Control API<br/>FastAPI] -. manages .-> I
```

## Why it looks like this

**Decode once, read many times.** Each camera is decoded by one process into a ring of
frames in shared memory ([maia-ring](https://github.com/jemaia-elwrose/maia-ring)). Any number
of pipelines read those frames in place; a 1080p frame costs a reader about 5 µs, whatever
its size. A request to maia-infer carries a ~200-byte reference instead of 6 MB of pixels
(1080p round trip: 4.6 ms with pixels, 0.28 ms by reference).

**Video and inference stay apart, deliberately.** DeepStream runs decode, preprocessing and
inference in one pipeline and avoids a memory copy. maia-gateway pays that one copy (decoded
frame into the ring) so that workers scale on their own, model frameworks mix (maia-infer for
light models, Triton for heavy ones), and use cases change without touching the video side.

**The camera gets one connection.** MediaMTX pulls each camera once; the gateway's ingest,
people watching live, and any other consumer all read MediaMTX.

**Each part has one job.**

| Part | Does | Never does |
|---|---|---|
| Gateway | camera ingest, decode, camera-level image steps, sampling, frame bus, clips, API | model preprocessing, inference, use-case logic |
| Workers (maia-infer, Triton) | model preprocessing and inference | touch video |
| Pipelines | use-case logic: what to detect, when to raise an event | decode video |

## Quick start (Docker)

```bash
docker compose up --build
```

That starts MediaMTX, Redis, the gateway and a simulated camera, `sim-1`. Then:

```bash
curl localhost:8080/cameras                        # state, decoder, fps, GOP per camera
curl localhost:8080/cameras/sim-1/snapshot -o snap.jpg
open http://localhost:8889/sim-1/                  # live view (WebRTC), on the same machine or LAN
open http://localhost:8888/sim-1/                  # live view (HLS): works through SSH/VS Code tunnels
curl -XPOST localhost:8080/cameras/sim-1/events -H 'content-type: application/json' \
     -d '{"label": "person"}'                       # cuts a clip: 5 s before, 10 s after
curl localhost:8080/clips                          # ...and the clip, when it is written
open http://localhost:8080/docs                    # every endpoint
```

Add your cameras in `docker/cameras.yml`, or while it runs:

```bash
curl -XPOST localhost:8080/cameras -H 'content-type: application/json' -d '{
  "id": "gate-1",
  "profile": {"source": "rtsp://user:pass@192.168.1.64/stream1", "sampling": {"fps": 5}}}'
```

NVIDIA GPU decoding (NVIDIA Container Toolkit): `docker compose -f docker-compose.yml -f docker-compose.gpu.yml up --build`.

Every port is published on `127.0.0.1` only. To reach the stack from other machines, run
with `MAIA_BIND=0.0.0.0` **and** `MAIA_API_KEY=...`: Docker's published ports bypass host
firewalls like ufw, and an exposed API is found by internet scanners within minutes. WebRTC
from other machines also needs the host's address in `webrtcAdditionalHosts`
(`docker/mediamtx.yml`) and port 8189 reachable; through a tunnel, use the HLS page.

A port already taken on the host? Change only the host side; inside the compose network
everything keeps its standard port: `MAIA_RTSP_PORT=7554 docker compose up` (also
`MAIA_API_PORT`, `MAIA_HLS_PORT`, `MAIA_WEBRTC_PORT`, `MAIA_REDIS_PORT`).

If MediaMTX is not reachable yet (still starting, or misconfigured), the gateway starts
anyway: relayed cameras show `waiting-for-mediamtx` with the reason in `GET /cameras` and
are retried in the background.

## Reading frames in a pipeline

```python
from maia_gateway.bus import FrameStream
from maia_infer import Client, detections

frames = FrameStream("gate-1")  # Redis at localhost:6379 by default
client = Client("tcp://localhost:5555")

for frame in frames.follow():  # the newest frame each time
    res = client.infer("yolo", frame)  # by reference: the worker reads the ring
    for d in detections(res):
        print(frame.meta.frame_index, d["name"], d["conf"])
```

| Mode | Call | For |
|---|---|---|
| Newest frame | `follow()` | live analytics: a slow model skips frames instead of falling behind |
| Every frame, in order | `follow(every=True)` | counting, tracking without gaps; overwritten frames are counted in `missed` |
| Shared between replicas | `follow(group="counting", consumer="a")` | several copies of one pipeline split a camera's frames (Redis consumer group) |
| References only | `follow(read=False)` | forward `FrameRef`s to a worker or Triton without touching the pixels |

`examples/detect_from_bus.py` is a complete pipeline (YOLO through maia-infer, an event and a
clip when a person appears). `examples/triton_shm_sketch.py` shows the Triton path: the ring is
registered with Triton as system shared memory and each request names a slot. It is a sketch,
not yet tested against a Triton server.

## Camera profiles

```yaml
cameras:
  gate-1:
    source: rtsp://user:pass@10.0.0.21/stream1   # or file:///video.mp4, or test://ball
    decode: auto            # auto | nvidia | apple | cpu
    pixfmt: bgr             # bgr | rgb | gray
    steps:                  # applied in this order, each at most once
      - rotate: 90          # 0 | 90 | 180 | 270
      - flip: horizontal    # horizontal | vertical
      - crop: [0.0, 0.3, 1.0, 1.0]        # region of interest, fractions: left, top, right, bottom
      - resize: [1280, 720]
      - privacy_mask: [[0.8, 0.0, 1.0, 0.2]]   # blacked out in every frame
      - enhance: night      # gamma and contrast lift for dark scenes
    sampling:
      mode: fps             # fps | iframes | motion
      fps: 5                # fractions work: 0.5 = one frame every 2 s
    ring: {slots: 32}
    clips: {pre_s: 5, post_s: 10}
    relay: true             # pull through MediaMTX (live view); false = ingest reads the camera itself
```

Change a profile while the camera runs with `PATCH /cameras/{id}` (only the fields you send);
only that camera's pipeline is rebuilt. Unknown keys are errors, not silently ignored.

**Sampling modes:**

| Mode | What reaches the ring | Decode cost | Good for |
|---|---|---|---|
| `fps` | an even rate | every frame decoded; dropped frames skip conversion and scaling | tracking, traffic, counting |
| `iframes` | only key frames (one per GOP; the API reports each camera's GOP in `gop_s`) | very low: other frames are never decoded | occupancy, PPE spot checks, parking |
| `motion` | frames where at least `motion_area` of the image changed (default 0.2 %, enough for a distant person), at most `fps`; one every `keepalive_s` anyway | every frame decoded | night warehouses, empty corridors |

## Hardware decoding

`decode: auto` takes the first the machine has: NVIDIA (NVDEC), Apple (VideoToolbox), CPU.
`GET /health` and `maia-gateway decoders` show what is available; each camera reports the
decoder it got.

| Target | Decoders | Run it |
|---|---|---|
| NVIDIA GPU | `nvh264dec`, `nvh265dec` | Docker with the GPU override, or native |
| Apple Silicon | `vtdec_hw` | native only: Docker on a Mac runs a Linux VM without VideoToolbox |
| CPU | `avdec_h264`, `avdec_h265` | anywhere |

Decode throughput, `maia-gateway bench` (decode + conversion to BGR, one stream):

| Machine | 720p | 1080p |
|---|---|---|
| 2-core x86 cloud VM, CPU (`avdec_h264`) | 469 fps | 235 fps |

At 1080p and 25 fps, that VM decodes about 9 full-rate cameras on the CPU; `iframes` mode
cuts the decode cost by the GOP length (25-50× for typical cameras).

## Scaling and robustness

- **One process per camera.** A stream that fails, stalls (no frame for 15 s) or crashes is
  restarted with backoff (1, 2, 4 ... 60 s) and never affects other cameras.
- **Admission control.** `MAIA_MAX_CAMERAS` cameras per gateway; `MAIA_GPUS` and
  `MAIA_SESSIONS_PER_GPU` cap NVIDIA decode sessions, cameras go to the least-loaded GPU,
  and an `auto` camera falls back to the CPU when every GPU is full.
- **Backpressure, not queues.** Rings overwrite their oldest frame; live readers take the
  newest. Nothing grows without bound.
- **Rings clean up after themselves**, and a camera restarted by the supervisor makes a new
  ring that readers reattach to on their own.
- **The registry is in Redis**, so cameras come back when the gateway restarts.
- **Multi-machine** is the next step: one gateway per machine owning its cameras, pipelines
  reading on the same machine. Nothing in v0.1 blocks it.

## Control API

| Method | Path | Does |
|---|---|---|
| GET | `/cameras` | every camera: state, decoder, size, fps out, GOP, restarts |
| POST | `/cameras` | add a camera `{id, profile}` |
| GET · PATCH · DELETE | `/cameras/{id}` | one camera; change its profile; remove it |
| POST | `/cameras/{id}/restart` | restart its process |
| GET | `/cameras/{id}/snapshot` | newest frame as JPEG |
| GET | `/cameras/{id}/view` | WebRTC, HLS and RTSP links |
| POST | `/cameras/{id}/events` | raise an event; returns the clip id |
| GET | `/clips`, `/clips/{camera}/{id}`, `/clips/{camera}/{id}.mp4` | list clips, metadata, video |
| GET | `/health`, `/metrics` | Redis, MediaMTX, decoders; Prometheus metrics |

Set `MAIA_API_KEY` to require an `X-API-Key` header. The CLI talks to the API too:
`maia-gateway cameras`, `maia-gateway cameras add gate-1 rtsp://... --fps 5`,
`maia-gateway event gate-1 --label person`.

## Event clips

Clips are cut from the camera's encoded stream, not from the frame ring: full frame rate,
no re-encode. Each camera keeps the last `pre_s` seconds of packets (starting at a key frame);
an event writes those plus the next `post_s` seconds to an MP4 with a JSON sidecar (events,
labels, your metadata), and announces it on the Redis stream `clips`. An event that arrives
while a clip is open extends that clip.

## Native install

GStreamer and its Python bindings come from the system:

```bash
# Ubuntu / Debian
sudo apt install python3-gi gir1.2-gstreamer-1.0 gir1.2-gst-plugins-base-1.0 gstreamer1.0-tools \
  gstreamer1.0-plugins-{base,good,bad,ugly} gstreamer1.0-libav gstreamer1.0-rtsp redis-server
python3 -m venv --system-site-packages .venv && . .venv/bin/activate

# macOS (Apple Silicon: hardware decoding works natively)
brew install gstreamer gobject-introspection pkg-config cairo redis
python3 -m venv .venv && . .venv/bin/activate
pip install pygobject          # the GStreamer bindings, built for this venv's Python

pip install -e ".[dev,infer]"
redis-server --daemonize yes                           # or: brew services start redis
maia-gateway serve --config examples/cameras-local.yml # synthetic cameras, no MediaMTX needed
```

For relayed RTSP cameras and live viewing, run MediaMTX too (a single binary from its
[releases](https://github.com/bluenviron/mediamtx/releases)) with `api: true` in its
config, and point `MAIA_MEDIAMTX_API` / `MAIA_MEDIAMTX_RTSP` at it. MediaMTX allows its
API from localhost only by default, which is fine when both run on the same machine:

```bash
./mediamtx docker/mediamtx.yml
```

## Measured, not assumed

| What | Result | Where |
|---|---|---|
| Decoded frame into the ring (1080p) | 0.74 ms (x86 VM), 0.11 ms (Apple Silicon) | `maia-ring bench` |
| Reading a frame in place (any size) | 2–5 µs | `maia-ring bench` |
| Frame to maia-infer and back (1080p) | 0.28 ms by reference vs 4.6 ms with pixels | maia-infer `bench/bench_transport.py` |
| CPU decode (1080p) | 235 fps on a 2-core VM | `maia-gateway bench` |

Still to measure: glass-to-ring latency (camera timestamp to frame in the ring) and the CPU
cost of cutting a clip during live ingest.

## Project layout

```
src/maia_gateway/
  ingest/pipeline.py   GStreamer pipeline: source, decode, steps, sampling, ring
  ingest/decoders.py   hardware decoder choice
  ingest/clips.py      pre-event packet buffer, MP4 writer
  ingest/process.py    a camera's process: pipeline + Redis + clips + commands
  supervisor.py        one process per camera, restarts, admission control
  bus.py               Redis Streams frame index, FrameStream reader
  mediamtx.py          MediaMTX paths and viewing links
  api/app.py           FastAPI control API
  config.py            camera profiles
  cli.py               maia-gateway serve | cameras | event | decoders | bench
examples/              a detection pipeline; a Triton shared-memory sketch
docker/                image (CPU + NVIDIA), MediaMTX config, cameras
tests/                 33 tests: real GStreamer pipelines, Redis, MediaMTX and RTSP
```

## Development

```bash
pip install -e ".[dev]"
MEDIAMTX=/path/to/mediamtx pytest -q    # ~60 s; tests needing GStreamer, Redis or MediaMTX skip without them
ruff check . && ruff format --check .
```

## License

Apache-2.0. GStreamer (LGPL) is used as a dynamically linked system library; MediaMTX is MIT.
