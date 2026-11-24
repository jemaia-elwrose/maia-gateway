"""One camera's ingest pipeline.

    source ─ parsebin ─ h26xparse ─ tee ─┬─ queue ─ appsink "packets"     (encoded: clips, GOP, I-frame marks)
                                          └─ queue ─ h26xparse ─ decoder ─(rate probe)─ flip ─ crop ─ scale
                                                     ─ enhance ─ convert ─ appsink "frames"  (into the ring)

Timestamps are the pipeline's *running time* everywhere (rate thinning, I-frame marks, clips,
the ring): it keeps increasing when a file loops, where the file's own timestamps start
again at 0. Files loop with segment seeks, without a flush, so the stream published to
MediaMTX stays continuous too.

Frames are thinned to the target rate by a probe right after the decoder, using their
timestamps, so dropped frames never pay for conversion or scaling. (GStreamer's videorate
element aborts on streams without frame durations, which RTSP cameras often send.)

``parsebin`` adds the right depayloader and parser for whatever arrives (RTP from RTSP,
MP4 or MKV files, a raw H.264 test stream), so the code below only deals with parsed
H.264 or H.265. The decoder is linked once the codec is known.
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from urllib.parse import parse_qs, urlparse

import numpy as np
from maia_ring import DISCONTINUITY, IFRAME, MOTION, FrameRef, RingWriter

from ..config import CameraProfile, Crop, Enhance, Flip, PrivacyMask, Resize, Rotate, ring_name
from . import decoders
from .gst import GLib, Gst

log = logging.getLogger("maia_gateway.ingest")

CODECS = {"video/x-h264": "h264", "video/x-h265": "h265"}
TEST_ENCODERS = ["x264enc", "openh264enc", "vtenc_h264_hw", "vtenc_h264", "avenc_h264"]
FLIP = {90: "clockwise", 180: "rotate-180", 270: "counterclockwise"}
PIXFMT_CAPS = {"bgr": ("BGR", 3), "rgb": ("RGB", 3), "gray": ("GRAY8", 1)}


@dataclass
class Packet:
    arrival_ns: int  # wall clock when it reached the gateway
    pts: int  # stream timestamps, ns (Gst.CLOCK_TIME_NONE if unknown)
    dts: int
    duration: int
    key: bool
    data: bytes


@dataclass
class Stats:
    state: str = "starting"
    codec: str | None = None
    decoder: str | None = None
    decoder_kind: str | None = None
    width: int | None = None
    height: int | None = None
    packets: int = 0
    decoded: int = 0
    written: int = 0
    gop_s: float | None = None
    started_ns: int = field(default_factory=time.time_ns)
    last_frame_ns: int | None = None
    error: str | None = None
    notes: list = field(default_factory=list)
    _rate_window: deque = field(default_factory=lambda: deque(maxlen=50))

    @property
    def fps_out(self) -> float | None:
        w = self._rate_window
        if len(w) < 2 or w[-1] == w[0]:
            return None
        return round((len(w) - 1) / ((w[-1] - w[0]) / 1e9), 2)

    def as_dict(self) -> dict:
        d = {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
        d["fps_out"] = self.fps_out
        return d


def _running(segment: Gst.Segment | None, t: int) -> int:
    """Stream timestamp -> running time (monotonic across file loops). Can be negative: with
    B-frames the first decode timestamps come before the start of the segment."""
    if t == Gst.CLOCK_TIME_NONE or segment is None:
        return t
    sign, value = segment.to_running_time_full(Gst.Format.TIME, t)
    return value if sign > 0 else -value if sign < 0 else Gst.CLOCK_TIME_NONE


def test_source(url: str) -> str:
    """test://ball?size=1280x720&fps=25&gop=50 -> a live synthetic H.264 camera."""
    u = urlparse(url)
    q = {k: v[0] for k, v in parse_qs(u.query).items()}
    width, height = (int(x) for x in q.get("size", "1280x720").split("x"))
    fps, gop, pattern = int(q.get("fps", 25)), int(q.get("gop", 50)), u.netloc or "ball"
    # 4:2:0 like real cameras; left free, some encoders pick 4:4:4, which most hardware
    # decoders (NVDEC included) cannot decode. ?format=Y444 makes such a stream on purpose.
    fmt = q.get("format", "I420")
    encoder = next((e for e in TEST_ENCODERS if decoders.available(e)), None)
    if encoder is None:
        raise RuntimeError("test:// sources need an H.264 encoder (x264enc, openh264enc or vtenc_h264)")
    settings = {
        "x264enc": f"x264enc tune=zerolatency speed-preset=ultrafast key-int-max={gop}",
        "openh264enc": f"openh264enc gop-size={gop}",
        "avenc_h264": f"avenc_h264 gop-size={gop}",
    }.get(encoder, f"{encoder} max-keyframe-interval={gop}")
    return (
        f"videotestsrc is-live=true pattern={pattern} ! video/x-raw,width={width},height={height},"
        f"framerate={fps}/1 ! videoconvert ! video/x-raw,format={fmt} ! {settings} ! h264parse"
    )


class Ingest:
    """Runs a camera's pipeline and writes its frames into the camera's ring.

    ``publish(ref)`` is called for every frame written (the gateway announces it on Redis);
    ``on_packet(packet)`` for every encoded access unit (the clip buffer)."""

    def __init__(
        self,
        camera_id: str,
        profile: CameraProfile,
        url: str | None = None,
        publish: Callable[[FrameRef], None] | None = None,
        on_packet: Callable[[Packet], None] | None = None,
        loop_files: bool | None = None,
        gpu: int | None = None,
        publish_url: str | None = None,
    ) -> None:
        self.gpu = gpu
        self.publish_url = publish_url
        self.camera_id = camera_id
        self.profile = profile
        self.url = url or profile.source
        self.publish = publish or (lambda ref: None)
        self.on_packet = on_packet or (lambda packet: None)
        self.loop_files = profile.loop if loop_files is None else loop_files
        self.stats = Stats()
        self.ring: RingWriter | None = None
        self.caps_string: str | None = None  # encoded caps, for writing clips
        self._key_pts: deque[int] = deque(maxlen=256)
        self._last_key_pts: int | None = None
        self._next_due: int | None = None
        self._motion_ref: np.ndarray | None = None
        self._last_kept_ns = 0
        self._discontinuity = True
        self._lock = threading.Lock()
        self._dec_segment: Gst.Segment | None = None  # for running time after the decoder
        self._segment_looping = False
        self._streams: list[str] = []
        self._start_phase = "playing"  # files that loop: preroll -> playing
        self.loops = 0
        self._loop: GLib.MainLoop | None = None
        self.pipeline = self._build_source()

    # ---------------------------------------------------------------- build
    def _build_source(self) -> Gst.Pipeline:
        u = self.url
        if u.startswith(("rtsp://", "rtsps://")):
            src = f"rtspsrc name=src location={u} protocols=tcp latency=200 ! parsebin name=pb"
        elif u.startswith("file://"):
            src = f"filesrc name=src location={urlparse(u).path} ! parsebin name=pb"
        elif u.startswith("test://"):
            src = f"{test_source(u)} ! parsebin name=pb"
        else:
            raise ValueError(f"unsupported source: {u}")
        pipeline = Gst.parse_launch(src)
        pipeline.get_by_name("pb").connect("pad-added", self._on_parsed_pad)
        pipeline.get_by_name("pb").connect("no-more-pads", self._on_no_more_pads)
        bus = pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._on_message)
        return pipeline

    @property
    def _live(self) -> bool:
        return not self.url.startswith("file://")

    def _make(self, factory: str, **props) -> Gst.Element:
        element = Gst.ElementFactory.make(factory, None)
        if element is None:
            raise RuntimeError(f"GStreamer element '{factory}' is not installed")
        for k, v in props.items():
            element.set_property(k.replace("_", "-"), v)
        self.pipeline.add(element)
        return element

    def _on_parsed_pad(self, _parsebin, pad: Gst.Pad) -> None:
        caps = pad.get_current_caps() or pad.query_caps(None)
        name = caps.get_structure(0).get_name()
        codec = CODECS.get(name)
        self._streams.append(name)
        if codec is None or self.stats.codec is not None:
            # audio, subtitles, other codecs, a second video track: left unlinked. Demuxers
            # only fail with "not-linked" when *no* track is linked; that case is _no_video().
            log.info("[%s] ignoring %s stream", self.camera_id, name)
            return
        try:
            self._link_rest(pad, codec)
        except Exception as exc:  # report through the bus so run() fails cleanly
            self.stats.error = str(exc)
            self.pipeline.post_message(Gst.Message.new_application(None, Gst.Structure.new_empty("maia-error")))

    def _on_no_more_pads(self, _parsebin) -> None:
        if self.stats.codec is None:
            self._no_video()
            self.pipeline.post_message(Gst.Message.new_application(None, Gst.Structure.new_empty("maia-fatal")))

    def _no_video(self) -> None:
        """Nothing we can decode: retrying will not change that, so the camera is 'failed'."""
        found = ", ".join(self._streams) or "no streams"
        self.stats.error = (
            f"no H.264/H.265 video in this source (found: {found}). Cameras send H.264/H.265; "
            "convert a file once with: ffmpeg -i in.mp4 -c:v libx264 -bf 0 -g 50 -an out.mp4"
        )

    def _link_rest(self, pad: Gst.Pad, codec: str) -> None:
        p = self.profile
        caps = pad.get_current_caps() or pad.query_caps(None)
        kind, decoder_name, notes = decoders.choose(codec, p.decode, self.gpu, caps)
        self.stats.codec, self.stats.decoder, self.stats.decoder_kind = codec, decoder_name, kind
        for note in notes:  # e.g. a 4:4:4 stream the GPU cannot decode: the CPU takes it
            log.warning("[%s] %s; using %s", self.camera_id, note, decoder_name)
        self.stats.notes = notes
        sync = not self._live  # files play at their own pace, like a camera

        parse = self._make(f"{codec}parse", config_interval=-1)
        bytestream = self._make(
            "capsfilter", caps=Gst.Caps.from_string(f"video/x-{codec},stream-format=byte-stream,alignment=au")
        )
        tee = self._make("tee")
        # encoded branch: never drop (clips need every packet), never block the decoder
        q_pkt = self._make("queue", max_size_buffers=0, max_size_time=0, max_size_bytes=64 << 20, leaky=2)
        packets = self._make("appsink", emit_signals=True, sync=sync, max_buffers=0, drop=False)
        packets.connect("new-sample", self._on_packet_sample)

        q_dec = self._make("queue", max_size_buffers=60, max_size_time=0, max_size_bytes=0)
        # a second parser converts byte-stream (needed by the clip branch) into whatever this
        # decoder accepts: Apple's VideoToolbox, for one, takes only AVC
        reparse = self._make(f"{codec}parse")
        decoder = self._make(decoder_name)
        chain: list[Gst.Element] = [q_dec, reparse, decoder]
        if p.sampling.mode == "iframes":
            # decode only key frames: everything else is dropped before the decoder
            decoder.get_static_pad("sink").add_probe(Gst.PadProbeType.BUFFER, self._drop_delta_units)
        else:
            decoder.get_static_pad("src").add_probe(
                Gst.PadProbeType.BUFFER | Gst.PadProbeType.EVENT_DOWNSTREAM, self._rate_limit
            )
        rotate, flip = p.step(Rotate), p.step(Flip)
        if rotate and rotate.rotate:
            chain.append(self._make("videoflip", method=FLIP[rotate.rotate]))
        if flip:
            chain.append(
                self._make("videoflip", method="horizontal-flip" if flip.flip == "horizontal" else "vertical-flip")
            )
        if p.step(Crop):
            crop = self._make("videocrop")
            crop.get_static_pad("sink").connect("notify::caps", self._set_crop, crop)
            chain.append(crop)
        chain.append(self._make("videoconvert"))
        chain.append(self._make("videoscale"))
        enhance = p.step(Enhance)
        if enhance and enhance.enhance == "night":
            chain += [self._make("gamma", gamma=1.6), self._make("videobalance", contrast=1.25)]
            chain.append(self._make("videoconvert"))
        fmt, _ = PIXFMT_CAPS[p.pixfmt]
        size = p.step(Resize)
        raw = f"video/x-raw,format={fmt}" + (f",width={size.resize[0]},height={size.resize[1]}" if size else "")
        chain.append(self._make("capsfilter", caps=Gst.Caps.from_string(raw)))
        # live: keep only the newest frames (latency over completeness). Files decode ahead of
        # their timestamps and wait for them, so nothing may be dropped there.
        live = self._live
        chain.append(
            self._make(
                "queue", max_size_buffers=2 if live else 30, max_size_time=0, max_size_bytes=0, leaky=2 if live else 0
            )
        )
        frames = self._make("appsink", emit_signals=True, sync=sync, max_buffers=2 if live else 0, drop=live)
        frames.connect("new-sample", self._on_frame_sample)
        chain.append(frames)

        links = [(parse, bytestream), (bytestream, tee), (q_pkt, packets), *zip(chain, chain[1:], strict=False)]
        extra: list[Gst.Element] = []
        if self.publish_url:
            # files and test sources: the same encoded packets go to MediaMTX, paced by their
            # timestamps, so people can watch them live; nothing is re-encoded
            q_pub = self._make("queue", max_size_buffers=0, max_size_time=0, max_size_bytes=32 << 20, leaky=2)
            rtsp = self._make("rtspclientsink", location=self.publish_url, protocols="tcp", latency=0)
            extra = [q_pub, rtsp]
            links.append((q_pub, rtsp))
        for a, b in links:
            if not a.link(b):
                raise RuntimeError(
                    f"cannot link {a.get_factory().get_name()} to {b.get_factory().get_name()} "
                    f"(no common format; GStreamer {Gst.version_string()})"
                )
        for element in [parse, bytestream, tee, q_pkt, packets, *chain, *extra]:
            element.sync_state_with_parent()
        tee.request_pad_simple("src_%u").link(q_pkt.get_static_pad("sink"))
        tee.request_pad_simple("src_%u").link(q_dec.get_static_pad("sink"))
        if extra:
            tee.request_pad_simple("src_%u").link(extra[0].get_static_pad("sink"))
        result = pad.link(parse.get_static_pad("sink"))
        if result != Gst.PadLinkReturn.OK:
            caps = pad.get_current_caps() or pad.query_caps(None)
            raise RuntimeError(
                f"cannot link the parsed stream ({result.value_nick}); stream caps: {caps.to_string()[:200]}; "
                f"GStreamer {Gst.version_string()} from {Gst.Registry.get().find_plugin('coreelements').get_filename()}"
            )
        self.stats.state = "running"
        log.info("[%s] %s via %s (%s)", self.camera_id, codec, decoder_name, kind)

    # ------------------------------------------------------------ callbacks
    @staticmethod
    def _drop_delta_units(_pad, info):
        buf = info.get_buffer()
        if buf is not None and buf.has_flags(Gst.BufferFlags.DELTA_UNIT):
            return Gst.PadProbeReturn.DROP
        return Gst.PadProbeReturn.OK

    def _rate_limit(self, _pad, info):
        """Keep one decoded frame per 1/fps seconds of running time."""
        if info.type & Gst.PadProbeType.EVENT_DOWNSTREAM:
            event = info.get_event()
            if event.type == Gst.EventType.SEGMENT:
                self._dec_segment = event.parse_segment().copy()
            return Gst.PadProbeReturn.OK
        self.stats.decoded += 1
        pts = info.get_buffer().pts
        if pts == Gst.CLOCK_TIME_NONE:
            return Gst.PadProbeReturn.OK
        if self._dec_segment is not None:
            pts = self._dec_segment.to_running_time(Gst.Format.TIME, pts)
        interval = int(1e9 / self.profile.sampling.fps)
        if self._next_due is not None and pts < self._next_due:
            return Gst.PadProbeReturn.DROP
        # the next frame is due one interval later; if we fell behind (a stall, a seek), restart
        self._next_due = (self._next_due or pts) + interval
        if self._next_due <= pts:
            self._next_due = pts + interval
        return Gst.PadProbeReturn.OK

    def _set_crop(self, pad, _pspec, crop) -> None:
        caps = pad.get_current_caps()
        if caps is None:
            return
        s = caps.get_structure(0)
        w, h = s.get_value("width"), s.get_value("height")
        left, top, right, bottom = self.profile.step(Crop).crop
        crop.set_property("left", int(w * left) & ~1)  # even offsets keep chroma planes aligned
        crop.set_property("top", int(h * top) & ~1)
        crop.set_property("right", int(w * (1 - right)) & ~1)
        crop.set_property("bottom", int(h * (1 - bottom)) & ~1)

    def _on_packet_sample(self, sink) -> Gst.FlowReturn:
        sample = sink.emit("pull-sample")
        if self._start_phase != "playing":
            return Gst.FlowReturn.OK  # a looping file before its segment seek: discarded
        buf = sample.get_buffer()
        if self.caps_string is None:
            self.caps_string = sample.get_caps().to_string()
        key = not buf.has_flags(Gst.BufferFlags.DELTA_UNIT)
        segment = sample.get_segment()
        pts, dts = (_running(segment, t) for t in (buf.pts, buf.dts))
        if key and pts != Gst.CLOCK_TIME_NONE:
            self._key_pts.append(pts)
            if self._last_key_pts is not None and pts > self._last_key_pts:
                gop = (pts - self._last_key_pts) / 1e9
                self.stats.gop_s = round(gop if self.stats.gop_s is None else 0.8 * self.stats.gop_s + 0.2 * gop, 2)
            self._last_key_pts = pts
        self.stats.packets += 1
        ok, info = buf.map(Gst.MapFlags.READ)
        if ok:
            try:
                self.on_packet(Packet(time.time_ns(), pts, dts, buf.duration, key, bytes(info.data)))
            finally:
                buf.unmap(info)
        return Gst.FlowReturn.OK

    def _on_frame_sample(self, sink) -> Gst.FlowReturn:
        sample = sink.emit("pull-sample")
        if self._start_phase != "playing":
            return Gst.FlowReturn.OK
        buf = sample.get_buffer()
        if self.profile.sampling.mode == "iframes":
            self.stats.decoded += 1  # no rate probe in this mode
        now = time.time_ns()
        s = sample.get_caps().get_structure(0)
        width, height = s.get_value("width"), s.get_value("height")
        sampling = self.profile.sampling
        pts = _running(sample.get_segment(), buf.pts)
        ok, info = buf.map(Gst.MapFlags.READ)
        if not ok:
            return Gst.FlowReturn.OK
        try:
            frame = self._as_array(info.data, width, height)
            flags = 0
            if sampling.mode == "motion":
                small = frame[::4, ::4].astype(np.int16)
                if small.ndim == 3:
                    small = small.sum(axis=2) // 3
                changed = (
                    1.0
                    if self._motion_ref is None
                    else (np.abs(small - self._motion_ref) > sampling.motion_delta).mean()
                )
                moved = changed >= sampling.motion_area
                alive = now - self._last_kept_ns >= sampling.keepalive_s * 1e9
                if not (moved or alive):
                    return Gst.FlowReturn.OK
                self._motion_ref = small
                flags |= MOTION if moved else 0
            self._write(frame, width, height, now, pts, flags)
        finally:
            buf.unmap(info)
        return Gst.FlowReturn.OK

    def _as_array(self, data, width: int, height: int) -> np.ndarray:
        channels = PIXFMT_CAPS[self.profile.pixfmt][1]
        row = width * channels
        stride = len(data) // height  # GStreamer pads rows to 4 bytes
        if stride < row:
            raise RuntimeError(f"frame buffer too small: {len(data)} bytes for {width}x{height}")
        shape, strides = ((height, width), (stride, 1)) if channels == 1 else ((height, width, 3), (stride, 3, 1))
        return np.ndarray(shape, np.uint8, buffer=data, strides=strides)

    def _write(self, frame: np.ndarray, width: int, height: int, now: int, pts: int, flags: int) -> None:
        if self.ring is None or (self.ring.geometry.width, self.ring.geometry.height) != (width, height):
            if self.ring is not None:
                self.ring.close()
            channels = PIXFMT_CAPS[self.profile.pixfmt][1]
            self.ring = RingWriter(
                ring_name(self.camera_id),
                slots=self.profile.ring.slots,
                width=width,
                height=height,
                channels=channels,
                pixfmt=self.profile.pixfmt,
            )
            self.stats.width, self.stats.height = width, height
            self._discontinuity = True
        if pts != Gst.CLOCK_TIME_NONE and pts in self._key_pts:
            flags |= IFRAME
        if self.profile.sampling.mode == "iframes":
            flags |= IFRAME
        if self._discontinuity:
            flags |= DISCONTINUITY
            self._discontinuity = False
        view = self.ring.begin()
        np.copyto(view, frame)
        mask = self.profile.step(PrivacyMask)
        if mask:
            for left, top, right, bottom in mask.privacy_mask:
                view[int(top * height) : int(bottom * height), int(left * width) : int(right * width)] = 0
        del view
        ref = self.ring.commit(timestamp_ns=now, pts_ns=0 if pts == Gst.CLOCK_TIME_NONE else pts, flags=flags)
        self._last_kept_ns = now
        self.stats.written += 1
        self.stats.last_frame_ns = now
        self.stats._rate_window.append(now)
        self.publish(ref)

    def _on_message(self, _bus, msg) -> None:
        t = msg.type
        if t == Gst.MessageType.ERROR:
            err, debug = msg.parse_error()
            if self.stats.codec is None and self._streams:  # streams found, none of them usable
                self._no_video()
                log.error("[%s] %s", self.camera_id, self.stats.error)
                self.stop("failed")
                return
            self.stats.error = f"{err.message} ({msg.src.get_name()})"
            log.error("[%s] %s | %s", self.camera_id, self.stats.error, debug)
            self.stop("error")
        elif t == Gst.MessageType.APPLICATION and msg.get_structure().get_name() == "maia-fatal":
            log.error("[%s] %s", self.camera_id, self.stats.error)
            self.stop("failed")
        elif t == Gst.MessageType.APPLICATION and msg.get_structure().get_name() == "maia-error":
            log.error("[%s] %s", self.camera_id, self.stats.error)
            self.stop("error")
        elif t == Gst.MessageType.ASYNC_DONE and self._start_phase == "preroll" and self.stats.codec:
            # The file is linked and prerolled, nothing has played yet: switch to segment
            # playback. The end of the file then posts SEGMENT_DONE, answered by a seek to 0
            # *without* a flush, so running time (and the stream published to MediaMTX) carries
            # on across loops; EOS and a flushing seek would restart timestamps at 0, which
            # HLS muxers reject.
            # (seek while paused, then PLAYING at once: the pipeline finishes its preroll by
            # itself; waiting for a second ASYNC_DONE hangs when a sink that does not preroll,
            # such as the one draining an audio track, is in the pipeline)
            self._segment_looping = self._segment_seek(Gst.SeekFlags.FLUSH)
            self._start_phase = "playing"
            self.pipeline.set_state(Gst.State.PLAYING)
        elif t == Gst.MessageType.SEGMENT_DONE:
            self.loops += 1
            self._discontinuity = True
            if not self._segment_seek(Gst.SeekFlags.NONE):
                self.stop("error")
        elif t == Gst.MessageType.EOS:
            if self._loops_file:  # the demuxer could not do segment seeks: flushing fallback
                self.pipeline.seek_simple(Gst.Format.TIME, Gst.SeekFlags.FLUSH | Gst.SeekFlags.KEY_UNIT, 0)
                self.loops += 1
                self._next_due = None
                self._discontinuity = True
            else:
                self.stop("ended")

    @property
    def _loops_file(self) -> bool:
        return self.url.startswith("file://") and self.loop_files

    def _segment_seek(self, flush) -> bool:
        # Known limit: GStreamer starts each pass at the previous pass's last *decode* time.
        # With B-frames the last frames are *displayed* a little later, so they overlap the
        # next pass by a frame or two. Analytics and clips handle that; MediaMTX's HLS muxer
        # does not, so files published for viewing should have no B-frames (README).
        flags = flush | Gst.SeekFlags.SEGMENT | Gst.SeekFlags.KEY_UNIT
        return self.pipeline.seek(1.0, Gst.Format.TIME, flags, Gst.SeekType.SET, 0, Gst.SeekType.NONE, -1)

    # -------------------------------------------------------------- control
    def run(self, watchdog_s: float = 15.0) -> str:
        """Run until the stream ends, fails or stalls. Returns the final state."""
        self._loop = GLib.MainLoop()
        GLib.timeout_add(1000, self._watchdog, watchdog_s)
        if threading.current_thread() is threading.main_thread():  # SIGTERM from the supervisor
            GLib.unix_signal_add(GLib.PRIORITY_HIGH, signal.SIGTERM, self._on_sigterm)
            GLib.unix_signal_add(GLib.PRIORITY_HIGH, signal.SIGINT, self._on_sigterm)
        if self._loops_file:
            self._start_phase = "preroll"  # PAUSED first; _on_message takes it to PLAYING
        target = Gst.State.PAUSED if self._start_phase == "preroll" else Gst.State.PLAYING
        if self.pipeline.set_state(target) == Gst.StateChangeReturn.FAILURE:
            self.stats.state, self.stats.error = "error", "pipeline would not start"
            return self.stats.state
        try:
            self._loop.run()
        finally:
            self.pipeline.set_state(Gst.State.NULL)
            if self.ring is not None:
                self.ring.close()
                self.ring = None
        return self.stats.state

    def _watchdog(self, limit_s: float) -> bool:
        if self.profile.sampling.mode == "motion":
            limit_s = max(limit_s, 2 * self.profile.sampling.keepalive_s)
        last = self.stats.last_frame_ns or self.stats.started_ns
        if (time.time_ns() - last) / 1e9 > limit_s:
            self.stats.error = f"no frames for {limit_s:.0f}s"
            log.warning("[%s] %s", self.camera_id, self.stats.error)
            self.stop("stalled")
            return False
        return self._loop is not None and self._loop.is_running()

    def _on_sigterm(self) -> bool:
        self.stop("stopped")
        return False

    def stop(self, state: str = "stopped") -> None:
        if self.stats.state not in ("error", "stalled", "ended", "failed"):  # the first reason stays
            self.stats.state = state
        if self._loop is not None:
            GLib.idle_add(self._loop.quit)
