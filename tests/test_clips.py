"""Clip buffering and merging, with synthetic packets (no camera needed)."""

import pytest
from conftest import needs_gst

pytestmark = needs_gst


def probe(path):
    """(duration s, width, height) of a video file."""
    import gi

    gi.require_version("GstPbutils", "1.0")
    from gi.repository import GstPbutils

    from maia_gateway.ingest.gst import Gst

    info = GstPbutils.Discoverer.new(10 * Gst.SECOND).discover_uri(f"file://{path}")
    v = info.get_video_streams()[0]
    return info.get_duration() / 1e9, v.get_width(), v.get_height()


def packets(seconds, fps=10, gop=10, t0=1_000_000_000_000):
    from maia_gateway.ingest.pipeline import Packet

    step = int(1e9 / fps)
    return [Packet(t0 + i * step, i * step, i * step, step, i % gop == 0, b"x") for i in range(int(seconds * fps))]


def test_buffer_keeps_the_window_and_starts_on_a_key_frame():
    from maia_gateway.ingest.clips import PacketBuffer

    buf = PacketBuffer(pre_s=2.0)
    for p in packets(10):
        buf.append(p)
    span = (buf.packets[-1].arrival_ns - buf.packets[0].arrival_ns) / 1e9
    assert buf.packets[0].key and 2.0 <= span < 3.0  # 2 s plus at most one GOP


def test_buffer_ignores_packets_until_the_first_key_frame():
    from maia_gateway.ingest.clips import PacketBuffer

    buf = PacketBuffer(pre_s=2.0)
    for p in packets(3)[3:]:  # starts mid-GOP
        buf.append(p)
    assert buf.packets[0].key


def test_overlapping_events_extend_one_clip(tmp_path):
    from maia_gateway.ingest.clips import ClipRecorder

    pk = packets(20)
    rec = ClipRecorder("c", tmp_path, pre_s=1.0, post_s=2.0, caps=lambda: None)
    for p in pk[:50]:
        rec.add(p)
    first = rec.event(at_ns=pk[49].arrival_ns, label="a")
    second = rec.event(at_ns=pk[60].arrival_ns, label="b")  # 1.1 s later: inside the open clip
    assert second.id == first.id and [e["label"] for e in first.events] == ["a", "b"]
    assert first.end_ns == pk[60].arrival_ns + int(2e9)


@pytest.mark.parametrize("caps", [None])
def test_failed_clip_is_reported_not_raised(tmp_path, caps):
    from maia_gateway.ingest.clips import ClipRecorder

    done = []
    rec = ClipRecorder("c", tmp_path, pre_s=0.5, post_s=0.2, caps=lambda: caps, on_done=done.append)
    pk = packets(3)
    for p in pk[:10]:
        rec.add(p)
    rec.event(at_ns=pk[9].arrival_ns)
    rec.flush()
    assert done[0].status == "failed" and "format unknown" in done[0].error
    assert (tmp_path / "c" / f"{done[0].id}.json").exists()
