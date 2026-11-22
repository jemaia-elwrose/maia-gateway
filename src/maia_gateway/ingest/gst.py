"""GStreamer import with a clear message when the Python bindings are missing."""

try:
    import gi

    gi.require_version("Gst", "1.0")
    gi.require_version("GstApp", "1.0")
    from gi.repository import GLib, Gst, GstApp  # noqa: F401
except (ImportError, ValueError) as exc:  # pragma: no cover
    raise ImportError(
        "GStreamer's Python bindings are missing. Linux: apt install python3-gi gir1.2-gstreamer-1.0 "
        "gir1.2-gst-plugins-base-1.0 (and create the venv with --system-site-packages). "
        "macOS: brew install gstreamer pygobject3. Or use the Docker image."
    ) from exc

Gst.init(None)
