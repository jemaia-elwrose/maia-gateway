# maia-gateway: one image for CPU and NVIDIA GPUs.
# Ubuntu's GStreamer includes NVIDIA's nvcodec plugin, which loads the driver at run time:
# without a GPU the gateway decodes on the CPU; run it with a GPU (NVIDIA Container
# Toolkit) and `decode: auto` picks NVDEC by itself.
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 PYTHONUNBUFFERED=1 \
    NVIDIA_DRIVER_CAPABILITIES=compute,video,utility \
    MAIA_CLIPS_DIR=/clips

# GStreamer + its Python bindings come from Ubuntu (they are the hard part of a native install);
# x264enc (plugins-ugly) is only for the built-in test cameras.
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 python3-venv python3-gi gir1.2-gstreamer-1.0 gir1.2-gst-plugins-base-1.0 \
      gstreamer1.0-tools gstreamer1.0-plugins-base gstreamer1.0-plugins-good \
      gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly gstreamer1.0-libav gstreamer1.0-rtsp \
      git ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# a venv that can see the system's GStreamer bindings
RUN python3 -m venv --system-site-packages /opt/venv
ENV PATH=/opt/venv/bin:$PATH

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install ".[infer]" \
 && python -c "from maia_gateway.ingest import decoders; import maia_ring, maia_infer; \
print('ok: decoders', decoders.report()['cpu'])"

EXPOSE 8080
CMD ["maia-gateway", "serve", "--config", "/config/cameras.yml"]
