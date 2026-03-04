# ── Stage 1: Build ────────────────────────────────────────────────────────────
# Install dependencies in a separate stage so build tools don't end up
# in the final image.
FROM python:3.12-slim-bookworm AS builder

WORKDIR /build

# Install build deps needed to compile any C extensions in requirements
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies into a prefix we can copy over
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM python:3.12-slim-bookworm AS runtime

# Keep image up to date with latest Debian security patches at build time
RUN apt-get update && apt-get upgrade -y --no-install-recommends \
    && apt-get install -y --no-install-recommends \
        libsqlite3-0 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from builder
COPY --from=builder /install /usr/local

# ── Non-root user ─────────────────────────────────────────────────────────────
# Never run as root inside the container
RUN groupadd --gid 1001 pskr \
 && useradd  --uid 1001 --gid pskr --no-create-home --shell /sbin/nologin pskr

# ── Directories ───────────────────────────────────────────────────────────────
# /data        — SQLite database (mount a named volume here)
# /etc/pskr    — config.yaml (mount or COPY at deploy time)
RUN mkdir -p /data /etc/pskr \
 && chown pskr:pskr /data /etc/pskr

# ── Application ───────────────────────────────────────────────────────────────
WORKDIR /app
COPY app/pskr-mqtt-cache/ ./pskr_mqtt_cache/

# Default config — override by mounting /etc/pskr/config.yaml
COPY config.yaml.example /etc/pskr/config.yaml
RUN chown pskr:pskr /etc/pskr/config.yaml

# ── Hardening ─────────────────────────────────────────────────────────────────
# Drop all capabilities — this service needs none
# (set in your compose/pod spec: cap_drop: [ALL])
#
# Read-only root filesystem friendly — only /data needs to be writable
# (set in your compose/pod spec: read_only: true)

USER pskr

# ── Runtime config ────────────────────────────────────────────────────────────
# Override database path to use the /data volume
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

EXPOSE 5000

VOLUME ["/data"]

HEALTHCHECK --interval=60s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5000/status')" || exit 1

CMD ["python", "-m", "pskr_mqtt_cache", "--config", "/etc/pskr/config.yaml"]
