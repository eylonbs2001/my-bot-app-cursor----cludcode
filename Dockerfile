# FalconEye Crypto Signals - production image
# Multi-stage build keeps the final image small and free of build tooling.

FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# System deps required to build numpy/pandas/matplotlib wheels and to
# verify TLS certs at runtime. Removed from final image via multi-stage.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        libffi-dev \
        libssl-dev \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --user -r requirements.txt

# ---------- runtime image ----------
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONWARNINGS="ignore::UserWarning,ignore::FutureWarning" \
    TZ=Asia/Jerusalem \
    PATH="/home/falcon/.local/bin:${PATH}"

# Run as non-root user — basic hardening.
RUN useradd --create-home --shell /usr/sbin/nologin falcon \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Pull the pre-built deps from the builder stage.
COPY --from=builder --chown=falcon:falcon /root/.local /home/falcon/.local

# Copy application source.
COPY --chown=falcon:falcon scanner.py ./

# Healthcheck: a live python process that can import the scanner module.
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import scanner" || exit 1

USER falcon

# The scanner blocks the main thread with run_forever(); no signal handling
# is wired up explicitly so use SIGTERM-friendly exec form via tini.
CMD ["python", "-u", "scanner.py"]
