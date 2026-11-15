# =============================================================================
# Dockerfile — ONE image for every Python service in the pipeline.
#
# producer, pricing, risk, storage, monitor and the one-shot jobs
# (kafka-init, db-init, seed-book, fake-feed) all run this image with a
# different command (docker-compose.yml). One image means one set of pinned
# dependencies and one build, and a service is still one file to read.
#
# What is NOT in the image: .env, secrets/, data/, logs/, results/ (see
# .dockerignore). Secrets reach a container only at run time, as files under
# /run/secrets/ for the services that need them.
# =============================================================================

# Pinned Python 3.12 on Debian trixie (slim). Multi-arch: runs natively on
# arm64 (Apple silicon) and amd64.
FROM python:3.12.15-slim-trixie

# No .pyc files in the image; unbuffered output so `docker compose logs`
# shows lines immediately; quiet, cache-less pip.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Run as an unprivileged user. UID/GID are build arguments so a Linux host
# can match its own user and keep the bind-mounted logs/ writable
# (docker compose build --build-arg UID=$(id -u) --build-arg GID=$(id -g)).
# Docker Desktop / OrbStack on macOS map ownership for bind mounts anyway.
ARG UID=1000
ARG GID=1000
RUN groupadd --gid "${GID}" app \
    && useradd --uid "${UID}" --gid "${GID}" --no-create-home --shell /usr/sbin/nologin app

WORKDIR /app

# Dependencies first: this layer is rebuilt only when requirements change.
# All of them ship wheels for linux/arm64 and linux/amd64 (no compiler needed).
COPY requirements.txt .
RUN pip install -r requirements.txt

# Application code and configuration (topology.toml, the risk book, SQL).
COPY pipeline/ pipeline/
COPY scripts/ scripts/
COPY db/ db/
COPY config/ config/

# Folders the services write to; bind-mounted from the host in compose.
RUN mkdir -p logs run results && chown app:app logs run results
USER app

# Each compose service sets its own command; this default is harmless.
CMD ["python", "-c", "print('set a command, e.g. python -m pipeline.producer')"]
