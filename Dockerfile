FROM python:3.11.7@sha256:63bec515ae23ef6b4563d29e547e81c15d80bf41eff5969cb43d034d333b63b8

WORKDIR /app

# Environment variables
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install uv for fast dependency installation
RUN pip install uv

# Copy workspace files
COPY workspace/dg-workspace /app

# Install dependencies using uv (includes dagster CLI with webserver and daemon)
RUN uv pip install --system -e . && \
    uv pip install --system dagster-webserver dagster-dg-cli

# Copy SQLMesh models
COPY workspace/sqlmesh /app/sqlmesh

# Create necessary directories
RUN mkdir -p /app/dagster_home

# Set default environment variables (can be overridden by docker-compose)
ENV DAGSTER_HOME=/app/dagster_home
ENV PYTHONPATH=/app/src:$PYTHONPATH
