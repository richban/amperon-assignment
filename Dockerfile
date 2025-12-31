FROM python:3.11.7@sha256:63bec515ae23ef6b4563d29e547e81c15d80bf41eff5969cb43d034d333b63b8

WORKDIR /app

# Environment variables
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install uv for fast dependency installation
RUN pip install uv

COPY workspace/dg-workspace /app
COPY workspace/sqlmesh /app/sqlmesh

RUN uv pip install --system -e .

RUN mkdir -p /app/data

ENV PYTHONPATH=/app/src:$PYTHONPATH
