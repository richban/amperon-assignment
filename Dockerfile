FROM python:3.11.7@sha256:63bec515ae23ef6b4563d29e547e81c15d80bf41eff5969cb43d034d333b63b8

WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH"
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Copy package files first
COPY ./pyproject.toml .
COPY ./tomorrow /app/tomorrow

# Install package in editable mode
RUN python -m pip install -e .

# Copy DLT configuration
COPY ./.dlt /app/.dlt
