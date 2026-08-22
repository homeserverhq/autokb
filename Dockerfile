FROM python:3.11-slim

WORKDIR /

ENV PYTHONPATH=/src
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies (build tools for some python packages)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-cache tiktoken encoding so the worker never downloads at runtime.
# The cl100k_base vocabulary (~10MB) is fetched once at build time and
# stored in /data/tiktoken-cache/. At runtime, tiktoken finds it there
# and never attempts a network call.
ENV TIKTOKEN_CACHE_DIR=/data/tiktoken-cache
RUN mkdir -p /data/tiktoken-cache && \
    python -c "import tiktoken; tiktoken.get_encoding('cl100k_base')"

COPY src/ /src/

COPY assets/ /assets/
