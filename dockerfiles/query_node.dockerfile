FROM python:3.11-slim AS core

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

ENV PYTHONPATH=/app:$PYTHONPATH

ARG BREAK_CACHE_ARG=0
RUN pip install --no-cache-dir "git+https://github.com/rayonlabs/fiber.git@1.0.0#egg=fiber[full]"
################################################################################

FROM core AS query_node

WORKDIR /app/validator/query_node

COPY validator/query_node/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY core /app/core
COPY validator/utils /app/validator/utils
COPY validator/models.py /app/validator/models.py
COPY validator/db /app/validator/db

COPY validator/query_node/src ./src
COPY validator/query_node/pyproject.toml .


ENV PYTHONPATH="${PYTHONPATH}:/app/validator/query_node/src"

# CMD ["tail", "-f", "/dev/null"]