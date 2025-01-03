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
    wget \
    ca-certificates \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && rm -rf /var/lib/apt/lists/*

# Needed for commit reveal
ENV PATH="/root/.cargo/bin:${PATH}"

RUN pip install --upgrade pip

ENV PYTHONPATH=/app:$PYTHONPATH

ARG BREAK_CACHE_ARG=0
RUN pip install --no-cache-dir "fiber[full] @ git+https://github.com/rayonlabs/fiber.git@2.1.0"
################################################################################

FROM core AS control_node
WORKDIR /app/validator/control_node

COPY validator/control_node/assets ./assets
RUN wget https://huggingface.co/datasets/tau-vision/synth-gen/resolve/main/synth_corpus.json -P /app/validator/control_node/assets

COPY validator/control_node/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY core /app/core
COPY validator/utils /app/validator/utils
COPY validator/models.py /app/validator/models.py
COPY validator/db /app/validator/db

COPY validator/control_node/src ./src
COPY validator/control_node/pyproject.toml .

ENV PYTHONPATH="${PYTHONPATH}:/app/validator/control_node/src"


# CMD ["tail", "-f", "/dev/null"]
