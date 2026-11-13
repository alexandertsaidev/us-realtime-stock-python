FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS dev
ENV UV_LINK_MODE=copy
WORKDIR /app

# 先裝依賴（利用 cache）
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project

COPY . /app
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app/src"
ENTRYPOINT []
CMD ["bash"]

FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder
ENV UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1
WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

FROM python:3.13-slim-bookworm AS prod
WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY src/ /app/src/

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app/src" \
    PYTHONUNBUFFERED=1

RUN useradd -r -u 1000 -m -d /home/appuser -s /usr/sbin/nologin appuser \
 && runuser -u appuser -- python -c "import duckdb; duckdb.connect().execute('INSTALL httpfs')"

USER appuser

# 開發版
# docker build --target dev -t us-rt-stock-python:dev .

# 生產版
# docker build --target prod -t us-rt-stock-python:v2.0 .

# 不指定 target：預設建最後一個階段（也就是 prod）
# docker build -t us-rt-stock-python:v2.0 .
