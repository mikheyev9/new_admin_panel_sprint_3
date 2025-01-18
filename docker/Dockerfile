FROM ghcr.io/astral-sh/uv:python3.13-alpine

ENV PATH="${PATH}:/root/.local/bin"
ENV UV_NO_VENV=0
ENV UV_SYSTEM_PYTHON=0
ENV UV_VENV_PATH="/app/.venv"

WORKDIR /app

COPY ./etl/pyproject.toml  ./etl/uv.lock ./

RUN uv venv create && uv sync --frozen

COPY ./etl .
ENV PATH="/app/.venv/bin:$PATH"

CMD ["uv", "run", "python", "main.py"]
