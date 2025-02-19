FROM ghcr.io/astral-sh/uv:python3.13-alpine

ENV PATH="${PATH}:/root/.local/bin"
ENV UV_NO_VENV=1

WORKDIR /app

COPY ./etl/pyproject.toml  ./etl/uv.lock ./

RUN uv pip install --system .

COPY ./etl .

CMD ["python", "main.py"]
