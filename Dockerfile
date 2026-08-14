FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

# Install deps first (cached layer)
# README.md is needed because pyproject.toml sets readme = "README.md"
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev

# Copy source + migrations + scripts
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY migrations/ ./migrations/
COPY alembic.ini ./
COPY tests/ ./tests/

# Ensure upload dir exists
RUN mkdir -p /tmp/simapp_uploads

ENV SIMAPP_DATABASE_URL=postgresql+psycopg://simapp:simapp@postgres:5432/simapp
ENV SIMAPP_UPLOAD_DIR=/tmp/simapp_uploads

EXPOSE 8000
