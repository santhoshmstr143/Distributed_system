# syntax=docker/dockerfile:1
FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app/src

WORKDIR /app

# Create non-root user
RUN groupadd -g 10001 appgroup && \
    useradd -u 10001 -g appgroup -m -s /bin/bash appuser

# Install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source and packaging metadata
COPY pyproject.toml README.md ./
COPY src/ ./src/

# Install the application
RUN pip install --no-cache-dir -e .

# Create data directories and adjust ownership
RUN mkdir -p /data /shared && \
    chown -R appuser:appgroup /app /data /shared

USER appuser

EXPOSE 8000 8100 8200 8201 8202 8203 8204 8205

CMD ["python", "-m", "docstore.router"]
