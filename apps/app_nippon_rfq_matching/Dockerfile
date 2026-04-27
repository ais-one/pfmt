# Use Debian-based Python image
FROM python:3.12-slim

# Build argument for macOS-specific requirements
ARG INSTALL_MACOS_REQUIREMENTS=false

# Prevent Python from generating .pyc files
ENV PYTHONDONTWRITEBYTECODE=1
# Keep Python stdout/stderr from being buffered
ENV PYTHONUNBUFFERED=1

# Install system dependencies and uv
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    pkg-config \
    libgomp1 \
    libopenblas-dev \
    liblapack-dev \
    libfreetype-dev \
    libpng-dev \
    libjpeg-dev \
    libwebp-dev \
    libtiff-dev \
    libavif-dev \
    && curl -LsSf https://astral.sh/uv/install.sh | sh \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Add uv to PATH
ENV PATH="/root/.local/bin:${PATH}"

# Set uv environment variables
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

# Set working directory
WORKDIR /app

# Create non-root user
RUN groupadd -g 1000 appuser && \
    useradd -r -u 1000 -g appuser appuser

# Copy requirements first for better caching
COPY requirements.txt .
COPY requirements-macos.txt .

# Install Python dependencies using uv
RUN uv pip install --system -r requirements.txt && \
    if [ "$INSTALL_MACOS_REQUIREMENTS" = "true" ]; then uv pip install --system -r requirements-macos.txt; fi

# Copy application code
COPY --chown=appuser:appuser ./app ./app

# Create data directories and set permissions
RUN mkdir -p /app/data/uploads /app/data/storage /app/data/storage/csv /app/data/vectors && \
    chown -R appuser:appuser /app

# Copy .env.example as .env if .env doesn't exist (for container startup)
COPY --chown=appuser:appuser .env.example .env.example

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 8000

# Run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
