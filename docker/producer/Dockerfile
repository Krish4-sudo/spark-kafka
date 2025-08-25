FROM python:3.9-slim
# FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
# Copy dependency files from two levels up
COPY ../../uv.lock ../../pyproject.toml ./

# Install uv and sync dependencies
RUN pip install uv && uv sync && uv add requests

# Copy source code
COPY src/producer/ .
COPY src/utils/ ./utils/
COPY config/.env .

# Create logs directory
RUN mkdir -p logs

# Run the producer
CMD ["python", "weather_producer.py"]