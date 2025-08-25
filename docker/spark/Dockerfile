FROM bitnami/spark:3.5.0
# FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

USER root

# Install Python dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install Python dependencies
# COPY src/consumer/requirements.txt .
# RUN uv pip install --no-cache-dir -r requirements.txt

# Copy dependency files from two levels up
COPY ../../uv.lock ../../pyproject.toml ./

# Install uv and sync dependencies
RUN pip install uv && uv sync && uv add requests

# Copy source code
COPY src/consumer/ .
COPY src/utils/ ./utils/
COPY config/.env .

# Create necessary directories
RUN mkdir -p data/output data/checkpoints logs

# Set environment variables
ENV SPARK_HOME=/opt/bitnami/spark
ENV PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip

# Run the Spark consumer
CMD ["python3", "spark_consumer.py"]