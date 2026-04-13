# Dockerfile for Ion Tool Population Daily Sync
# Use slim-bookworm to avoid Docker Hub metadata/lease errors with python:3.11-slim
FROM python:3.11-slim-bookworm

# Install Java (required for JDBC driver; Bookworm has OpenJDK 17, not 21)
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Set environment variable for V2 Production (can be overridden)
ENV ENVIRONMENT=v2_production

# Default command runs daily sync
CMD ["python3", "daily_tool_sync.py"]

