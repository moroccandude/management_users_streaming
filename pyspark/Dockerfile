# Use an official Spark base image
FROM apache/spark-py:latest

# Switch to root user for installations
USER root

# Update package lists and install system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    wget
    
WORKDIR /app

# Install Python packages with system-wide permissions
RUN pip3 install --no-cache-dir \
    pyspark \
    confluent-kafka \
    pandas

# Create a non-root user
RUN useradd -m sparkuser && \
    chown -R sparkuser:sparkuser /app

# Switch to non-root user
USER sparkuser

# Default command
CMD ["spark-submit"]