FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y rclone curl unzip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Create working directory
WORKDIR /app

# Copy source
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .

# Volume for data temp storage
VOLUME ["/data"]

# Entry point
ENTRYPOINT ["python", "d2s3.py"]
