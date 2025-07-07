FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    cron \
    supervisor \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy everything first, then organize
COPY . /tmp/source/

# Copy requirements files
RUN cp /tmp/source/requirements.txt . && \
    cp "/tmp/source/Binance Futures - Scripts/requirements.txt" ./binance_requirements.txt && \
    cp "/tmp/source/OKX Futures - Scripts /requirements.txt" ./okx_requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -r binance_requirements.txt && \
    pip install --no-cache-dir -r okx_requirements.txt

# Copy application code
RUN cp -r "/tmp/source/Binance Futures - Scripts/" ./binance/ && \
    cp -r "/tmp/source/OKX Futures - Scripts /" ./okx/

# Create necessary directories
RUN mkdir -p /app/data /app/logs /app/master_data

# Copy configuration files
RUN cp /tmp/source/crontab /etc/cron.d/tao-detector-cron && \
    chmod 0644 /etc/cron.d/tao-detector-cron && \
    crontab /etc/cron.d/tao-detector-cron && \
    touch /var/log/cron.log

RUN cp /tmp/source/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

RUN cp /tmp/source/entrypoint.sh /entrypoint.sh && \
    chmod +x /entrypoint.sh

# Clean up
RUN rm -rf /tmp/source

# Set Python path
ENV PYTHONPATH=/app:/app/binance:/app/okx
ENV PYTHONUNBUFFERED=1

EXPOSE 8080 8081

ENTRYPOINT ["/entrypoint.sh"]
