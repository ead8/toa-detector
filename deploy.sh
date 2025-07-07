#!/bin/bash

echo "🚀 Deploying TAO-DETECTOR..."

# Stop existing containers
echo "Stopping existing containers..."
docker compose down

# Build and start services
echo "Building and starting services..."
docker compose up --build -d

# Wait for services to initialize
echo "Waiting for services to initialize..."
sleep 30

# Check service health
echo "Checking service health..."

# Check OKX service (port 8083)
if curl -f http://localhost:8083/health >/dev/null 2>&1; then
    echo "✅ OKX service ready"
else
    echo "⚠️ OKX service not ready"
fi

# Check Binance service (port 8082) - may not have health endpoint
if curl -f http://localhost:8082/health >/dev/null 2>&1; then
    echo "✅ Binance service ready"
else
    echo "⚠️ Binance service not ready (normal - no health endpoint)"
fi

# Show recent logs
echo "Showing recent logs..."
docker compose logs --tail=10

echo "✅ Deployment complete!"
echo "Monitor logs with: docker compose logs -f"
echo "Check status with: docker compose ps"
