#!/bin/bash

echo "🛑 Stopping TAO-DETECTOR..."

# Stop all services
docker compose down

echo "✅ All services stopped!"
echo "Data preserved in ./data/, ./logs/, and ./master_data/"
echo "To restart: ./deploy.sh"
