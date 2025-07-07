#!/bin/bash

echo "📊 TAO-DETECTOR Status Monitor"
echo "================================"

# Container status
echo "🐳 Container Status:"
docker compose ps

echo ""
echo "🏥 Health Checks:"
echo "Binance: $(curl -s http://localhost:8080/health | jq -r '.status' 2>/dev/null || echo 'Not responding')"
echo "OKX: $(curl -s http://localhost:8081/health | jq -r '.status' 2>/dev/null || echo 'Not responding')"

echo ""
echo "📁 Data Directory Sizes:"
du -sh data/ logs/ master_data/ 2>/dev/null || echo "Directories not found"

echo ""
echo "📝 Recent Logs (last 10 lines):"
echo "--- Binance New Coin Detector ---"
docker compose logs --tail=10 tao-detector | grep binance_new_coin_detector || echo "No recent logs"

echo ""
echo "--- OKX New Coin Detector ---"
docker compose logs --tail=10 tao-detector | grep okx_new_coin_detector || echo "No recent logs"
