#!/bin/bash
set -e

echo "Initializing databases..."
cd /app/binance && python binance_duckdb_utils.py
cd /app/okx && python duckdb_utils.py

echo "Starting cron service..."
service cron start

echo "Starting supervisor..."
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf
