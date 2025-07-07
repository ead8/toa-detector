#!/bin/bash

echo "📊 Exporting TAO-DETECTOR crypto data..."

# Create export folder
FOLDER="crypto_data_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$FOLDER"

# Export data from existing databases
docker compose exec tao-detector python3 -c "
import os,pandas as pd,duckdb
from datetime import datetime

os.makedirs('/app/data/export', exist_ok=True)
ts = datetime.now().strftime('%Y%m%d_%H%M%S')

# Export Binance OHLCV data
try:
    conn = duckdb.connect('/app/binance_futures_master.db')
    df = conn.execute('''
    SELECT 
        open_time,
        open,
        high, 
        low,
        close,
        volume,
        symbol,
        exchange
    FROM futures_candles 
    ORDER BY symbol, open_time
    ''').df()
    
    if len(df) > 0:
        df.to_csv(f'/app/data/export/binance_ohlcv_{ts}.csv', index=False)
        print(f'✅ Binance OHLCV: {len(df):,} rows, {df[\"symbol\"].nunique()} symbols')
        print(f'📅 Date range: {df[\"open_time\"].min()} to {df[\"open_time\"].max()}')
    conn.close()
except Exception as e:
    print(f'❌ Binance OHLCV error: {e}')

# Export OKX OHLCV data (if exists)
try:
    if os.path.exists('/app/okx_futures_master.db'):
        conn = duckdb.connect('/app/okx_futures_master.db')
        df = conn.execute('''
        SELECT 
            open_time,
            open,
            high, 
            low,
            close,
            volume,
            symbol,
            exchange
        FROM futures_candles 
        ORDER BY symbol, open_time
        ''').df()
        
        if len(df) > 0:
            df.to_csv(f'/app/data/export/okx_ohlcv_{ts}.csv', index=False)
            print(f'✅ OKX OHLCV: {len(df):,} rows, {df[\"symbol\"].nunique()} symbols')
        conn.close()
    else:
        print('⚠️ OKX database not found')
except Exception as e:
    print(f'❌ OKX OHLCV error: {e}')

# Export Binance symbols list
try:
    conn = duckdb.connect('/app/master_data/binance_duck.db')
    df = conn.execute('SELECT * FROM tao_detector.seen_listings').df()
    if len(df) > 0:
        df.to_csv(f'/app/data/export/binance_symbols_{ts}.csv', index=False)
        print(f'✅ Binance symbols: {len(df)} rows')
    conn.close()
except Exception as e:
    print(f'❌ Binance symbols error: {e}')

# Export OKX symbols list
try:
    conn = duckdb.connect('/app/master_data/duck.db')
    df = conn.execute('SELECT * FROM tao_detector.seen_listings').df()
    if len(df) > 0:
        df.to_csv(f'/app/data/export/okx_symbols_{ts}.csv', index=False)
        print(f'✅ OKX symbols: {len(df)} rows')
    conn.close()
except Exception as e:
    print(f'❌ OKX symbols error: {e}')

print('Done!')
"

# Copy files to local folder
docker cp $(docker compose ps -q tao-detector):/app/data/export/. "$FOLDER/"

echo "✅ Export completed!"
echo "📁 Files ready in: $FOLDER/"
ls -lh "$FOLDER/"

# Show what's in the files
echo ""
echo "📊 Data preview:"
for file in "$FOLDER"/*.csv; do
    if [ -f "$file" ]; then
        echo "📄 $(basename "$file"):"
        head -3 "$file"
        echo ""
    fi
done
