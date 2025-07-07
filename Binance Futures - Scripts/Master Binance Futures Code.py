import os
import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timedelta
import time

# Constants
BINANCE_FUTURES_URL = "https://fapi.binance.com"
KLINES_ENDPOINT = "/fapi/v1/klines"
EXCHANGE_INFO_ENDPOINT = "/fapi/v1/exchangeInfo"
DATA_DIR = "data"
INTERVAL = "1m"
LIMIT = 1500
DAYS = 7
CONCURRENCY = 1  # Very conservative - only 1 request at a time
MAX_RETRIES = 3
REQUEST_DELAY = 1.0  # 1 second between requests

# Ensure output folder exists
os.makedirs(DATA_DIR, exist_ok=True)

# Semaphore for concurrency
semaphore = asyncio.Semaphore(CONCURRENCY)

async def fetch_symbols(session):
    url = f"{BINANCE_FUTURES_URL}{EXCHANGE_INFO_ENDPOINT}"
    async with session.get(url) as resp:
        data = await resp.json()
        return [s["symbol"] for s in data["symbols"] if s["contractType"] == "PERPETUAL"]

async def get_first_candle_time(session, symbol):
    url = f"{BINANCE_FUTURES_URL}{KLINES_ENDPOINT}"
    params = {"symbol": symbol, "interval": INTERVAL, "limit": 1, "startTime": 0}
    
    for attempt in range(MAX_RETRIES):
        try:
            await asyncio.sleep(REQUEST_DELAY)  # Rate limiting
            async with session.get(url, params=params) as resp:
                if resp.status == 429:  # Rate limit
                    wait_time = 60  # Wait 1 minute on rate limit
                    print(f"Rate limited for {symbol}, waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                    
                data = await resp.json()
                if isinstance(data, list) and data:
                    return int(data[0][0])  # open_time in ms
                elif isinstance(data, dict) and data.get('code') == -1121:  # Invalid symbol
                    print(f"Invalid symbol: {symbol}")
                    return None
                else:
                    return None
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(5)  # Wait 5 seconds on error
                continue
            else:
                print(f"Failed to get first candle for {symbol}: {e}")
                return None
    return None

async def fetch_ohlcv_chunk(session, symbol, start_time):
    url = f"{BINANCE_FUTURES_URL}{KLINES_ENDPOINT}"
    params = {
        "symbol": symbol,
        "interval": INTERVAL,
        "limit": LIMIT,
        "startTime": start_time,
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            await asyncio.sleep(REQUEST_DELAY)  # Rate limiting
            async with session.get(url, params=params) as resp:
                if resp.status == 429:  # Rate limit
                    wait_time = 60  # Wait 1 minute on rate limit
                    print(f"Rate limited for {symbol}, waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                    
                return await resp.json()
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(5)  # Wait 5 seconds on error
                continue
            else:
                print(f"Failed to fetch chunk for {symbol}: {e}")
                return []
    return []

async def fetch_ohlcv_7_days(session, symbol, start_time):
    end_time = start_time + DAYS * 24 * 60 * 60 * 1000  # 7 days in ms
    all_klines = []
    current_time = start_time
    
    while current_time < end_time:
        klines = await fetch_ohlcv_chunk(session, symbol, current_time)
        if not klines:
            break
        all_klines.extend(klines)
        last_time = int(klines[-1][0])
        current_time = last_time + 60 * 1000  # advance by 1 minute
        if len(klines) < LIMIT:
            break
        
        # Longer delay between chunks to be very conservative
        await asyncio.sleep(0.5)
    
    return all_klines

async def save_to_csv(symbol, klines):
    df = pd.DataFrame(klines, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades", "taker_base_vol",
        "taker_quote_vol", "ignore"
    ])
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df.to_csv(f"{DATA_DIR}/{symbol}.csv", index=False)

async def process_symbol(session, symbol):
    async with semaphore:
        try:
            print(f"Processing {symbol}...")
            first_time = await get_first_candle_time(session, symbol)
            if first_time is None:
                print(f"No data for {symbol}")
                return
                
            klines = await fetch_ohlcv_7_days(session, symbol, first_time)
            if klines and len(klines) > 0:
                await save_to_csv(symbol, klines)
                print(f"Saved {symbol} ({len(klines)} candles)")
            else:
                print(f"No data for {symbol}")
        except Exception as e:
            print(f"Error processing {symbol}: {e}")

async def main():
    async with aiohttp.ClientSession() as session:
        symbols = await fetch_symbols(session)
        print(f"Fetched {len(symbols)} symbols")
        
        # Process symbols one by one to be very conservative
        batch_size = 10  # Very small batches
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            print(f"Processing batch {i//batch_size + 1}/{(len(symbols) + batch_size - 1)//batch_size} ({len(batch)} symbols)")
            
            # Process one symbol at a time
            for symbol in batch:
                await process_symbol(session, symbol)
                print(f"Completed {symbol}")
            
            # Long wait between batches
            if i + batch_size < len(symbols):
                print("Waiting 30 seconds before next batch...")
                await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(main()) 