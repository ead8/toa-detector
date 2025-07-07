import os
import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timedelta, timezone
from duckdb_utils import OKXDatabaseManager

import logging
import ssl
import time
import traceback

# Constants
OKX_BASE_URL = "https://www.okx.com"
CANDLES_ENDPOINT = "/api/v5/market/history-candles"
INSTRUMENTS_ENDPOINT = "/api/v5/public/instruments"
INTERVAL = "1m"
LIMIT = 100  # Max candles per request
DAYS = 7
CONCURRENCY = 15  # Increased concurrency
MAX_RETRIES = 5
BASE_REQUEST_DELAY = 0.1  # Reduced base delay
START_YEAR = 2014  # OKX founded as OKCoin in 2014
MAX_REQUESTS_PER_SECOND = 8  # OKX rate limit

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("okx_first_7days.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AdaptiveRateLimiter:
    """Adaptive rate limiter that adjusts based on API responses"""
    def __init__(self, max_rate=MAX_REQUESTS_PER_SECOND, interval=1.0):
        self.max_rate = max_rate
        self.interval = interval
        self.semaphore = asyncio.Semaphore(max_rate)
        self.last_reset = time.time()
        self.request_count = 0
        self.backoff_factor = 1

    async def wait(self):
        async with self.semaphore:
            now = time.time()
            elapsed = now - self.last_reset
            
            if elapsed > self.interval:
                self.request_count = 0
                self.last_reset = now
                self.backoff_factor = max(1, self.backoff_factor * 0.9)  # Reduce backoff
            
            if self.request_count >= self.max_rate:
                sleep_time = min(5.0, self.interval - elapsed + 0.1)
                logger.warning(f"Rate limit approached, waiting {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)
                self.last_reset = time.time()
                self.request_count = 0
            
            self.request_count += 1
            await asyncio.sleep(BASE_REQUEST_DELAY * self.backoff_factor)
            
    def increase_backoff(self):
        """Increase backoff when rate limited"""
        self.backoff_factor = min(8, self.backoff_factor * 1.5)
        logger.info(f"Increased backoff to {self.backoff_factor:.2f}")

async def fetch_symbols(session, rate_limiter):
    """Fetch all SWAP symbols from OKX."""
    await rate_limiter.wait()
    async with session.get(f"{OKX_BASE_URL}{INSTRUMENTS_ENDPOINT}", 
                          params={"instType": "SWAP"}) as resp:
        if resp.status != 200:
            logger.error(f"Failed to fetch symbols: HTTP {resp.status}")
            return []
        
        data = await resp.json()
        if data.get("code", "0") != "0":
            logger.error(f"API error fetching symbols: {data.get('msg')}")
            return []
        
        return [{"instId": s["instId"], "listTime": int(s["listTime"])} for s in data["data"]]

async def get_first_candle_time(session, rate_limiter, symbol, list_time):
    """Find the earliest 1-minute candle using optimized search."""
    start_time = list_time
    end_time = int(datetime.now(timezone.utc).timestamp() * 1000)
    params = {
        "instId": symbol,
        "bar": INTERVAL,
        "limit": 1  # Only need the first candle
    }
    url = f"{OKX_BASE_URL}{CANDLES_ENDPOINT}"
    
    # First try at listing time
    params["after"] = start_time
    for attempt in range(MAX_RETRIES):
        try:
            await rate_limiter.wait()
            async with session.get(url, params=params) as resp:
                if resp.status == 429:
                    rate_limiter.increase_backoff()
                    logger.warning(f"Rate limit hit for {symbol}, retrying...")
                    continue
                    
                if resp.status != 200:
                    logger.warning(f"HTTP {resp.status} for {symbol}, attempt {attempt + 1}")
                    await asyncio.sleep(0.5)
                    continue
                
                data = await resp.json()
                if data.get("code", "0") != "0":
                    logger.warning(f"API error for {symbol}: {data.get('msg')}")
                    await asyncio.sleep(0.5)
                    continue
                
                candles = data.get("data", [])
                if candles:
                    earliest_time = int(candles[0][0])
                    logger.info(f"Found first candle for {symbol} at {earliest_time}")
                    return earliest_time
                break
        except aiohttp.ClientError as e:
            logger.error(f"ClientError for {symbol}: {e}")
            await asyncio.sleep(1)
    
    # Binary search fallback
    logger.info(f"Using binary search for {symbol}")
    while end_time - start_time > 60000:  # 1 minute
        mid_time = (start_time + end_time) // 2
        params["after"] = mid_time
        
        for attempt in range(MAX_RETRIES):
            try:
                await rate_limiter.wait()
                async with session.get(url, params=params) as resp:
                    if resp.status == 429:
                        rate_limiter.increase_backoff()
                        logger.warning(f"Rate limit hit for {symbol}, retrying...")
                        continue
                        
                    if resp.status != 200:
                        logger.warning(f"HTTP {resp.status} for {symbol}, attempt {attempt + 1}")
                        await asyncio.sleep(0.5)
                        continue
                    
                    data = await resp.json()
                    if data.get("code", "0") != "0":
                        logger.warning(f"API error for {symbol}: {data.get('msg')}")
                        await asyncio.sleep(0.5)
                        continue
                    
                    candles = data.get("data", [])
                    if candles:
                        earliest_time = int(candles[0][0])
                        logger.debug(f"Found candle at {earliest_time} for {symbol}")
                        end_time = earliest_time
                    else:
                        start_time = mid_time
                    break
            except aiohttp.ClientError as e:
                logger.error(f"ClientError for {symbol}: {e}")
                await asyncio.sleep(1)
    
    logger.info(f"Earliest 1m data for {symbol} at {start_time}")
    return start_time

async def fetch_ohlcv_chunk(session, rate_limiter, symbol, chunk_start):
    """Fetch a single chunk of OHLCV data."""
    params = {
        "instId": symbol,
        "bar": INTERVAL,
        "limit": LIMIT,
        "after": int(chunk_start)
    }
    url = f"{OKX_BASE_URL}{CANDLES_ENDPOINT}"
    
    for attempt in range(MAX_RETRIES):
        try:
            await rate_limiter.wait()
            async with session.get(url, params=params) as resp:
                if resp.status == 429:
                    rate_limiter.increase_backoff()
                    logger.warning(f"Rate limit hit for {symbol}, retrying chunk...")
                    continue
                    
                if resp.status != 200:
                    logger.warning(f"HTTP {resp.status} for {symbol} at {chunk_start}")
                    await asyncio.sleep(0.5)
                    continue
                
                data = await resp.json()
                if data.get("code", "0") != "0":
                    logger.warning(f"API error for {symbol} at {chunk_start}: {data.get('msg')}")
                    await asyncio.sleep(0.5)
                    continue
                
                return data.get("data", [])
        except aiohttp.ClientError as e:
            logger.error(f"ClientError for {symbol} at {chunk_start}: {e}")
            await asyncio.sleep(1)
    
    logger.warning(f"Failed to fetch chunk for {symbol} at {chunk_start}")
    return []

async def fetch_ohlcv_7_days(session, rate_limiter, symbol, start_time):
    """Fetch 7 days of 1-minute OHLCV data in parallel chunks."""
    end_time = start_time + DAYS * 24 * 60 * 60 * 1000
    chunk_size = LIMIT * 60 * 1000  # 100 minutes in ms
    
    # Create time chunks
    chunks = []
    current = start_time
    while current < end_time:
        chunks.append(current)
        current += chunk_size
    
    # Fetch all chunks in parallel
    tasks = [fetch_ohlcv_chunk(session, rate_limiter, symbol, chunk) for chunk in chunks]
    results = await asyncio.gather(*tasks)
    
    # Combine and sort results
    all_klines = [kline for sublist in results for kline in sublist if kline]
    all_klines.sort(key=lambda x: int(x[0]))  # Sort by timestamp
    
    logger.info(f"Fetched {len(all_klines)} candles for {symbol}")
    return all_klines

async def process_symbol(session, rate_limiter, symbol_info):
    """Process a single symbol."""
    symbol = symbol_info["instId"]
    list_time = symbol_info["listTime"]
    
    logger.debug(f"Processing symbol: {symbol}")
    logger.debug(f"List time type: {type(list_time)}")
    logger.debug(f"List time value: {list_time}")
    
    logger.info(f"Processing {symbol}...")
    
    try:
        first_time = await get_first_candle_time(session, rate_limiter, symbol, list_time)
        if not first_time:
            logger.warning(f"Skipping {symbol}, no valid 1m data.")
            return {"symbol": symbol, "list_date": None, "candles": 0}
            
        klines = await fetch_ohlcv_7_days(session, rate_limiter, symbol, first_time)
        
        # Insert into DuckDB with extensive logging
        logger.debug(f"Attempting to insert candles for {symbol}")
        logger.debug(f"Candles count: {len(klines)}")
        logger.debug(f"First candle: {klines[0] if klines else 'No candles'}")
        
        db_result = OKXDatabaseManager.insert_candles(
            klines, 
            symbol, 
            list_time,  # This should be an integer 
            context='all', 
            exchange='OKX Futures'
        )
        
        return db_result
    except Exception as e:
        logger.error(f"Error processing {symbol}: {e}")
        logger.error(f"Error type: {type(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {"symbol": symbol, "list_date": None, "candles": 0}

async def main():
    """Main function to orchestrate the download process."""
    # Initialize DuckDB database for all coins
    OKXDatabaseManager.initialize_database(context='all')
    
    rate_limiter = AdaptiveRateLimiter()
    
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=ssl.create_default_context()),
        timeout=aiohttp.ClientTimeout(total=30)
    ) as session:
        logger.info("Fetching symbols from OKX...")
        symbols = await fetch_symbols(session, rate_limiter)
        logger.info(f"Fetched {len(symbols)} symbols from OKX")
        
        tasks = [process_symbol(session, rate_limiter, s) for s in symbols]
        results = await asyncio.gather(*tasks)

if __name__ == "__main__":
    # Initialize database before any operations
    OKXDatabaseManager.initialize_database()
    
    # Run the main download process
    asyncio.run(main())