import os
import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timedelta, timezone
from binance_duckdb_utils import BinanceDatabaseManager
import logging
import ssl
import time
import random

# Constants
BINANCE_FUTURES_URL = "https://fapi.binance.com"
KLINES_ENDPOINT = "/fapi/v1/klines"
EXCHANGE_INFO_ENDPOINT = "/fapi/v1/exchangeInfo"
INTERVAL = "1m"
LIMIT = 1500  # Max candles per request
DAYS = 7
CONCURRENCY = 3
MAX_RETRIES = 5
BASE_REQUEST_DELAY = 0.5
START_YEAR = 2019  # Binance Futures launched in 2019
MAX_REQUESTS_PER_SECOND = 5
BAN_BACKOFF_MIN = 300  # 5 minutes
BAN_BACKOFF_MAX = 600  # 10 minutes

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("binance_futures_first_7days.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AdaptiveRateLimiter:
    """Enhanced adaptive rate limiter with better ban handling"""
    def __init__(self, max_rate=5, interval=1.0):
        self.max_rate = max_rate
        self.interval = interval
        self.semaphore = asyncio.Semaphore(max_rate)
        self.last_reset = time.time()
        self.request_count = 0
        self.backoff_factor = 1
        self.ban_detected = False
        self.last_ban_time = 0
        self.consecutive_bans = 0
        self.request_times = []

    async def wait(self):
        # Handle ban status with exponential backoff
        if self.ban_detected:
            ban_wait_time = BAN_BACKOFF_MIN * (2 ** min(self.consecutive_bans, 4))
            ban_wait = max(0, self.last_ban_time + ban_wait_time - time.time())
            if ban_wait > 0:
                logger.warning(f"Ban detected, waiting {ban_wait:.1f}s before requests (attempt {self.consecutive_bans + 1})")
                await asyncio.sleep(ban_wait)
            self.ban_detected = False

        async with self.semaphore:
            now = time.time()
            
            # Clean old request times (keep only last minute)
            self.request_times = [t for t in self.request_times if now - t < 60]
            
            # Check if we're making too many requests per minute
            if len(self.request_times) >= self.max_rate * 60:
                sleep_time = 60 - (now - self.request_times[0])
                if sleep_time > 0:
                    logger.warning(f"Rate limit per minute reached, waiting {sleep_time:.2f}s")
                    await asyncio.sleep(sleep_time)
            
            # Reset counter if interval passed
            elapsed = now - self.last_reset
            if elapsed > self.interval:
                self.request_count = 0
                self.last_reset = now
                self.backoff_factor = max(1, self.backoff_factor * 0.95)
            
            # Check requests per second
            if self.request_count >= self.max_rate:
                sleep_time = self.interval - elapsed + 0.1
                logger.warning(f"Rate limit per second reached, waiting {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)
                self.last_reset = time.time()
                self.request_count = 0
            
            self.request_count += 1
            self.request_times.append(time.time())
            
            # Apply progressive delay
            delay = BASE_REQUEST_DELAY * self.backoff_factor
            await asyncio.sleep(delay)
            
    def increase_backoff(self):
        """Increase backoff when rate limited"""
        self.backoff_factor = min(10, self.backoff_factor * 2)
        logger.info(f"Increased backoff to {self.backoff_factor:.2f}")
        
    def handle_ban(self):
        """Handle IP ban situation (HTTP 418)"""
        self.ban_detected = True
        self.last_ban_time = time.time()
        self.consecutive_bans += 1
        self.backoff_factor = 10  # Set to max backoff
        logger.warning(f"Ban detected! Consecutive bans: {self.consecutive_bans}")
        
    def reset_ban_counter(self):
        """Reset ban counter on successful requests"""
        if self.consecutive_bans > 0:
            self.consecutive_bans = max(0, self.consecutive_bans - 1)

async def fetch_symbols(session, rate_limiter):
    """Fetch all futures symbols from Binance."""
    await rate_limiter.wait()
    async with session.get(f"{BINANCE_FUTURES_URL}{EXCHANGE_INFO_ENDPOINT}") as resp:
        if resp.status == 418:
            rate_limiter.handle_ban()
            return []
        if resp.status != 200:
            logger.error(f"Failed to fetch symbols: HTTP {resp.status}")
            return []
        
        data = await resp.json()
        symbols = [
            {"symbol": s["symbol"], "listTime": int(s.get("onboardDate", 0))} 
            for s in data["symbols"] 
            if s["contractType"] == "PERPETUAL"
        ]
        logger.info(f"Fetched {len(symbols)} symbols from Binance Futures")
        return symbols

async def get_first_candle_time(session, rate_limiter, symbol, list_time):
    """Find the earliest 1-minute candle using optimized search."""
    start_time = list_time if list_time > 0 else int(datetime(START_YEAR, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    end_time = int(datetime.now(timezone.utc).timestamp() * 1000)
    params = {
        "symbol": symbol,
        "interval": INTERVAL,
        "limit": 1  # Only need the first candle
    }
    url = f"{BINANCE_FUTURES_URL}{KLINES_ENDPOINT}"
    
    # First try at listing time
    params["endTime"] = start_time + 60000  # Look 1 minute after listing
    for attempt in range(MAX_RETRIES):
        try:
            await rate_limiter.wait()
            async with session.get(url, params=params) as resp:
                if resp.status == 429:
                    rate_limiter.increase_backoff()
                    logger.warning(f"Rate limit hit for {symbol}, retrying...")
                    continue
                elif resp.status == 418:
                    rate_limiter.handle_ban()
                    ban_wait = random.uniform(BAN_BACKOFF_MIN, BAN_BACKOFF_MAX)
                    logger.warning(f"HTTP 418 for {symbol}, waiting {ban_wait:.1f}s")
                    await asyncio.sleep(ban_wait)
                    continue
                    
                if resp.status != 200:
                    logger.warning(f"HTTP {resp.status} for {symbol}, attempt {attempt + 1}")
                    await asyncio.sleep(0.5)
                    continue
                
                data = await resp.json()
                if isinstance(data, dict) and data.get("code"):
                    logger.warning(f"API error for {symbol}: {data.get('msg')}")
                    await asyncio.sleep(0.5)
                    continue
                
                if data:
                    earliest_time = int(data[0][0])
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
        params["endTime"] = mid_time
        
        for attempt in range(MAX_RETRIES):
            try:
                await rate_limiter.wait()
                async with session.get(url, params=params) as resp:
                    if resp.status == 429:
                        rate_limiter.increase_backoff()
                        logger.warning(f"Rate limit hit for {symbol}, retrying...")
                        continue
                    elif resp.status == 418:
                        rate_limiter.handle_ban()
                        ban_wait = random.uniform(BAN_BACKOFF_MIN, BAN_BACKOFF_MAX)
                        logger.warning(f"HTTP 418 for {symbol}, waiting {ban_wait:.1f}s")
                        await asyncio.sleep(ban_wait)
                        continue
                        
                    if resp.status != 200:
                        logger.warning(f"HTTP {resp.status} for {symbol}, attempt {attempt + 1}")
                        await asyncio.sleep(0.5)
                        continue
                    
                    data = await resp.json()
                    if isinstance(data, dict) and data.get("code"):
                        logger.warning(f"API error for {symbol}: {data.get('msg')}")
                        await asyncio.sleep(0.5)
                        continue
                    
                    if data:
                        earliest_time = int(data[0][0])
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
    """Fetch a single chunk of OHLCV data with enhanced ban handling."""
    params = {
        "symbol": symbol,
        "interval": INTERVAL,
        "limit": LIMIT,
        "startTime": int(chunk_start)
    }
    url = f"{BINANCE_FUTURES_URL}{KLINES_ENDPOINT}"
    
    for attempt in range(MAX_RETRIES):
        try:
            await rate_limiter.wait()
            async with session.get(url, params=params) as resp:
                if resp.status == 429:
                    rate_limiter.increase_backoff()
                    logger.warning(f"Rate limit hit for {symbol}, retrying chunk... (attempt {attempt + 1})")
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    continue
                elif resp.status == 418:
                    rate_limiter.handle_ban()
                    ban_wait = random.uniform(BAN_BACKOFF_MIN, BAN_BACKOFF_MAX)
                    logger.warning(f"HTTP 418 for {symbol} at {chunk_start}, waiting {ban_wait:.1f}s")
                    await asyncio.sleep(ban_wait)
                    continue
                    
                if resp.status != 200:
                    logger.warning(f"HTTP {resp.status} for {symbol} at {chunk_start}")
                    await asyncio.sleep(2 ** attempt)
                    continue
                
                data = await resp.json()
                if isinstance(data, dict) and data.get("code"):
                    logger.warning(f"API error for {symbol} at {chunk_start}: {data.get('msg')}")
                    await asyncio.sleep(2 ** attempt)
                    continue
                
                # Success - reset ban counter
                rate_limiter.reset_ban_counter()
                return data
                
        except aiohttp.ClientError as e:
            logger.error(f"ClientError for {symbol} at {chunk_start}: {e}")
            await asyncio.sleep(2 ** attempt)
    
    logger.warning(f"Failed to fetch chunk for {symbol} at {chunk_start} after {MAX_RETRIES} attempts")
    return []

async def fetch_ohlcv_7_days(session, rate_limiter, symbol, start_time):
    """Fetch 7 days of 1-minute OHLCV data with controlled concurrency."""
    end_time = start_time + DAYS * 24 * 60 * 60 * 1000
    chunk_size = LIMIT * 60 * 1000  # 1500 minutes in ms
    
    # Create time chunks
    chunks = []
    current = start_time
    while current < end_time:
        chunks.append(current)
        current += chunk_size
    
    # Fetch chunks with limited concurrency
    all_klines = []
    chunk_sem = asyncio.Semaphore(CONCURRENCY)
    
    async def fetch_chunk(chunk):
        async with chunk_sem:
            return await fetch_ohlcv_chunk(session, rate_limiter, symbol, chunk)
    
    tasks = [fetch_chunk(chunk) for chunk in chunks]
    results = await asyncio.gather(*tasks)
    
    # Combine and sort results
    for klines in results:
        if klines:
            all_klines.extend(klines)
    
    all_klines.sort(key=lambda x: int(x[0]))  # Sort by timestamp
    
    logger.info(f"Fetched {len(all_klines)} candles for {symbol}")
    return all_klines

async def process_symbol(session, rate_limiter, symbol_info):
    """Process a single symbol."""
    symbol = symbol_info["symbol"]
    list_time = symbol_info["listTime"]
    
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
        
        db_result = BinanceDatabaseManager.insert_candles(
            klines, 
            symbol, 
            list_time,  
            context='all', 
            exchange='Binance Futures'
        )
        
        return db_result
    except Exception as e:
        logger.error(f"Error processing {symbol}: {e}")
        return {"symbol": symbol, "list_date": None, "candles": 0}

async def main():
    """Main function to orchestrate the download process."""
    # Initialize DuckDB database for all coins
    BinanceDatabaseManager.initialize_database(context='all')
    
    rate_limiter = AdaptiveRateLimiter()
    
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=ssl.create_default_context()),
        timeout=aiohttp.ClientTimeout(total=60)
    ) as session:
        logger.info("Fetching symbols from Binance Futures...")
        symbols = await fetch_symbols(session, rate_limiter)
        logger.info(f"Fetched {len(symbols)} symbols from Binance Futures")
        
        tasks = [process_symbol(session, rate_limiter, s) for s in symbols]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    # For MacBook, prevent sleep
    os.system("caffeinate -dimsu python binance_futures_first_7days.py &")
    asyncio.run(main())