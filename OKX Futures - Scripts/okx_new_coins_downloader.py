import os
import aiohttp
import asyncio
import pandas as pd
import json
import random
import signal
from datetime import datetime, timedelta, timezone
import logging
import ssl
from duckdb_utils import OKXDatabaseManager  
from telegram_utils import alert_new_listing
from aiohttp import web

# Constants
OKX_BASE_URL = "https://www.okx.com"
CANDLES_ENDPOINT = "/api/v5/market/history-candles"
INSTRUMENTS_ENDPOINT = "/api/v5/public/instruments"
DATA_DIR = "data"
INTERVAL = "1m"
LIMIT = 100
DAYS = 7
CONCURRENCY = 3
MAX_RETRIES = 3
BASE_REQUEST_DELAY = 0.3
HEALTH_CHECK_PORT = 8081
MAX_ERROR_COUNT = 5  # Shutdown after 5 consecutive errors
MIN_CHECK_INTERVAL = 15  # 1 minute
GENESIS_CANDLE_BUFFER = 60  # Wait 60 seconds after listing to ensure genesis candle exists

# Structured logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s", "module": "%(module)s", "function": "%(funcName)s"}',
    handlers=[
        logging.FileHandler("okx_new_coins.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

os.makedirs(DATA_DIR, exist_ok=True)
semaphore = asyncio.Semaphore(CONCURRENCY)

# Global state
is_running = True
current_tasks = set()
last_symbol_fetch = datetime.min.replace(tzinfo=timezone.utc)
symbol_cache = []
symbol_cache_expiry = timedelta(minutes=5)  # Cache symbols for 5 minutes
last_no_new_alert = None


async def health_check(request):
    """Health check endpoint for monitoring"""
    return web.json_response({
        "status": "running" if is_running else "stopping",
        "last_check": datetime.now(timezone.utc).isoformat(),
        "active_tasks": len(current_tasks),
        "service": "okx_new_coin_detector",
        "symbol_cache_age": (datetime.now(timezone.utc) - last_symbol_fetch).total_seconds(),
        "uptime": (datetime.now(timezone.utc) - start_time).total_seconds()
    })

async def start_health_server():
    """Start HTTP server for health checks"""
    app = web.Application()
    app.add_routes([web.get('/health', health_check)])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', HEALTH_CHECK_PORT)
    await site.start()
    logger.info(f"Health check server running on port {HEALTH_CHECK_PORT}")

async def fetch_symbols(session):
    """Fetch swap symbols listed in the last year with exponential backoff"""
    global last_symbol_fetch, symbol_cache
    
    # Use cached symbols if recent
    if datetime.now(timezone.utc) - last_symbol_fetch < symbol_cache_expiry:
        return symbol_cache
    
    backoff = 1
    for attempt in range(MAX_RETRIES + 1):
        try:
            async with session.get(
                f"{OKX_BASE_URL}{INSTRUMENTS_ENDPOINT}", 
                params={"instType": "SWAP"},
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    logger.error(f"Failed to fetch symbols: HTTP {resp.status}")
                    continue
                    
                data = await resp.json()
                if data.get("code", "0") != "0":
                    logger.error(f"API error fetching symbols: {data.get('msg')}")
                    continue
                
                # Filter symbols listed in the last 24 hours
                twenty_four_hours_ago = int((datetime.now(timezone.utc) - timedelta(hours=24)).timestamp() * 1000)
                new_symbols = [
                    {"instId": s["instId"], "listTime": int(s["listTime"])} 
                    for s in data["data"] 
                    if int(s["listTime"]) >= twenty_four_hours_ago
                ]
                
                # Update cache
                symbol_cache = new_symbols
                last_symbol_fetch = datetime.now(timezone.utc)
                return new_symbols
                
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(f"Network error fetching symbols (attempt {attempt+1}): {e}")
        
        # Exponential backoff with jitter
        sleep_time = backoff + (random.random() * 0.5)
        logger.info(f"Retrying symbol fetch in {sleep_time:.1f}s...")
        await asyncio.sleep(sleep_time)
        backoff *= 2
    
    logger.error("Failed to fetch symbols after multiple attempts")
    return []

async def fetch_genesis_candle(session, symbol, list_time):
    """Fetch the very first candle after listing time"""
    # Wait for at least 60 seconds after listing to ensure candle exists
    current_time = int(datetime.now(timezone.utc).timestamp() * 1000)
    if current_time - list_time < GENESIS_CANDLE_BUFFER * 1000:
        wait_time = (GENESIS_CANDLE_BUFFER * 1000 - (current_time - list_time)) / 1000 + 1
        logger.info(f"Waiting {wait_time:.1f}s for genesis candle to be available for {symbol}")
        await asyncio.sleep(wait_time)
    
    params = {
        "instId": symbol,
        "bar": INTERVAL,
        "limit": 1,
        "after": list_time
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(
                f"{OKX_BASE_URL}{CANDLES_ENDPOINT}", 
                params=params,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                # Handle rate limits
                if resp.status == 429:
                    retry_delay = 3 + attempt * 2
                    logger.warning(f"Rate limit hit for {symbol}, waiting {retry_delay}s...")
                    await asyncio.sleep(retry_delay)
                    continue
                
                # Handle other HTTP errors
                if resp.status != 200:
                    logger.warning(f"HTTP {resp.status} for genesis candle of {symbol}")
                    await asyncio.sleep(1 + attempt)
                    continue
                
                data = await resp.json()
                
                # Handle API errors
                if data.get("code", "0") != "0":
                    logger.warning(f"API error for genesis candle of {symbol}: {data.get('msg')}")
                    await asyncio.sleep(1 + attempt)
                    continue
                
                candles = data.get("data", [])
                if candles:
                    return candles[0]
                else:
                    logger.warning(f"No genesis candle found for {symbol}")
                    return None
                    
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(f"Network error for genesis candle of {symbol}: {e}")
            await asyncio.sleep(1 + attempt)
    
    logger.error(f"Failed to fetch genesis candle for {symbol} after multiple attempts")
    return None

async def fetch_ohlcv_7_days(session, symbol, list_time):
    """Fetch 7 days of OHLCV data for a symbol"""
    # Calculate time ranges in milliseconds
    start_time = list_time
    end_time = list_time + (DAYS * 24 * 60 * 60 * 1000)
    all_klines = []
    
    # First get the genesis candle
    genesis_candle = await fetch_genesis_candle(session, symbol, list_time)
    if genesis_candle:
        all_klines.append(genesis_candle)
        start_time = int(genesis_candle[0]) + 60000  # Start after first candle
    
    if start_time >= end_time:
        return all_klines
    
    next_ts = end_time  # Start from the end of the period
    
    logger.info(f"Fetching {symbol} from {datetime.fromtimestamp(start_time/1000, tz=timezone.utc)} "
                f"to {datetime.fromtimestamp(end_time/1000, tz=timezone.utc)}")

    while next_ts > start_time:
        params = {
            "instId": symbol,
            "bar": INTERVAL,
            "limit": str(LIMIT),
            "before": str(next_ts)
        }
        
        klines = []
        for attempt in range(MAX_RETRIES):
            try:
                await asyncio.sleep(BASE_REQUEST_DELAY * (2 ** attempt))
                async with session.get(
                    f"{OKX_BASE_URL}{CANDLES_ENDPOINT}", 
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as resp:
                    # Handle rate limits
                    if resp.status == 429:
                        retry_delay = 3 + attempt * 2
                        logger.warning(f"Rate limit hit for {symbol}, waiting {retry_delay}s...")
                        await asyncio.sleep(retry_delay)
                        continue
                    
                    # Handle other HTTP errors
                    if resp.status != 200:
                        logger.warning(f"HTTP {resp.status} for {symbol} at ts={next_ts}")
                        await asyncio.sleep(1 + attempt)
                        continue
                    
                    data = await resp.json()
                    
                    # Handle API errors
                    if data.get("code", "0") != "0":
                        logger.warning(f"API error for {symbol} at ts={next_ts}: {data.get('msg')}")
                        await asyncio.sleep(1 + attempt)
                        continue
                    
                    klines = data.get("data", [])
                    break
                    
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.error(f"Network error for {symbol} at ts={next_ts}: {e}")
                await asyncio.sleep(1 + attempt)
        
        if not klines:
            logger.info(f"No more data for {symbol}")
            break
            
        # Add new candles and update next timestamp
        all_klines.extend(klines)
        next_ts = int(klines[-1][0])  # Oldest candle in batch
        
        # Debugging logs
        first_ts = int(klines[0][0])
        last_ts = int(klines[-1][0])
        logger.debug(f"Fetched {len(klines)} candles between "
                     f"{datetime.fromtimestamp(first_ts/1000)} and "
                     f"{datetime.fromtimestamp(last_ts/1000)}")

    # Sort chronologically (oldest first)
    all_klines.sort(key=lambda x: int(x[0]))
    logger.info(f"Total {len(all_klines)} candles fetched for {symbol}")
    return all_klines

async def save_to_csv_and_db(symbol, klines, list_time):
    """
    Save data to both CSV and DuckDB database.
    
    Args:
        symbol (str): Trading symbol
        klines (list): Candle data
        list_time (int): Timestamp when symbol was listed
    
    Returns:
        dict: Saving result summary
    """
    result = {
        "symbol": symbol,
        "list_date": datetime.fromtimestamp(list_time / 1000, tz=timezone.utc)
    }
    
    if not klines:
        logger.warning(f"No data to save for {symbol}")
        return result
    
    try:
        # Create DataFrame with proper column names
        df = pd.DataFrame(klines, columns=[
            "ts", "open", "high", "low", "close", "volume", "volume_currency",
            "volume_currency_quote", "confirm"
        ])
        
        # Convert timestamp to datetime
        df["ts"] = pd.to_datetime(df["ts"].astype(float), unit="ms")
        
        # Convert OHLCV columns to numeric (this is the key fix)
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'volume_currency', 'volume_currency_quote']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Log data types for debugging
        logger.debug(f"DataFrame dtypes for {symbol}: {df.dtypes.to_dict()}")
        
        # Generate CSV filename
        filename = f"{DATA_DIR}/{symbol.replace('-', '')}_first_7days_1m_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        logger.info(f"Saved {len(df)} candles to {filename}")
        
        # Insert into DuckDB
        db_result = OKXDatabaseManager.insert_candles(klines, symbol, list_time)
        
        # Optional: Analyze symbol data after insertion
        symbol_analysis = OKXDatabaseManager.analyze_symbol_data(symbol)
        if symbol_analysis:
            logger.info(f"Symbol Analysis: {symbol_analysis}")
        
        # Combine results
        result.update({
            "csv_rows": len(df),
            "db_inserted_rows": db_result.get("inserted_rows", 0)
        })
        
        return result
    
    except Exception as e:
        logger.error(f"Error saving data for {symbol}: {e}")
        logger.error(f"Error type: {type(e)}")
        
        # Backup raw data if CSV/DB insertion fails
        backup_filename = f"{DATA_DIR}/{symbol.replace('-', '')}_raw_data_1m_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt"
        with open(backup_filename, 'w') as f:
            for line in klines:
                f.write(str(line) + '\n')
        logger.info(f"Saved raw data backup to {backup_filename}")
        
        return {
            "symbol": symbol, 
            "error": str(e),
            "backup_file": backup_filename
        }
    
    except Exception as e:
        logger.error(f"Error saving data for {symbol}: {e}")
        
        # Backup raw data if CSV/DB insertion fails
        backup_filename = f"{DATA_DIR}/{symbol.replace('-', '')}_raw_data_1m_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt"
        with open(backup_filename, 'w') as f:
            for line in klines:
                f.write(str(line) + '\n')
        logger.info(f"Saved raw data backup to {backup_filename}")
        
        return {
            "symbol": symbol, 
            "error": str(e),
            "backup_file": backup_filename
        }

async def process_symbol(session, symbol_info):
    """Process a single symbol: fetch data, save to CSV and DB."""
    symbol = symbol_info["instId"]
    list_time = symbol_info["listTime"]
    
    logger.info(f"Processing {symbol} (listed {datetime.fromtimestamp(list_time / 1000, tz=timezone.utc)})...")
    
    async with semaphore:
        try:
            # First send immediate alert with listing info
            genesis_time = datetime.fromtimestamp(list_time / 1000, tz=timezone.utc)
            alert_new_listing(
                symbol=symbol,
                list_time=genesis_time,
                exchange="OKX"
            )
            
            # Then fetch and process data
            klines = await fetch_ohlcv_7_days(session, symbol, list_time)
            result = await save_to_csv_and_db(symbol, klines, list_time)
            
            # Verify we have the genesis candle
            if klines:
                first_candle_time = datetime.fromtimestamp(int(klines[0][0]) / 1000, tz=timezone.utc)
                if abs((first_candle_time - genesis_time).total_seconds()) > 120:
                    logger.warning(f"Genesis candle time mismatch for {symbol}: "
                                   f"expected {genesis_time}, got {first_candle_time}")
            
            return result
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            return {"symbol": symbol, "error": str(e)}

async def detection_cycle():
    """Single detection cycle: find and process new symbols"""
    global last_no_new_alert 
    
    connector = aiohttp.TCPConnector(
        ssl=ssl.create_default_context(),
        limit_per_host=10
    )
    
    async with aiohttp.ClientSession(connector=connector) as session:
        logger.info("Fetching current symbols from OKX...")
        symbols = await fetch_symbols(session)
        
        if not symbols:
            logger.warning("No symbols retrieved from OKX")
            return 0, 0
        
        # Get already processed symbols from database
        processed_symbols = OKXDatabaseManager.get_processed_symbols("OKX")
        
        # Filter to only new unseen symbols
        new_symbols = [s for s in symbols if s['instId'] not in processed_symbols]
        
        if not new_symbols:
            logger.info("No new symbols found this cycle")
            return 0, 0
        
        logger.info(f"Found {len(new_symbols)} new symbols")
        success_count = 0
        
        # Process each new symbol
        for symbol_info in new_symbols:
            symbol = symbol_info['instId']
            task = asyncio.create_task(process_symbol(session, symbol_info))
            current_tasks.add(task)
            task.add_done_callback(lambda t: current_tasks.remove(t))
            
            try:
                result = await task
                if 'error' not in result:
                    success_count += 1
                    # Record as processed
                    OKXDatabaseManager.record_processed_symbol("OKX", symbol)
            except asyncio.CancelledError:
                logger.warning(f"Processing cancelled for {symbol}")
            except Exception as e:
                logger.error(f"Unexpected error processing {symbol}: {e}")
        
        return len(new_symbols), success_count

async def run_detector():
    """Main detector loop with health checks and graceful shutdown"""
    global is_running, start_time
    start_time = datetime.now(timezone.utc)
    
    # Initialize databases
    OKXDatabaseManager.initialize_database()
    OKXDatabaseManager.create_tao_detector_schema()
    
    # Start health check server
    health_server = asyncio.create_task(start_health_server())
    
    # Send startup notification
    alert_new_listing(
        symbol="OKX Detector", 
        list_time=start_time,
        exchange="Service Startup",
        message="🚀 Service Started - Running 24/7 with 1-minute checks"
    )
    logger.info("Detector service started")
    
    # Handle graceful shutdown
    def signal_handler(sig, frame):
        global is_running  # Change from nonlocal to global
        logger.info(f"Received signal {sig}, shutting down gracefully...")
        is_running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    error_count = 0
    
    while is_running:
        cycle_start = datetime.now(timezone.utc)
        try:
            logger.info("Starting detection cycle")
            detected, processed = await detection_cycle()
            
            # Send cycle summary if new listings found
            if detected > 0:
                summary_msg = (
                    f"🔍 Detection Cycle Complete\n"
                    f"• New listings: {detected}\n"
                    f"• Successfully processed: {processed}\n"
                    f"• Errors: {detected - processed}"
                )
                alert_new_listing(
                    symbol="OKX Summary",
                    list_time=cycle_start,
                    exchange="Cycle Report",
                    message=summary_msg
                )
            
            # Reset error counter on success
            error_count = 0
            
        except Exception as e:
            error_count += 1
            logger.error(f"Error in detection cycle ({error_count}/{MAX_ERROR_COUNT}): {e}")
            alert_new_listing(
                symbol="OKX Error",
                list_time=datetime.now(timezone.utc),
                exchange="System Alert",
                message=f"⚠️ Detector error: {str(e)[:200]}"
            )
            
            if error_count >= MAX_ERROR_COUNT:
                logger.critical("Max error count reached. Shutting down.")
                alert_new_listing(
                    symbol="OKX CRITICAL",
                    list_time=datetime.now(timezone.utc),
                    exchange="System Alert",
                    message="🛑 CRITICAL: Detector shutting down after repeated errors"
                )
                is_running = False
        
        # Calculate time until next cycle
        cycle_duration = (datetime.now(timezone.utc) - cycle_start).total_seconds()
        sleep_time = max(1, MIN_CHECK_INTERVAL - cycle_duration)
        
        logger.info(f"Cycle completed in {cycle_duration:.2f}s. Sleeping for {sleep_time:.2f}s")
        await asyncio.sleep(sleep_time)
    
    # Cleanup
    logger.info("Shutting down...")
    alert_new_listing(
        symbol="OKX Shutdown",
        list_time=datetime.now(timezone.utc),
        exchange="System Alert",
        message="🛑 Service Stopped"
    )
    
    # Cancel all pending tasks
    for task in current_tasks:
        task.cancel()
    
    await asyncio.gather(*current_tasks, return_exceptions=True)
    health_server.cancel()
    
    logger.info("Service shutdown complete")

if __name__ == "__main__":
    # Run the detector service
    asyncio.run(run_detector())