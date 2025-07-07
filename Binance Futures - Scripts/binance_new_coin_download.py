import os
import json
import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timedelta, timezone
import logging
import ssl
import signal
import time
import pickle
import traceback

# Import custom utilities
from binance_duckdb_utils import BinanceDatabaseManager
from binance_telegram_utils import send_telegram, alert_new_listing

# Constants
BINANCE_FUTURES_URL = "https://fapi.binance.com"
KLINES_ENDPOINT = "/fapi/v1/klines"
EXCHANGE_INFO_ENDPOINT = "/fapi/v1/exchangeInfo"
DATA_DIR = "data"
INTERVAL = "1m"
LIMIT = 1500
DAYS = 7
CONCURRENCY = 2
MAX_RETRIES = 3
BASE_REQUEST_DELAY = 0.3
BATCH_SIZE = 15
BATCH_DELAY = 2.4
START_YEAR = 2019
CHECKPOINT_FILE = f"{DATA_DIR}/checkpoint.pkl"
MAX_LISTING_AGE_HOURS = 24  # Only process listings from last 24 hours

# Detection cycle constants
MIN_CHECK_INTERVAL = 15  # 1 minute
MAX_ERROR_COUNT = 5  # Shutdown after 5 consecutive errors

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s", "module": "%(module)s", "function": "%(funcName)s"}',
    handlers=[
        logging.FileHandler("binance_futures_detector.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Global variables for graceful shutdown
is_running = True
start_time = None
current_tasks = set()
last_no_new_alert = datetime.now(timezone.utc)  # Initialize with current time

os.makedirs(DATA_DIR, exist_ok=True)
semaphore = asyncio.Semaphore(CONCURRENCY)

async def test_api_access(session):
    """Test API access with a single request to detect bans."""
    url = f"{BINANCE_FUTURES_URL}{KLINES_ENDPOINT}"
    params = {"symbol": "BTCUSDT", "interval": INTERVAL, "limit": 1}
    try:
        async with session.get(url, params=params) as resp:
            logger.info(f"API Test - Status Code: {resp.status}")
            logger.info(f"API Test - Headers: {resp.headers}")
            
            response_text = await resp.text()
            logger.info(f"API Test - Raw Response: {response_text[:500]}")
            
            if resp.status == 418 or resp.status == 429:
                logger.warning(f"API test failed: HTTP {resp.status}, waiting...")
                return False
            
            if resp.status != 200:
                logger.warning(f"API test failed: HTTP {resp.status}")
                return False
            
            try:
                data = json.loads(response_text)
                logger.info(f"API Test - Parsed Data: {data[:2]}")
                return True
            except json.JSONDecodeError as e:
                logger.error(f"JSON Decode Error in API Test: {e}")
                logger.error(f"Problematic response: {response_text}")
                return False
    except Exception as e:
        logger.error(f"API Test ClientError: {e}")
        return False

async def fetch_symbols(session):
    """Fetch all futures symbols from Binance with DEBUG logs."""
    try:
        logger.info("Fetching symbols from Binance Futures API...")
        
        async with session.get(f"{BINANCE_FUTURES_URL}{EXCHANGE_INFO_ENDPOINT}") as resp:
            data = await resp.json()
            
            if not isinstance(data, dict) or "symbols" not in data:
                logger.error("Invalid API response structure")
                return []

            symbols = []
            current_time = datetime.now(timezone.utc).timestamp()
            newest_listing_time = 0  # Track the newest onboardDate
            
            for s in data["symbols"]:
                if s.get("contractType") != "PERPETUAL":
                    logger.debug(f"Skipping non-PERPETUAL symbol: {s['symbol']}")
                    continue

                onboard_date = s.get("onboardDate", 0)
                list_time = onboard_date / 1000  # Convert to seconds
                
                # Debug: Log raw onboardDate and converted time
                logger.debug(
                    f"Symbol: {s['symbol']}, "
                    f"onboardDate (ms): {onboard_date}, "
                    f"Converted listTime (UTC): {datetime.fromtimestamp(list_time, tz=timezone.utc)}"
                )

                # Track the newest listing time
                if list_time > newest_listing_time:
                    newest_listing_time = list_time

                # Skip if no onboardDate or invalid
                if onboard_date == 0:
                    logger.warning(f"No onboardDate for {s['symbol']}, skipping")
                    continue
                if list_time > current_time + 3600:
                    logger.warning(f"Future-dated listing: {s['symbol']} (listTime: {datetime.fromtimestamp(list_time, tz=timezone.utc)})")
                    continue
                if list_time < datetime(START_YEAR, 1, 1, tzinfo=timezone.utc).timestamp():
                    logger.warning(f"Invalid old listing: {s['symbol']} (listTime: {datetime.fromtimestamp(list_time, tz=timezone.utc)})")
                    continue

                symbols.append({
                    "symbol": s["symbol"],
                    "listTime": list_time,
                    "baseAsset": s.get("baseAsset", ""),
                    "quoteAsset": s.get("quoteAsset", ""),
                    "status": s.get("status", "")
                })

            # Log summary of listings
            logger.info(
                f"Fetched {len(symbols)} PERPETUAL symbols. "
                f"Newest listing: {datetime.fromtimestamp(newest_listing_time, tz=timezone.utc)}"
            )
            
            return symbols

    except Exception as e:
        logger.error(f"Failed to fetch symbols: {e}\n{traceback.format_exc()}")
        return []

async def get_first_candle_time(session, symbol):
    """Find the earliest 1-minute candle with improved validation."""
    start_time = int(datetime(START_YEAR, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    end_time = int(datetime.now(timezone.utc).timestamp() * 1000)
    params = {
        "symbol": symbol,
        "interval": INTERVAL,
        "limit": LIMIT
    }
    url = f"{BINANCE_FUTURES_URL}{KLINES_ENDPOINT}"
    step = (end_time - start_time) // 4

    while step > 60_000:
        mid_time = start_time + step
        params["endTime"] = mid_time
        for attempt in range(MAX_RETRIES):
            try:
                await asyncio.sleep(BASE_REQUEST_DELAY * (2 ** attempt))
                async with session.get(url, params=params) as resp:
                    weight_used = int(resp.headers.get("X-MBX-USED-WEIGHT-1M", 0))
                    if weight_used > 600 or resp.status == 418:
                        logger.warning(f"High weight ({weight_used}) or HTTP 418 for {symbol}, pausing 3s...")
                        await asyncio.sleep(3.0)
                    if resp.status == 429 or resp.status == 418:
                        logger.warning(f"Rate limit or ban (HTTP {resp.status}) for {symbol}, waiting {10 * (2 ** attempt)}s...")
                        await asyncio.sleep(10 * (2 ** attempt))
                        continue
                    if resp.status != 200:
                        logger.warning(f"HTTP {resp.status} for {symbol}, attempt {attempt + 1}")
                        await asyncio.sleep(5)
                        continue
                    data = await resp.json()
                    if isinstance(data, dict) and data.get("code"):
                        logger.warning(f"API error for {symbol}: {data.get('msg')}")
                        await asyncio.sleep(5)
                        continue
                    if not data:
                        end_time = mid_time - 60_000
                        step = (end_time - start_time) // 4
                        break
                    earliest_time = int(data[-1][0])
                    logger.debug(f"Fetched {len(data)} candles for {symbol}, earliest: {datetime.fromtimestamp(earliest_time / 1000, timezone.utc)}")
                    if len(data) < LIMIT:
                        logger.info(f"Earliest 1m data for {symbol} at {datetime.fromtimestamp(earliest_time / 1000, timezone.utc)}")
                        return earliest_time
                    end_time = earliest_time - 60_000
                    step = (end_time - start_time) // 4
                    break
            except aiohttp.ClientError as e:
                logger.error(f"ClientError for {symbol}: {e}")
                await asyncio.sleep(5)
                if attempt == MAX_RETRIES - 1:
                    logger.error(f"Max retries reached for {symbol}")
                    return None
        else:
            continue
        if end_time - start_time <= 60_000:
            break

    params.pop("endTime", None)
    for attempt in range(MAX_RETRIES):
        try:
            await asyncio.sleep(BASE_REQUEST_DELAY * (2 ** attempt))
            async with session.get(url, params=params) as resp:
                weight_used = int(resp.headers.get("X-MBX-USED-WEIGHT-1M", 0))
                if weight_used > 600 or resp.status == 418:
                    logger.warning(f"High weight ({weight_used}) or HTTP 418 for {symbol}, pausing 3s...")
                    await asyncio.sleep(3.0)
                if resp.status == 429 or resp.status == 418:
                    logger.warning(f"Rate limit or ban (HTTP {resp.status}) for {symbol}, waiting {10 * (2 ** attempt)}s...")
                    await asyncio.sleep(10 * (2 ** attempt))
                    continue
                if resp.status != 200:
                    logger.warning(f"HTTP {resp.status} for {symbol}, attempt {attempt + 1}")
                    await asyncio.sleep(5)
                    continue
                data = await resp.json()
                if isinstance(data, dict) and data.get("code"):
                    logger.warning(f"API error for {symbol}: {data.get('msg')}")
                    await asyncio.sleep(5)
                    continue
                if not data:
                    logger.warning(f"No 1m data for {symbol}")
                    return None
                earliest_time = int(data[-1][0])
                logger.info(f"Earliest 1m data for {symbol} at {datetime.fromtimestamp(earliest_time / 1000, timezone.utc)}")
                return earliest_time
        except aiohttp.ClientError as e:
            logger.error(f"ClientError for {symbol}: {e}")
            await asyncio.sleep(5)
    logger.warning(f"No 1m data for {symbol}")
    return None

async def fetch_ohlcv_7_days(session, symbol, start_time):
    """Fetch 7 days of 1-minute OHLCV data with improved error handling."""
    end_time = start_time + DAYS * 24 * 60 * 60 * 1000
    all_klines = []
    current_time = start_time
    params = {
        "symbol": symbol,
        "interval": INTERVAL,
        "limit": LIMIT
    }
    url = f"{BINANCE_FUTURES_URL}{KLINES_ENDPOINT}"

    while current_time < end_time:
        params["startTime"] = int(current_time)
        for attempt in range(MAX_RETRIES):
            try:
                await asyncio.sleep(BASE_REQUEST_DELAY * (2 ** attempt))
                async with session.get(url, params=params) as resp:
                    weight_used = int(resp.headers.get("X-MBX-USED-WEIGHT-1M", 0))
                    if weight_used > 600 or resp.status == 418:
                        logger.warning(f"High weight ({weight_used}) or HTTP 418 for {symbol}, pausing 3s...")
                        await asyncio.sleep(3.0)
                    if resp.status == 429 or resp.status == 418:
                        logger.warning(f"Rate limit or ban (HTTP {resp.status}) for {symbol}, waiting {10 * (2 ** attempt)}s...")
                        await asyncio.sleep(10 * (2 ** attempt))
                        continue
                    if resp.status != 200:
                        logger.warning(f"HTTP {resp.status} for {symbol} at {current_time}")
                        await asyncio.sleep(5)
                        continue
                    data = await resp.json()
                    if isinstance(data, dict) and data.get("code"):
                        logger.warning(f"API error for {symbol} at {current_time}: {data.get('msg')}")
                        await asyncio.sleep(5)
                        continue
                    klines = data
                    if not klines:
                        logger.warning(f"No more data for {symbol} at {datetime.fromtimestamp(current_time / 1000, timezone.utc)}")
                        break
                    
                    # Validate timestamp continuity
                    prev_time = None
                    for kline in klines:
                        current_kline_time = int(kline[0])
                        if prev_time and (current_kline_time - prev_time) != 60_000:
                            logger.warning(f"Timestamp gap detected for {symbol}: {prev_time} -> {current_kline_time}")
                        prev_time = current_kline_time
                    
                    all_klines.extend(klines)
                    try:
                        next_timestamp = int(klines[-1][0]) + 60_000
                        current_time = next_timestamp
                    except (ValueError, IndexError) as e:
                        logger.error(f"Error parsing timestamp for {symbol}: {e}")
                        break
                    break
            except aiohttp.ClientError as e:
                logger.error(f"ClientError for {symbol} at {current_time}: {e}")
                await asyncio.sleep(5)
        else:
            break
        if not klines:
            break
    
    logger.info(f"Fetched {len(all_klines)} candles for {symbol}")
    return all_klines

async def save_to_csv(symbol, klines, start_time):
    """Save OHLCV data to CSV with enhanced metadata."""
    result = {
        "symbol": symbol,
        "list_date": datetime.fromtimestamp(start_time / 1000, timezone.utc) if start_time else None,
        "candles": 0,
        "first_timestamp": None,
        "last_timestamp": None
    }
    
    if not klines:
        logger.warning(f"No data to save for {symbol}")
        return result
    
    try:
        df = pd.DataFrame(klines, columns=[
            "ts", "open", "high", "low", "close", "volume", "close_time",
            "quote_volume", "trades", "taker_buy_volume", "taker_buy_quote_volume", "ignore"
        ])
        
        # Convert and validate timestamps
        df["ts"] = pd.to_datetime(df["ts"].astype(float), unit="ms")
        result["first_timestamp"] = df["ts"].min()
        result["last_timestamp"] = df["ts"].max()
        
        # Add metadata columns
        df["symbol"] = symbol
        df["listing_date"] = datetime.fromtimestamp(start_time / 1000, timezone.utc) if start_time else pd.NaT
        
        filename = f"{DATA_DIR}/{symbol.replace('-', '')}_first_7days_1m_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        
        logger.info(f"Saved {len(df)} candles to {filename}")
        result["candles"] = len(df)
        result["filename"] = filename
        
    except Exception as e:
        logger.error(f"Error saving data for {symbol}: {e}")
        backup_filename = f"{DATA_DIR}/{symbol.replace('-', '')}_raw_data_1m_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt"
        with open(backup_filename, 'w') as f:
            for line in klines:
                f.write(str(line) + '\n')
        logger.info(f"Saved raw data backup to {backup_filename}")
        result["error"] = str(e)
        result["backup_file"] = backup_filename
    
    return result

async def save_summary(results):
    """Save summary of processed symbols with enhanced details."""
    try:
        if not results:
            logger.warning("No results to save in summary")
            return
            
        df = pd.DataFrame(results)
        
        # Calculate additional metrics
        df["data_duration_hours"] = (df["last_timestamp"] - df["first_timestamp"]).dt.total_seconds() / 3600
        df["candles_per_hour"] = df["candles"] / df["data_duration_hours"]
        df["time_discrepancy_minutes"] = df["time_discrepancy_minutes"].round(2)
        
        filename = f"{DATA_DIR}/binance_futures_first_7days_summary_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        
        logger.info(f"Saved detailed summary to {filename}")
        return filename
    except Exception as e:
        logger.error(f"Error saving summary: {e}")
        return None

async def process_symbol(session, symbol_info, processed_symbols):
    """Process a single symbol with comprehensive validation and alerting."""
    symbol = symbol_info["symbol"]
    list_time = symbol_info["listTime"]
    
    # Skip if already processed
    if symbol in processed_symbols:
        logger.info(f"Skipping {symbol}, already processed.")
        return None
    
    # Validate listing date
    current_time = datetime.now(timezone.utc).timestamp()
    listing_age = current_time - list_time
    
    if listing_age > (MAX_LISTING_AGE_HOURS * 3600):
        logger.info(f"Skipping {symbol}, listed {listing_age/3600:.1f} hours ago (older than {MAX_LISTING_AGE_HOURS} hour threshold)")
        return None
    
    if list_time > current_time + 3600:  # 1 hour in future
        logger.warning(f"Suspicious future listing date for {symbol}: {datetime.fromtimestamp(list_time, tz=timezone.utc)}")
        return None
    
    listing_date = datetime.fromtimestamp(list_time, tz=timezone.utc)
    logger.info(f"Processing {symbol} (officially listed {listing_date})...")
    
    try:
        # Get first trade time
        first_time = await get_first_candle_time(session, symbol)
        if not first_time:
            logger.warning(f"No valid first candle for {symbol}")
            return None
        
        first_trade_time = datetime.fromtimestamp(first_time/1000, tz=timezone.utc)
        
        # Compare listTime and first_time
        time_diff = (first_time / 1000) - list_time
        logger.info(f"Symbol: {symbol}, Official listTime: {list_time} ({listing_date}), First trade: {first_time/1000} ({first_trade_time}), Difference: {time_diff/60:.2f} minutes")
        if abs(time_diff) > 3600:  # More than 1 hour difference
            logger.warning(f"Large discrepancy between official listing ({listing_date}) and first trade ({first_trade_time}) for {symbol}: {time_diff/3600:.2f} hours")
        
        # Use the earlier of the two timestamps for data fetching
        effective_start_time = min(list_time * 1000, first_time)
        effective_start_time_dt = datetime.fromtimestamp(effective_start_time/1000, tz=timezone.utc)
        logger.info(f"Using effective start time for {symbol}: {effective_start_time_dt}")
        
        # Send enhanced alert
        alert_new_listing(
            symbol=symbol,
            list_time=listing_date,
            exchange="Binance Futures"
        )
        
        # Fetch 7 days of data
        klines = await fetch_ohlcv_7_days(session, symbol, effective_start_time)
        
        # Save to CSV
        result = await save_to_csv(symbol, klines, effective_start_time)
        
        # Insert into DuckDB
        if klines:
            db_result = BinanceDatabaseManager.insert_candles(
                klines, 
                symbol, 
                effective_start_time  # Use effective start time
            )
            logger.info(f"Database insertion result: {db_result}")
        
        # Record as processed
        BinanceDatabaseManager.record_detected_listing("Binance Futures", symbol)
        processed_symbols.add(symbol)
        
        # Add metadata to result
        result.update({
            "official_listing_date": listing_date,
            "first_trade_date": first_trade_time,
            "effective_start_time": effective_start_time_dt,
            "listing_age_hours": listing_age / 3600,
            "time_discrepancy_minutes": time_diff / 60
        })
        
        return result
        
    except Exception as e:
        logger.error(f"Error processing {symbol}: {e}")
        logger.error(traceback.format_exc())
        send_telegram(f"⚠️ Error processing {symbol}: {str(e)[:200]}")
        return None

async def process_batch(session, batch, processed_symbols):
    """Process a batch of symbols."""
    batch_results = []
    for symbol_info in batch:
        result = await process_symbol(session, symbol_info, processed_symbols)
        if result:
            batch_results.append(result)
    return batch_results

async def detection_cycle(session):
    """Single detection cycle with enhanced monitoring and alerts."""
    global last_no_new_alert  # Explicitly declare global
    
    try:
        # Test API access
        cycle_start = datetime.now(timezone.utc)
        if not await test_api_access(session):
            logger.error("API access test failed")
            send_telegram("❌ Binance Futures API Access Failed")
            return 0, 0

        # Fetch symbols with timestamp
        fetch_start = datetime.now(timezone.utc)
        symbols = await fetch_symbols(session)
        fetch_duration = (datetime.now(timezone.utc) - fetch_start).total_seconds()
        
        if not symbols:
            logger.error("No symbols fetched from API")
            send_telegram("❌ Failed to fetch symbols from Binance Futures API")
            return 0, 0

        # Get processed symbols
        processed_symbols = BinanceDatabaseManager.get_processed_symbols()
        logger.info(f"Already processed symbols: {len(processed_symbols)}")

        # Filter new symbols (last MAX_LISTING_AGE_HOURS)
        current_time = datetime.now(timezone.utc).timestamp()
        new_symbols = []
        newest_listing_time = 0
        symbols_by_age = {24: 0, 12: 0, 6: 0, 1: 0}  # Track listings by age buckets
        
        for s in symbols:
            listing_age = current_time - s["listTime"]
            is_new = (
                s["symbol"] not in processed_symbols
                and listing_age <= (MAX_LISTING_AGE_HOURS * 3600)
            )
            
            # Track newest listing time
            if s["listTime"] > newest_listing_time:
                newest_listing_time = s["listTime"]
            
            # Count symbols by age buckets
            age_hours = listing_age / 3600
            if age_hours <= 1:
                symbols_by_age[1] += 1
            elif age_hours <= 6:
                symbols_by_age[6] += 1
            elif age_hours <= 12:
                symbols_by_age[12] += 1
            elif age_hours <= 24:
                symbols_by_age[24] += 1

            if is_new:
                new_symbols.append(s)

        # Prepare monitoring statistics
        newest_listing_str = datetime.fromtimestamp(newest_listing_time, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        stats_msg = (
            f"📊 Binance Futures Listings Statistics\n"
            f"• Total symbols: {len(symbols)}\n"
            f"• New listings (last {MAX_LISTING_AGE_HOURS}h): {len(new_symbols)}\n"
            f"• Last new listing: {newest_listing_str}\n"
            f"• Listings by age:\n"
            f"  - ≤1h: {symbols_by_age[1]}\n"
            f"  - ≤6h: {symbols_by_age[6]}\n"
            f"  - ≤12h: {symbols_by_age[12]}\n"
            f"  - ≤24h: {symbols_by_age[24]}\n"
            f"• Fetch duration: {fetch_duration:.2f}s"
        )
        
        logger.info(stats_msg)
        
        # Send periodic monitoring stats (every 6 hours)
        now = datetime.now(timezone.utc)
        if (now - last_no_new_alert).total_seconds() >= 6 * 3600:
            send_telegram(stats_msg)
            last_no_new_alert = now

        if not new_symbols:
            logger.info(f"No new symbols detected in the last {MAX_LISTING_AGE_HOURS} hours")
            return 0, 0
        
        # Process new symbols
        process_start = datetime.now(timezone.utc)
        success_count = 0
        results = []
        
        for i in range(0, len(new_symbols), BATCH_SIZE):
            batch = new_symbols[i:i + BATCH_SIZE]
            logger.info(f"Processing batch {i//BATCH_SIZE + 1}/{(len(new_symbols) + BATCH_SIZE - 1)//BATCH_SIZE}")
            
            batch_results = await process_batch(session, batch, processed_symbols)
            results.extend(batch_results)
            success_count += len(batch_results)
            
            # Rate limiting between batches
            await asyncio.sleep(BATCH_DELAY)
        
        process_duration = (datetime.now(timezone.utc) - process_start).total_seconds()
        
        # Save summary
        summary_file = await save_summary(results)
        if summary_file:
            logger.info(f"Cycle summary saved to {summary_file}")

        
        return len(new_symbols), success_count
    
    except Exception as e:
        logger.error(f"Detection cycle failed: {e}")
        send_telegram(f"⚠️ Binance Futures Detection Error: {str(e)[:200]}")
        return 0, 0

async def run_detector():
    """Main detector loop with enhanced monitoring."""
    global is_running, start_time, last_no_new_alert
    start_time = datetime.now(timezone.utc)
    
    # Initialize databases
    BinanceDatabaseManager.initialize_database()
    BinanceDatabaseManager.create_tao_detector_schema()
    
    # Create aiohttp session
    connector = aiohttp.TCPConnector(ssl=ssl.create_default_context())
    async with aiohttp.ClientSession(connector=connector) as session:
        # Send startup notification
        alert_new_listing(
            symbol="Binance Futures Detector", 
            list_time=start_time,
            exchange="Service Startup",
            message=f"Starting detection service at {start_time}"
        )
        logger.info("Detector service started at %s", start_time)
        
        # Graceful shutdown handler
        def signal_handler(sig, frame):
            global is_running
            logger.info(f"Received signal {sig}, shutting down gracefully...")
            is_running = False
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        error_count = 0
        
        while is_running:
            cycle_start = datetime.now(timezone.utc)
            try:
                logger.info("Starting detection cycle")
                detected, processed = await detection_cycle(session)
                
                if detected > 0:
                    summary_msg = (
                        f"🔍 Binance Futures Detection Cycle Complete\n"
                        f"• New listings detected: {detected}\n"
                        f"• Successfully processed: {processed}\n"
                        f"• Errors: {detected - processed}\n"
                        f"• Cycle duration: {(datetime.now(timezone.utc) - cycle_start).total_seconds():.1f}"
                    )
                    send_telegram(summary_msg)
                
                # Reset error counter on success
                error_count = 0
                
            except Exception as e:
                error_count += 1
                logger.error(f"Error in detection cycle ({error_count}/{MAX_ERROR_COUNT}): {e}")
                send_telegram(f"⚠️ Binance Futures Detector Error: {str(e)[:200]}")
                
                if error_count >= MAX_ERROR_COUNT:
                    logger.critical("Max error count reached. Shutting down.")
                    send_telegram("🛑 CRITICAL: Binance Futures Detector shutting down after repeated errors")
                    is_running = False
            
            # Calculate sleep time
            cycle_duration = (datetime.now(timezone.utc) - cycle_start).total_seconds()
            sleep_time = max(1, MIN_CHECK_INTERVAL - cycle_duration)
            
            logger.info(f"Cycle completed in {cycle_duration:.2f}s. Sleeping for {sleep_time:.2f}s")
            await asyncio.sleep(sleep_time)
        
        # Cleanup
        uptime = datetime.now(timezone.utc) - start_time
        logger.info("Shutting down after %s of uptime", uptime)
        send_telegram(f"🛑 Binance Futures Detector Stopped after {uptime}")

if __name__ == "__main__":
    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Run the detector service
    asyncio.run(run_detector())