import os
import duckdb
import pandas as pd
import logging
from datetime import datetime, timezone
import asyncio
import traceback
import logging.config

# Logging configuration
logging.basicConfig(
    level=logging.DEBUG,  # Change to DEBUG for more information
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("binance_futures_detector_debug.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database Configuration
DB_PATH_NEW_COINS = "okx_new_coins.db"
DB_PATH_ALL_COINS = "okx_futures_master.db"
NEW_COINS_TABLE = "new_coins_candles"
ALL_COINS_TABLE = "futures_candles"
TAO_DB_PATH = "master_data/duck.db"
TAO_SCHEMA = "tao_detector"
TAO_TABLE = "seen_listings"

# Default table name for backward compatibility
TABLE_NAME = ALL_COINS_TABLE
DB_PATH = DB_PATH_ALL_COINS

class OKXDatabaseManager:
    @staticmethod
    def initialize_database(context='all'):
        """
        Initialize database based on context with improved connection handling.
        """
        try:
            db_path = DB_PATH_NEW_COINS if context == 'new' else DB_PATH_ALL_COINS
            table_name = NEW_COINS_TABLE if context == 'new' else ALL_COINS_TABLE
            
            # Use a connection string with additional parameters
            conn = duckdb.connect(db_path, config={'allow_unsigned_extensions': 'true'})
            
            # Set pragmas to improve concurrency and performance
            conn.execute("PRAGMA threads=4")
            conn.execute("PRAGMA memory_limit='4GB'")
            
            conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                open_time TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                symbol VARCHAR,
                exchange VARCHAR DEFAULT 'OKX Futures',
                volume_currency DOUBLE,
                volume_currency_quote DOUBLE,
                list_date TIMESTAMP,
                data_fetch_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, open_time)
            )
            """)
            
            conn.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{context}_symbol_open_time 
            ON {table_name} (symbol, open_time)
            """)
            
            conn.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{context}_exchange_symbol 
            ON {table_name} (exchange, symbol)
            """)
            
            logger.info(f"DuckDB {context} coins database initialized successfully")
            return conn
        except Exception as e:
            logger.error(f"Error initializing {context} coins database: {e}")
            raise

    @staticmethod
    def insert_candles(candles_data, symbol, list_time, context='all', exchange='OKX Futures'):
        """
        Enhanced candle insertion with comprehensive validation
        """
        if not candles_data:
            logger.warning(f"No candles to insert for {symbol}")
            return {"symbol": symbol, "inserted_rows": 0}
        
        try:
            # Extensive logging for debugging
            logger.debug(f"Inserting candles for {symbol}")
            logger.debug(f"list_time type: {type(list_time)}")
            logger.debug(f"list_time value: {list_time}")

            # Ensure list_time is an integer
            if isinstance(list_time, str):
                try:
                    list_time = int(list_time)
                    logger.debug(f"Converted list_time to int: {list_time}")
                except (ValueError, TypeError) as e:
                    logger.error(f"Failed to convert list_time for {symbol}: {e}")
                    logger.error(f"Original list_time: {list_time}")
                    return {"symbol": symbol, "error": f"Invalid list_time: {list_time}"}
        
            # Additional type checking
            if not isinstance(list_time, (int, float)):
                logger.error(f"Unexpected list_time type for {symbol}: {type(list_time)}")
                return {"symbol": symbol, "error": f"Unexpected list_time type: {type(list_time)}"}

            # Ensure list_time is an integer
            list_time = int(list_time)

            db_path = DB_PATH_NEW_COINS if context == 'new' else DB_PATH_ALL_COINS
            table_name = NEW_COINS_TABLE if context == 'new' else ALL_COINS_TABLE
            
            conn = duckdb.connect(db_path)
            
            # Convert candles to DataFrame with comprehensive columns
            df = pd.DataFrame(candles_data, columns=[
                "open_time_ms", "open", "high", "low", "close", "volume", "volume_currency",
                "volume_currency_quote", "confirm"
            ])
            
            # Timestamp conversion and validation
            df['open_time'] = pd.to_datetime(df['open_time_ms'].astype(float), unit='ms')
            
            # Debug logging for timestamp conversion
            logger.debug(f"First timestamp: {df['open_time'].iloc[0]}")
            logger.debug(f"Timestamp dtype: {df['open_time'].dtype}")

            # Convert list_time to datetime
            df['list_date'] = pd.to_datetime(list_time, unit='ms')
            
            df['data_fetch_date'] = pd.Timestamp.now()
            df['exchange'] = exchange  # Add exchange field
            
            # Validate timestamp continuity
            df = OKXDatabaseManager.validate_timestamp_continuity(df, symbol)
            
            # Validate OHLCV data
            df = OKXDatabaseManager.validate_ohlcv_data(df, symbol)
            
            # Drop unnecessary columns
            df = df.drop(columns=['open_time_ms', 'confirm'])
            
            # Add symbol column
            df['symbol'] = symbol
            
            # Reorder columns to match table schema
            df = df[['open_time', 'open', 'high', 'low', 'close', 
                    'volume', 'symbol', 'exchange', 'volume_currency', 
                    'volume_currency_quote', 'list_date', 'data_fetch_date']]
            
            # Use INSERT OR REPLACE to handle potential duplicates
            conn.register('temp_df', df)
            conn.execute(f"""
            INSERT OR REPLACE INTO {table_name} 
            SELECT * FROM temp_df
            """)
            
            # Get the number of actually inserted rows
            inserted_rows = conn.execute(f"SELECT COUNT(*) FROM temp_df").fetchone()[0]
            
            # Log detailed insertion info
            logger.info(f"Inserted {inserted_rows} new candles for {symbol} from {exchange}")
            
            # Optional: Generate data quality report
            data_quality_report = OKXDatabaseManager.generate_data_quality_report(df)
            logger.info(f"Data Quality Report for {symbol}: {data_quality_report}")
            
            conn.close()
            
            return {
                "symbol": symbol, 
                "exchange": exchange,
                "inserted_rows": inserted_rows,
                "list_date": datetime.fromtimestamp(list_time / 1000, timezone.utc),
                "database": "new_coins" if context == 'new' else "all_coins",
                "data_quality": data_quality_report
            }
        except Exception as e:
            # Comprehensive error logging
            logger.error(f"Error inserting candles for {symbol}: {e}")
            logger.error(f"Error type: {type(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {"symbol": symbol, "error": str(e)}

    @staticmethod
    def validate_timestamp_continuity(df, symbol):
        """
        Validate timestamp continuity and detect gaps
        """
        # Sort by timestamp
        df_sorted = df.sort_values('open_time')
        
        # Check for timestamp gaps
        df_sorted['timestamp_diff'] = df_sorted['open_time'].diff()
        
        # Detect significant gaps (more than 2 minutes)
        gaps = df_sorted[df_sorted['timestamp_diff'] > pd.Timedelta(minutes=2)]
        
        if not gaps.empty:
            logger.warning(f"Timestamp gaps detected for {symbol}:")
            for _, row in gaps.iterrows():
                logger.warning(f"Gap of {row['timestamp_diff']} at {row['open_time']}")
        
        return df_sorted.drop(columns=['timestamp_diff'])

    @staticmethod
    def validate_ohlcv_data(df, symbol):
        """
        Validate and clean OHLCV data before database insertion.
        
        Args:
            df (pd.DataFrame): DataFrame with OHLCV data
            symbol (str): Trading symbol for logging
        
        Returns:
            pd.DataFrame: Cleaned and validated DataFrame
        """
        logger.debug(f"Validating OHLCV data for {symbol}")
        
        # Convert OHLCV columns to numeric, handling any string values
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'volume_currency', 'volume_currency_quote']
        
        for col in numeric_columns:
            if col in df.columns:
                # Convert to numeric, coercing errors to NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Log if any values were converted to NaN
                nan_count = df[col].isna().sum()
                if nan_count > 0:
                    logger.warning(f"Found {nan_count} non-numeric values in {col} column for {symbol}")
        
        # Remove rows where critical OHLCV data is missing or invalid
        initial_count = len(df)
        
        # Check for invalid OHLCV values (after conversion to numeric)
        invalid_mask = (
            (df['open'].isna()) |
            (df['high'].isna()) |
            (df['low'].isna()) |
            (df['close'].isna()) |
            (df['open'] <= 0) |
            (df['high'] <= 0) |
            (df['low'] <= 0) |
            (df['close'] <= 0) |
            (df['high'] < df['low']) |  # High should be >= Low
            (df['open'] > df['high']) | # Open should be <= High
            (df['open'] < df['low']) |  # Open should be >= Low
            (df['close'] > df['high']) | # Close should be <= High
            (df['close'] < df['low'])   # Close should be >= Low
        )
        
        if invalid_mask.any():
            invalid_count = invalid_mask.sum()
            logger.warning(f"Removing {invalid_count} rows with invalid OHLCV data for {symbol}")
            df = df[~invalid_mask].copy()
        
        # Check for volume issues (allow zero volume but not negative)
        if 'volume' in df.columns:
            negative_volume_mask = df['volume'] < 0
            if negative_volume_mask.any():
                negative_count = negative_volume_mask.sum()
                logger.warning(f"Found {negative_count} rows with negative volume for {symbol}")
                df = df[~negative_volume_mask].copy()
        
        # Ensure timestamp is properly formatted
        if 'open_time' in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df['open_time']):
                df['open_time'] = pd.to_datetime(df['open_time'], errors='coerce')
            
            # Remove rows with invalid timestamps
            invalid_ts_mask = df['open_time'].isna()
            if invalid_ts_mask.any():
                invalid_ts_count = invalid_ts_mask.sum()
                logger.warning(f"Removing {invalid_ts_count} rows with invalid timestamps for {symbol}")
                df = df[~invalid_ts_mask].copy()
        
        # Sort by timestamp to ensure chronological order
        if 'open_time' in df.columns and len(df) > 0:
            df = df.sort_values('open_time').reset_index(drop=True)
            
            # Check for timestamp gaps (optional warning)
            if len(df) > 1:
                time_diffs = df['open_time'].diff()
                expected_diff = pd.Timedelta(minutes=1)  # 1-minute candles
                large_gaps = time_diffs > expected_diff * 2  # More than 2 minutes gap
                
                gap_count = large_gaps.sum()
                logger.warning(f"Timestamp gaps detected for {symbol}:")
                
                # Log details of significant gaps
                gap_indices = df[large_gaps].index
                for idx in gap_indices[:5]:  # Log first 5 gaps
                    gap_size = time_diffs.iloc[idx]
                    gap_time = df['open_time'].iloc[idx]
                    logger.warning(f"Gap of {gap_size} at {gap_time}")
                
                if len(gap_indices) > 5:
                    logger.warning(f"... and {len(gap_indices) - 5} more gaps")
        
        final_count = len(df)
        if final_count != initial_count:
            logger.info(f"Data validation for {symbol}: {initial_count} -> {final_count} rows")
        
        return df

    @staticmethod
    def generate_data_quality_report(df):
        """
        Generate a comprehensive data quality report
        """
        return {
            "total_rows": len(df),
            "unique_timestamps": df['open_time'].nunique(),
            "date_range": {
                "start": df['open_time'].min(),
                "end": df['open_time'].max()
            },
            "price_stats": {
                "min_price": df['close'].min(),
                "max_price": df['close'].max(),
                "mean_price": df['close'].mean()
            },
            "volume_stats": {
                "total_volume": df['volume'].sum(),
                "mean_volume": df['volume'].mean(),
                "max_volume": df['volume'].max()
            }
        }

    @staticmethod
    def analyze_symbol_data(symbol):
        """
        Comprehensive analysis of stored symbol data
        """
        try:
            conn = duckdb.connect(DB_PATH_ALL_COINS)
            
            # Fetch comprehensive symbol data
            query = f"""
            SELECT 
                COUNT(*) as total_candles,
                MIN(open_time) as earliest_candle,
                MAX(open_time) as latest_candle,
                MIN(close) as lowest_price,
                MAX(close) as highest_price,
                AVG(close) as average_price,
                SUM(volume) as total_volume
            FROM {ALL_COINS_TABLE}
            WHERE symbol = ?
            """
            
            result = conn.execute(query, (symbol,)).fetchone()
            
            # Detect potential data issues
            data_issues = []
            if result[0] == 0:
                data_issues.append("No data available")
            
            return {
                "symbol": symbol,
                "total_candles": result[0],
                "date_range": {
                    "start": result[1],
                    "end": result[2]
                },
                "price_range": {
                    "lowest": result[3],
                    "highest": result[4],
                    "average": result[5]
                },
                "total_volume": result[6],
                "potential_issues": data_issues
            }
        except Exception as e:
            logger.error(f"Error analyzing symbol data: {e}")
            return None

    @staticmethod
    def query_database(query, params=None):
        """
        Execute a custom query on the master database.
        
        Args:
            query (str): SQL query to execute
            params (tuple, optional): Query parameters
        
        Returns:
            pandas.DataFrame: Query results
        """
        try:
            conn = duckdb.connect(DB_PATH)
            if params:
                result = conn.execute(query, params).df()
            else:
                result = conn.execute(query).df()
            conn.close()
            return result
        except Exception as e:
            logger.error(f"Database query error: {e}")
            return None

    @staticmethod
    def get_symbol_stats():
        """
        Get comprehensive statistics for all symbols in the database.
        
        Returns:
            pandas.DataFrame: Symbol statistics
        """
        query = f"""
        SELECT 
            symbol, 
            COUNT(*) as candle_count, 
            MIN(open_time) as earliest_data, 
            MAX(open_time) as latest_data,
            MIN(list_date) as first_listed_date
        FROM {TABLE_NAME}
        GROUP BY symbol
        ORDER BY candle_count DESC
        """
        return OKXDatabaseManager.query_database(query)

    @staticmethod
    def get_symbol_data(symbol, start_date=None, end_date=None):
        """
        Retrieve candle data for a specific symbol with optional date range.
        
        Args:
            symbol (str): Trading symbol
            start_date (str, optional): Start date for data retrieval
            end_date (str, optional): End date for data retrieval
        
        Returns:
            pandas.DataFrame: Candle data for the symbol
        """
        base_query = f"SELECT * FROM {TABLE_NAME} WHERE symbol = ?"
        params = [symbol]
        
        if start_date:
            base_query += " AND open_time >= ?"
            params.append(start_date)
        
        if end_date:
            base_query += " AND open_time <= ?"
            params.append(end_date)
        
        base_query += " ORDER BY open_time"
        
        return OKXDatabaseManager.query_database(base_query, tuple(params))

    @staticmethod
    def get_database_status(context='all'):
        """
        Get an overview of the database status.
        
        Args:
            context (str): 'new' or 'all'
        
        Returns:
            dict: Database status information
        """
        try:
            db_path = DB_PATH_NEW_COINS if context == 'new' else DB_PATH_ALL_COINS
            table_name = NEW_COINS_TABLE if context == 'new' else ALL_COINS_TABLE
            
            conn = duckdb.connect(db_path)
            
            # Check table existence
            tables = conn.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'main'
            """).fetchall()
            
            # Check if our specific table exists
            table_exists = any(table[0] == table_name for table in tables)
            
            # Get row count if table exists
            row_count = 0
            if table_exists:
                row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            
            conn.close()
            
            return {
                "database_path": db_path,
                "tables": [table[0] for table in tables],
                "target_table_exists": table_exists,
                "row_count": row_count,
                "context": context
            }
        except Exception as e:
            logger.error(f"Error checking {context} coins database status: {e}")
            return None

    @staticmethod
    def table_exists(table_name=TABLE_NAME, context='all'):
        """
        Check if the table exists in the database.
        
        Args:
            table_name (str, optional): Name of the table to check
            context (str, optional): Database context ('new' or 'all')
        
        Returns:
            bool: True if table exists, False otherwise
        """
        try:
            # Choose the appropriate database path based on context
            db_path = DB_PATH_NEW_COINS if context == 'new' else DB_PATH_ALL_COINS
            
            conn = duckdb.connect(db_path)
            # Use a more robust method to check table existence
            result = conn.execute(f"""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            """).fetchone()[0]
            conn.close()
            return result > 0
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False
    
    @staticmethod
    def create_tao_detector_schema():
        """Create schema matching TAO-DETECTOR requirements"""
        try:
            # Create directory if needed
            os.makedirs("master_data", exist_ok=True)
            
            conn = duckdb.connect(TAO_DB_PATH)
            conn.execute(f"""
            CREATE SCHEMA IF NOT EXISTS {TAO_SCHEMA};
            """)
            
            conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TAO_SCHEMA}.{TAO_TABLE} (
                exchange VARCHAR,
                symbol VARCHAR,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (exchange, symbol)
            )
            """)
            
            logger.info("TAO-DETECTOR schema created successfully")
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Error creating TAO-DETECTOR schema: {e}")
            return False
            
    @staticmethod
    def record_detected_listing(exchange, symbol):
        """Record detected listing in TAO-DETECTOR schema"""
        try:
            conn = duckdb.connect(TAO_DB_PATH)
            conn.execute(
                f"INSERT OR IGNORE INTO {TAO_SCHEMA}.{TAO_TABLE} (exchange, symbol) VALUES (?, ?)",
                (exchange, symbol)
            )
            conn.close()
            logger.info(f"Recorded {symbol} on {exchange} in TAO-DETECTOR schema")
            return True
        except Exception as e:
            logger.error(f"Error recording detected listing: {e}")
            return False
            
    @staticmethod
    def get_processed_symbols(exchange="OKX"):
        """Get all processed symbols from database"""
        try:
            conn = duckdb.connect(TAO_DB_PATH)
            result = conn.execute(
                f"SELECT symbol FROM {TAO_SCHEMA}.{TAO_TABLE} WHERE exchange = ?",
                (exchange,)
            ).fetchall()
            symbols = {row[0] for row in result}
            logger.info(f"Found {len(symbols)} processed symbols for {exchange}")
            return symbols
        except Exception as e:
            logger.error(f"Error getting processed symbols: {e}")
            return set()
            
    @staticmethod
    def record_processed_symbol(exchange, symbol):
        """Record processed symbol in database"""
        try:
            conn = duckdb.connect(TAO_DB_PATH)
            conn.execute(
                f"INSERT OR IGNORE INTO {TAO_SCHEMA}.{TAO_TABLE} (exchange, symbol) VALUES (?, ?)",
                (exchange, symbol)
            )
            conn.close()
            logger.info(f"Recorded {symbol} as processed for {exchange}")
            return True
        except Exception as e:
            logger.error(f"Error recording processed symbol: {e}")
            return False
            
    @staticmethod
    def get_last_processed_time():
        """Get the timestamp of the last processed symbol"""
        try:
            conn = duckdb.connect(TAO_DB_PATH)
            result = conn.execute(
                f"SELECT MAX(detected_at) FROM {TAO_SCHEMA}.{TAO_TABLE}"
            ).fetchone()
            last_time = result[0] if result[0] else datetime.min.replace(tzinfo=timezone.utc)
            logger.info(f"Last processed time: {last_time}")
            return last_time
        except Exception as e:
            logger.error(f"Error getting last processed time: {e}")
            return datetime.min.replace(tzinfo=timezone.utc)

    async def detection_cycle():
        """Single detection cycle: find and process new symbols"""
        connector = aiohttp.TCPConnector(
            ssl=ssl.create_default_context(),
            limit_per_host=10
        )
        
        async with aiohttp.ClientSession(connector=connector) as session:
            logger.info("Fetching current symbols from Binance Futures...")
            
            # Test API access
            if not await test_api_access(session):
                logger.error("API access test failed")
                send_telegram("❌ Binance Futures API Access Failed")
                return 0, 0
            
            try:
                # Fetch symbols with detailed error handling
                symbols = await fetch_symbols(session)
                
                if not symbols:
                    logger.warning("No symbols retrieved from Binance Futures")
                    send_telegram("⚠️ No Binance Futures Symbols Retrieved")
                    return 0, 0
                
                # Rest of the existing code...
            
            except Exception as e:
                logger.error(f"Comprehensive detection cycle error: {e}")
                send_telegram(f"🚨 Binance Futures Detection Error: {str(e)[:200]}")
                return 0, 0

    async def process_symbol(session, rate_limiter, symbol_info):
        """Process a single symbol."""
        symbol = symbol_info["instId"]
        list_time = symbol_info["listTime"]
        
        # Add extensive logging
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
        
            # Save to CSV (keep existing functionality)
            csv_result = await save_to_csv(symbol, klines, first_time)
        
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
        
            return {**csv_result, **db_result}
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            logger.error(f"Error type: {type(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {"symbol": symbol, "list_date": None, "candles": 0}