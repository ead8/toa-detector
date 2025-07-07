import os
import duckdb
import pandas as pd
import logging
from datetime import datetime, timezone

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Database Configuration
DB_PATH_NEW_COINS = "binance_new_coins.db"
DB_PATH_ALL_COINS = "binance_futures_master.db"
NEW_COINS_TABLE = "new_coins_candles"
ALL_COINS_TABLE = "futures_candles"

class BinanceDatabaseManager:
    @classmethod
    def initialize_database(cls, context='all'):
        """
        Initialize database based on context with improved connection handling.
        """
        try:
            db_path = DB_PATH_NEW_COINS if context == 'new' else DB_PATH_ALL_COINS
            table_name = NEW_COINS_TABLE if context == 'new' else ALL_COINS_TABLE
            
            # Create master_data directory if it doesn't exist
            os.makedirs("master_data", exist_ok=True)
            
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
                exchange VARCHAR DEFAULT 'Binance Futures',
                quote_volume DOUBLE,
                trades INTEGER,
                taker_buy_volume DOUBLE,
                taker_buy_quote_volume DOUBLE,
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

    @classmethod
    def create_tao_detector_schema(cls):
        """Create schema matching TAO-DETECTOR requirements"""
        try:
            os.makedirs("master_data", exist_ok=True)
            conn = duckdb.connect("master_data/binance_duck.db")
            
            conn.execute("""
            CREATE SCHEMA IF NOT EXISTS tao_detector;
            """)
            
            conn.execute("""
            CREATE TABLE IF NOT EXISTS tao_detector.seen_listings (
                exchange VARCHAR,
                symbol VARCHAR,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (exchange, symbol)
            )
            """)
            
            logger.info("Created TAO-DETECTOR schema successfully")
            conn.close()
        except Exception as e:
            logger.error(f"Error creating TAO-DETECTOR schema: {e}")

    @classmethod
    def record_detected_listing(cls, exchange, symbol):
        """Record detected listing in TAO-DETECTOR schema"""
        try:
            conn = duckdb.connect("master_data/binance_duck.db")
            conn.execute(
                "INSERT OR IGNORE INTO tao_detector.seen_listings (exchange, symbol) VALUES (?, ?)",
                (exchange, symbol)
            )
            conn.close()
            logger.info(f"Recorded {symbol} on {exchange} in TAO-DETECTOR schema")
        except Exception as e:
            logger.error(f"Error recording detected listing: {e}")

    @classmethod
    def get_processed_symbols(cls, exchange="Binance Futures"):
        """Get all processed symbols from database"""
        try:
            conn = duckdb.connect("master_data/binance_duck.db")
            result = conn.execute(
                "SELECT symbol FROM tao_detector.seen_listings WHERE exchange = ?",
                (exchange,)
            ).fetchall()
            return {row[0] for row in result}
        except Exception as e:
            logger.error(f"Error getting processed symbols: {e}")
            return set()

    @classmethod
    def safe_convert(cls, value, convert_func, default=None):
        """Helper function for safe type conversion"""
        try:
            if value is None or str(value).lower() in ('none', 'null', ''):
                return default
            return convert_func(value)
        except (ValueError, TypeError) as e:
            logger.warning(f"Conversion error for {value}: {e}")
            return default

    @classmethod
    def convert_candle_data(cls, candle_data):
        """Convert raw candle data to proper types with validation"""
        try:
            return [
                cls.safe_convert(candle_data[0], int, 0),        # timestamp (ms)
                cls.safe_convert(candle_data[1], float, 0.0),      # open
                cls.safe_convert(candle_data[2], float, 0.0),      # high
                cls.safe_convert(candle_data[3], float, 0.0),      # low
                cls.safe_convert(candle_data[4], float, 0.0),      # close
                cls.safe_convert(candle_data[5], float, 0.0),      # volume
                cls.safe_convert(candle_data[6], int, 0),          # close_time
                cls.safe_convert(candle_data[7], float, 0.0),      # quote_volume
                cls.safe_convert(candle_data[8], int, 0),          # trades
                cls.safe_convert(candle_data[9], float, 0.0),      # taker_buy_volume
                cls.safe_convert(candle_data[10], float, 0.0),     # taker_buy_quote_volume
            ]
        except IndexError as e:
            logger.error(f"Incomplete candle data: {candle_data}")
            return None

    @classmethod
    def insert_candles(cls, candles_data, symbol, list_time, context='all', exchange='Binance Futures'):
        """
        Enhanced candle insertion with proper column mapping
        """
        if not candles_data:
            logger.warning(f"No candles to insert for {symbol}")
            return {"symbol": symbol, "inserted_rows": 0}
        
        try:
            db_path = DB_PATH_NEW_COINS if context == 'new' else DB_PATH_ALL_COINS
            table_name = NEW_COINS_TABLE if context == 'new' else ALL_COINS_TABLE
            
            # Convert and validate all candles
            converted_data = []
            for candle in candles_data:
                converted = cls.convert_candle_data(candle)
                if converted:
                    converted_data.append(converted)
            
            if not converted_data:
                logger.error(f"No valid candles to insert for {symbol}")
                return {"symbol": symbol, "error": "No valid candles"}
            
            # Create DataFrame with converted data
            df = pd.DataFrame(converted_data, columns=[
                "open_time_ms", "open", "high", "low", "close", "volume", 
                "close_time_ms", "quote_volume", "trades", 
                "taker_buy_volume", "taker_buy_quote_volume"
            ])
            
            # Add metadata columns with proper naming
            df['open_time'] = pd.to_datetime(df['open_time_ms'], unit='ms')
            df['list_date'] = pd.to_datetime(cls.safe_convert(list_time, int, 0), unit='ms')
            df['data_fetch_date'] = pd.Timestamp.now()
            df['symbol'] = symbol
            df['exchange'] = exchange
            
            # Validate data before insertion
            df = cls.validate_timestamp_continuity(df, symbol)
            df = cls.validate_ohlcv_data(df, symbol)
            
            # Select only columns that match the new database schema
            final_columns = [
                'open_time', 'open', 'high', 'low', 'close', 'volume',
                'symbol', 'exchange', 'quote_volume', 'trades',
                'taker_buy_volume', 'taker_buy_quote_volume',
                'list_date', 'data_fetch_date'
            ]
            df = df[final_columns]
            
            # Insert into database
            conn = duckdb.connect(db_path)
            
            # Use INSERT OR IGNORE to avoid duplicates
            conn.register('temp_df', df)
            result = conn.execute(f"""
                INSERT OR IGNORE INTO {table_name}
                SELECT * FROM temp_df
            """)
            
            inserted_rows = conn.execute(f"SELECT COUNT(*) FROM temp_df").fetchone()[0]
            conn.close()
            
            logger.info(f"Successfully inserted {inserted_rows} candles for {symbol} from {exchange}")
            return {
                "symbol": symbol,
                "exchange": exchange,
                "inserted_rows": inserted_rows,
                "first_timestamp": df['open_time'].min(),
                "last_timestamp": df['open_time'].max()
            }
            
        except Exception as e:
            logger.error(f"Error inserting candles for {symbol}: {e}")
            return {"symbol": symbol, "error": str(e)}

    @classmethod
    def get_all_symbols(cls):
        """Get all symbols currently in the database"""
        try:
            conn = duckdb.connect(DB_PATH_ALL_COINS)
            result = conn.execute(f"SELECT DISTINCT symbol FROM {ALL_COINS_TABLE}").fetchall()
            conn.close()
            return {row[0] for row in result}
        except Exception as e:
            logger.error(f"Error getting all symbols: {e}")
            return set()

    @classmethod
    def get_symbol_stats(cls):
        """Get comprehensive statistics for all symbols in the database"""
        try:
            conn = duckdb.connect(DB_PATH_ALL_COINS)
            query = f"""
            SELECT 
                symbol, 
                COUNT(*) as candle_count, 
                MIN(open_time) as earliest_data, 
                MAX(open_time) as latest_data,
                MIN(list_date) as first_listed_date
            FROM {ALL_COINS_TABLE}
            GROUP BY symbol
            ORDER BY candle_count DESC
            """
            df = conn.execute(query).df()
            conn.close()
            return df
        except Exception as e:
            logger.error(f"Error getting symbol stats: {e}")
            return None

    @classmethod
    def validate_timestamp_continuity(cls, df, symbol):
        """Validate timestamp continuity and detect gaps"""
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

    @classmethod
    def validate_ohlcv_data(cls, df, symbol):
        """Validate and clean OHLCV data before database insertion"""
        logger.debug(f"Validating OHLCV data for {symbol}")
        
        # Convert OHLCV columns to numeric, handling any string values
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'quote_volume', 
                          'taker_buy_volume', 'taker_buy_quote_volume']
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Log if any values were converted to NaN
                nan_count = df[col].isna().sum()
                if nan_count > 0:
                    logger.warning(f"Found {nan_count} non-numeric values in {col} column for {symbol}")
        
        # Remove rows where critical OHLCV data is missing or invalid
        initial_count = len(df)
        
        # Check for invalid OHLCV values
        invalid_mask = (
            (df['open'].isna()) |
            (df['high'].isna()) |
            (df['low'].isna()) |
            (df['close'].isna()) |
            (df['open'] <= 0) |
            (df['high'] <= 0) |
            (df['low'] <= 0) |
            (df['close'] <= 0) |
            (df['high'] < df['low']) |
            (df['open'] > df['high']) | 
            (df['open'] < df['low']) |
            (df['close'] > df['high']) | 
            (df['close'] < df['low'])
        )
        
        if invalid_mask.any():
            invalid_count = invalid_mask.sum()
            logger.warning(f"Removing {invalid_count} rows with invalid OHLCV data for {symbol}")
            df = df[~invalid_mask].copy()
        
        final_count = len(df)
        if final_count != initial_count:
            logger.info(f"Data validation for {symbol}: {initial_count} -> {final_count} rows")
        
        return df

    @classmethod
    def analyze_symbol_data(cls, symbol, context='all'):
        """Comprehensive analysis of stored symbol data"""
        try:
            db_path = DB_PATH_NEW_COINS if context == 'new' else DB_PATH_ALL_COINS
            table_name = NEW_COINS_TABLE if context == 'new' else ALL_COINS_TABLE
            
            conn = duckdb.connect(db_path)
            
            result = conn.execute(f"""
                SELECT 
                    COUNT(*) as total_candles,
                    MIN(open_time) as earliest_candle,
                    MAX(open_time) as latest_candle,
                    MIN(close) as lowest_price,
                    MAX(close) as highest_price,
                    AVG(close) as average_price,
                    SUM(volume) as total_volume
                FROM {table_name}
                WHERE symbol = ?
                GROUP BY symbol
            """, (symbol,)).fetchone()
            
            if not result:
                return {"symbol": symbol, "error": "No data found"}
            
            return {
                "symbol": symbol,
                "total_candles": result[0],
                "date_range": {
                    "start": result[1],
                    "end": result[2]
                },
                "price_range": {
                    "low": result[3],
                    "high": result[4],
                    "average": result[5]
                },
                "total_volume": result[6]
            }
        except Exception as e:
            logger.error(f"Error analyzing {symbol}: {e}")
            return {"symbol": symbol, "error": str(e)}