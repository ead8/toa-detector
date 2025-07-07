
import os
import sys
import asyncio
import aiohttp
import pandas as pd
import logging
import ssl
import time
import json
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Set, Optional, Tuple
import traceback


sys.path.append(os.path.join(os.path.dirname(__file__), 'Binance Futures - Scripts'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'OKX Futures - Scripts'))

# Import database managers
from binance_duckdb_utils import BinanceDatabaseManager
from duckdb_utils import OKXDatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('master_backfill.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MasterBackfillManager:
    """Master backfill manager for both exchanges"""
    
    def __init__(self):
        self.binance_url = "https://fapi.binance.com"
        self.okx_url = "https://www.okx.com"
        self.interval = "1m"
        self.days = 7
        self.min_days = 7  # Minimum days of data required (for verification)
        self.concurrency = 2  # Conservative concurrency
        self.max_retries = 3
        self.base_delay = 0.5
        
        # Initialize databases
        self.initialize_databases()
        
        # Statistics tracking
        self.stats = {
            'binance': {'total_symbols': 0, 'processed': 0, 'backfilled': 0, 'errors': 0},
            'okx': {'total_symbols': 0, 'processed': 0, 'backfilled': 0, 'errors': 0}
        }
        
    def initialize_databases(self):
        """Initialize both exchange databases"""
        try:
            logger.info("Initializing databases...")
            
            # Initialize Binance database
            BinanceDatabaseManager.initialize_database('all')
            BinanceDatabaseManager.create_tao_detector_schema()
            
            # Initialize OKX database
            OKXDatabaseManager.initialize_database('all')
            OKXDatabaseManager.create_tao_detector_schema()
            
            logger.info("Databases initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing databases: {e}")
            raise
    
    async def get_binance_symbols(self, session) -> List[Dict]:
        """Get all Binance Futures symbols with their listing times"""
        try:
            logger.info("Fetching Binance Futures symbols...")
            
            async with session.get(f"{self.binance_url}/fapi/v1/exchangeInfo") as resp:
                if resp.status != 200:
                    logger.error(f"Failed to fetch Binance symbols: HTTP {resp.status}")
                    return []
                
                data = await resp.json()
                symbols = []
                
                for symbol_info in data.get('symbols', []):
                    if symbol_info.get('contractType') == 'PERPETUAL' and symbol_info.get('status') == 'TRADING':
                        symbols.append({
                            'symbol': symbol_info['symbol'],
                            'baseAsset': symbol_info['baseAsset'],
                            'quoteAsset': symbol_info['quoteAsset'],
                            'listTime': int(symbol_info.get('onboardDate', 0)),
                            'exchange': 'Binance Futures'
                        })
                
                logger.info(f"Found {len(symbols)} Binance Futures symbols")
                return symbols
                
        except Exception as e:
            logger.error(f"Error fetching Binance symbols: {e}")
            return []
    
    async def get_okx_symbols(self, session) -> List[Dict]:
        """Get all OKX Futures symbols with their listing times"""
        try:
            logger.info("Fetching OKX Futures symbols...")
            
            async with session.get(f"{self.okx_url}/api/v5/public/instruments", 
                                 params={"instType": "SWAP"}) as resp:
                if resp.status != 200:
                    logger.error(f"Failed to fetch OKX symbols: HTTP {resp.status}")
                    return []
                
                data = await resp.json()
                if data.get("code", "0") != "0":
                    logger.error(f"OKX API error: {data.get('msg')}")
                    return []
                
                symbols = []
                for symbol_info in data.get('data', []):
                    symbols.append({
                        'symbol': symbol_info['instId'],
                        'listTime': int(symbol_info['listTime']),
                        'exchange': 'OKX Futures'
                    })
                
                logger.info(f"Found {len(symbols)} OKX Futures symbols")
                return symbols
                
        except Exception as e:
            logger.error(f"Error fetching OKX symbols: {e}")
            return []
    
    def get_missing_symbols(self, exchange: str, all_symbols: List[Dict]) -> List[Dict]:
        """Get symbols that are missing from the database or have incomplete data"""
        try:
            if exchange == 'Binance Futures':
                # Get symbols with their stats from Binance database
                stats = BinanceDatabaseManager.get_symbol_stats()
                if stats is None:
                    logger.warning("No stats found in Binance database, will process all symbols")
                    return all_symbols
                
                # Create dict of symbols with their candle counts
                db_symbols = {}
                if stats is not None and not stats.empty:
                    for _, row in stats.iterrows():
                        db_symbols[row['symbol']] = row['candle_count']
            else:
                # Get symbols with their stats from OKX database
                stats = OKXDatabaseManager.get_symbol_stats()
                if stats is None:
                    logger.warning("No stats found in OKX database, will process all symbols")
                    return all_symbols
                
                # Create dict of symbols with their candle counts
                db_symbols = {}
                if stats is not None and not stats.empty:
                    for _, row in stats.iterrows():
                        db_symbols[row['symbol']] = row['candle_count']
            
            # Find missing symbols or those with insufficient data
            missing = []
            min_candles = self.min_days * 24 * 60  # 7 days * 24 hours * 60 minutes
            
            for symbol_info in all_symbols:
                symbol = symbol_info['symbol']
                # Add if symbol is missing or has less than min_candles (7 days)
                if symbol not in db_symbols or db_symbols[symbol] < min_candles:
                    missing.append(symbol_info)
            
            logger.info(f"Found {len(missing)} missing/incomplete symbols for {exchange}")
            return missing
            
        except Exception as e:
            logger.error(f"Error getting missing symbols for {exchange}: {e}")
            logger.error(f"Exception details: {str(e)}")
            return all_symbols  # Return all symbols if we can't determine missing ones
    
    async def fetch_binance_first_candle(self, session, symbol: str, start_time: int) -> Optional[int]:
        """Fetch the first candle time for a Binance symbol"""
        try:
            await asyncio.sleep(self.base_delay)
            
            params = {
                "symbol": symbol,
                "interval": self.interval,
                "limit": 1,
                "startTime": start_time
            }
            
            async with session.get(f"{self.binance_url}/fapi/v1/klines", params=params) as resp:
                if resp.status == 429:
                    logger.warning(f"Rate limited for {symbol}, waiting...")
                    await asyncio.sleep(60)
                    return None
                
                if resp.status != 200:
                    logger.warning(f"HTTP {resp.status} for {symbol}")
                    return None
                
                data = await resp.json()
                if data and isinstance(data, list):
                    return int(data[0][0])  # Return open_time
                    
        except Exception as e:
            logger.error(f"Error fetching first candle for {symbol}: {e}")
        
        return None
    
    async def fetch_okx_first_candle(self, session, symbol: str, start_time: int) -> Optional[int]:
        """Fetch the first candle time for an OKX symbol"""
        try:
            await asyncio.sleep(self.base_delay)
            
            params = {
                "instId": symbol,
                "bar": self.interval,
                "limit": 1,
                "after": start_time
            }
            
            async with session.get(f"{self.okx_url}/api/v5/market/history-candles", params=params) as resp:
                if resp.status == 429:
                    logger.warning(f"Rate limited for {symbol}, waiting...")
                    await asyncio.sleep(30)
                    return None
                
                if resp.status != 200:
                    logger.warning(f"HTTP {resp.status} for {symbol}")
                    return None
                
                data = await resp.json()
                if data.get("code", "0") == "0" and data.get("data"):
                    return int(data["data"][0][0])  # Return open_time
                    
        except Exception as e:
            logger.error(f"Error fetching first candle for {symbol}: {e}")
        
        return None
    
    async def fetch_binance_7_days(self, session, symbol: str, start_time: int) -> List:
        """Fetch 7 days of data for Binance symbol"""
        try:
            end_time = start_time + (self.days * 24 * 60 * 60 * 1000)
            all_klines = []
            current_time = start_time
            
            while current_time < end_time:
                await asyncio.sleep(self.base_delay)
                
                params = {
                    "symbol": symbol,
                    "interval": self.interval,
                    "limit": 1500,
                    "startTime": current_time,
                    "endTime": min(current_time + (1500 * 60 * 1000), end_time)
                }
                
                async with session.get(f"{self.binance_url}/fapi/v1/klines", params=params) as resp:
                    if resp.status == 429:
                        logger.warning(f"Rate limited for {symbol}, waiting...")
                        await asyncio.sleep(60)
                        continue
                    
                    if resp.status != 200:
                        logger.warning(f"HTTP {resp.status} for {symbol}")
                        break
                    
                    klines = await resp.json()
                    if not klines:
                        break
                    
                    all_klines.extend(klines)
                    current_time = int(klines[-1][0]) + 60000  # Next minute
                    
                    if len(klines) < 1500:
                        break
            
            return all_klines
            
        except Exception as e:
            logger.error(f"Error fetching 7 days data for {symbol}: {e}")
            return []
    
    async def fetch_okx_7_days(self, session, symbol: str, start_time: int) -> List:
        """Fetch 7 days of data for OKX symbol"""
        try:
            end_time = start_time + (self.days * 24 * 60 * 60 * 1000)
            all_klines = []
            current_time = end_time  # OKX uses reverse chronological order
            
            while current_time > start_time:
                await asyncio.sleep(self.base_delay)
                
                params = {
                    "instId": symbol,
                    "bar": self.interval,
                    "limit": 100,
                    "before": current_time
                }
                
                async with session.get(f"{self.okx_url}/api/v5/market/history-candles", params=params) as resp:
                    if resp.status == 429:
                        logger.warning(f"Rate limited for {symbol}, waiting...")
                        await asyncio.sleep(30)
                        continue
                    
                    if resp.status != 200:
                        logger.warning(f"HTTP {resp.status} for {symbol}")
                        break
                    
                    data = await resp.json()
                    if data.get("code", "0") != "0" or not data.get("data"):
                        break
                    
                    klines = data["data"]
                    all_klines.extend(klines)
                    current_time = int(klines[-1][0])  # Oldest timestamp
                    
                    if len(klines) < 100:
                        break
            
            # Reverse to get chronological order and filter to 7 days
            all_klines.reverse()
            filtered_klines = [k for k in all_klines if int(k[0]) >= start_time]
            
            return filtered_klines
            
        except Exception as e:
            logger.error(f"Error fetching 7 days data for {symbol}: {e}")
            return []
    
    async def process_binance_symbol(self, session, symbol_info: Dict) -> Dict:
        """Process a single Binance symbol"""
        symbol = symbol_info['symbol']
        list_time = symbol_info['listTime']
        
        try:
            logger.info(f"Processing Binance symbol: {symbol}")
            
            # Get first candle time
            if list_time <= 0:
                list_time = int(datetime(2019, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
            
            first_candle_time = await self.fetch_binance_first_candle(session, symbol, list_time)
            if not first_candle_time:
                logger.warning(f"No first candle found for {symbol}")
                return {"symbol": symbol, "status": "no_data"}
            
            # Fetch 7 days of data
            klines = await self.fetch_binance_7_days(session, symbol, first_candle_time)
            if not klines:
                logger.warning(f"No klines data for {symbol}")
                return {"symbol": symbol, "status": "no_klines"}
            
            # Store in database
            result = BinanceDatabaseManager.insert_candles(
                klines, symbol, first_candle_time, context='all', exchange='Binance Futures'
            )
            
            logger.info(f"Processed {symbol}: {result.get('inserted_rows', 0)} candles")
            return {"symbol": symbol, "status": "success", "candles": result.get('inserted_rows', 0)}
            
        except Exception as e:
            logger.error(f"Error processing Binance symbol {symbol}: {e}")
            return {"symbol": symbol, "status": "error", "error": str(e)}
    
    async def process_okx_symbol(self, session, symbol_info: Dict) -> Dict:
        """Process a single OKX symbol"""
        symbol = symbol_info['symbol']
        list_time = symbol_info['listTime']
        
        try:
            logger.info(f"Processing OKX symbol: {symbol}")
            
            # Get first candle time
            if list_time <= 0:
                list_time = int(datetime(2014, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
            
            first_candle_time = await self.fetch_okx_first_candle(session, symbol, list_time)
            if not first_candle_time:
                logger.warning(f"No first candle found for {symbol}")
                return {"symbol": symbol, "status": "no_data"}
            
            # Fetch 7 days of data
            klines = await self.fetch_okx_7_days(session, symbol, first_candle_time)
            if not klines:
                logger.warning(f"No klines data for {symbol}")
                return {"symbol": symbol, "status": "no_klines"}
            
            # Store in database
            result = OKXDatabaseManager.insert_candles(
                klines, symbol, first_candle_time, context='all', exchange='OKX Futures'
            )
            
            logger.info(f"Processed {symbol}: {result.get('inserted_rows', 0)} candles")
            return {"symbol": symbol, "status": "success", "candles": result.get('inserted_rows', 0)}
            
        except Exception as e:
            logger.error(f"Error processing OKX symbol {symbol}: {e}")
            return {"symbol": symbol, "status": "error", "error": str(e)}
    
    async def backfill_exchange(self, exchange: str, symbols: List[Dict]):
        """Backfill data for a specific exchange"""
        logger.info(f"Starting backfill for {exchange} ({len(symbols)} symbols)")
        
        # Create SSL context
        ssl_context = ssl.create_default_context()
        connector = aiohttp.TCPConnector(ssl=ssl_context, limit=10)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            semaphore = asyncio.Semaphore(self.concurrency)
            
            async def process_with_semaphore(symbol_info):
                async with semaphore:
                    if exchange == 'Binance Futures':
                        return await self.process_binance_symbol(session, symbol_info)
                    else:
                        return await self.process_okx_symbol(session, symbol_info)
            
            # Process symbols in batches
            batch_size = 10
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i+batch_size]
                
                logger.info(f"Processing batch {i//batch_size + 1}/{(len(symbols) + batch_size - 1)//batch_size}")
                
                # Process batch
                tasks = [process_with_semaphore(symbol_info) for symbol_info in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Update statistics
                for result in results:
                    if isinstance(result, Exception):
                        self.stats[exchange.lower().replace(' ', '_')]['errors'] += 1
                        logger.error(f"Exception in processing: {result}")
                    else:
                        self.stats[exchange.lower().replace(' ', '_')]['processed'] += 1
                        if result.get('status') == 'success':
                            self.stats[exchange.lower().replace(' ', '_')]['backfilled'] += 1
                
                # Wait between batches
                await asyncio.sleep(2)
        
        logger.info(f"Completed backfill for {exchange}")
    
    def generate_report(self):
        """Generate a comprehensive backfill report"""
        report = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'summary': {
                'total_processed': sum(self.stats[ex]['processed'] for ex in self.stats),
                'total_backfilled': sum(self.stats[ex]['backfilled'] for ex in self.stats),
                'total_errors': sum(self.stats[ex]['errors'] for ex in self.stats)
            },
            'by_exchange': self.stats
        }
        
        # Save report to file
        with open('backfill_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info("Backfill Report:")
        logger.info(f"Total Processed: {report['summary']['total_processed']}")
        logger.info(f"Total Backfilled: {report['summary']['total_backfilled']}")
        logger.info(f"Total Errors: {report['summary']['total_errors']}")
        
        for exchange, stats in self.stats.items():
            logger.info(f"{exchange}: {stats['processed']} processed, {stats['backfilled']} backfilled, {stats['errors']} errors")
        
        return report
    
    async def run_full_backfill(self):
        """Run complete backfill for both exchanges"""
        logger.info("Starting master backfill process...")
        
        # Create SSL context
        ssl_context = ssl.create_default_context()
        connector = aiohttp.TCPConnector(ssl=ssl_context, limit=10)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            # Get all symbols from both exchanges
            binance_symbols = await self.get_binance_symbols(session)
            okx_symbols = await self.get_okx_symbols(session)
            
            # Update statistics
            self.stats['binance']['total_symbols'] = len(binance_symbols)
            self.stats['okx']['total_symbols'] = len(okx_symbols)
            
            # Get missing symbols or those with incomplete data
            # This checks the database for existing data and only processes what's needed
            missing_binance = self.get_missing_symbols('Binance Futures', binance_symbols)
            missing_okx = self.get_missing_symbols('OKX Futures', okx_symbols)
            
            logger.info(f"Will process {len(missing_binance)} Binance symbols and {len(missing_okx)} OKX symbols")
        
        # Run backfill for both exchanges
        await self.backfill_exchange('Binance Futures', missing_binance)
        await self.backfill_exchange('OKX Futures', missing_okx)
        
        # Generate final report
        report = self.generate_report()
        
        logger.info("Master backfill completed!")
        return report

async def main():
    """Main function to run the backfill"""
    try:
        manager = MasterBackfillManager()
        report = await manager.run_full_backfill()
        
        print("\n" + "="*50)
        print("BACKFILL COMPLETED")
        print("="*50)
        print(f"Total Processed: {report['summary']['total_processed']}")
        print(f"Total Backfilled: {report['summary']['total_backfilled']}")
        print(f"Total Errors: {report['summary']['total_errors']}")
        print("="*50)
        
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    asyncio.run(main())
