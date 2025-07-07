#!/usr/bin/env python3
"""
Backfill Verification Script for TAO-DETECTOR
Checks if all coins from both exchanges have their first 7 days of data in the main databases
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Set

# Add script directories to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'Binance Futures - Scripts'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'OKX Futures - Scripts'))

try:
    from binance_duckdb_utils import BinanceDatabaseManager
    from duckdb_utils import OKXDatabaseManager
    import duckdb
    import pandas as pd
except ImportError as e:
    print(f"Import error: {e}")
    print("Please ensure all required packages are installed")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('backfill_verification.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BackfillVerifier:
    """Verifies backfill completeness for both exchanges"""
    
    def __init__(self):
        self.binance_db_path = "Binance Futures - Scripts/binance_futures_master.db"
        self.okx_db_path = "OKX Futures - Scripts/okx_futures_master.db"
        
        # Expected minimum days of data
        self.min_days = 7
        self.min_candles = self.min_days * 24 * 60  # 7 days * 24 hours * 60 minutes
        
    def check_database_exists(self, db_path: str, exchange: str) -> bool:
        """Check if database file exists"""
        if not os.path.exists(db_path):
            logger.error(f"{exchange} database not found at {db_path}")
            return False
        logger.info(f"{exchange} database found at {db_path}")
        return True
    
    def get_binance_stats(self) -> Dict:
        """Get comprehensive statistics for Binance database"""
        try:
            if not self.check_database_exists(self.binance_db_path, "Binance"):
                return {"error": "Database not found"}
            
            conn = duckdb.connect(self.binance_db_path)
            
            # Get basic stats
            total_symbols = conn.execute("SELECT COUNT(DISTINCT symbol) FROM futures_candles").fetchone()[0]
            total_candles = conn.execute("SELECT COUNT(*) FROM futures_candles").fetchone()[0]
            
            # Get symbols with their candle counts
            symbol_stats = conn.execute("""
                SELECT 
                    symbol,
                    COUNT(*) as candle_count,
                    MIN(open_time) as first_candle,
                    MAX(open_time) as last_candle,
                    EXTRACT(epoch FROM (MAX(open_time) - MIN(open_time))) / 86400 as days_covered
                FROM futures_candles 
                GROUP BY symbol 
                ORDER BY candle_count DESC
            """).fetchall()
            
            # Analyze completeness
            symbols_with_min_data = 0
            symbols_with_7_days = 0
            
            for symbol, count, first, last, days in symbol_stats:
                if count >= 1440:  # At least 1 day of data
                    symbols_with_min_data += 1
                if days >= 6.9:  # Close to 7 days (allowing for small gaps)
                    symbols_with_7_days += 1
            
            conn.close()
            
            return {
                "total_symbols": total_symbols,
                "total_candles": total_candles,
                "symbols_with_min_data": symbols_with_min_data,
                "symbols_with_7_days": symbols_with_7_days,
                "completeness_percentage": (symbols_with_7_days / total_symbols * 100) if total_symbols > 0 else 0,
                "symbol_details": symbol_stats[:20]  # Top 20 symbols
            }
            
        except Exception as e:
            logger.error(f"Error getting Binance stats: {e}")
            return {"error": str(e)}
    
    def get_okx_stats(self) -> Dict:
        """Get comprehensive statistics for OKX database"""
        try:
            if not self.check_database_exists(self.okx_db_path, "OKX"):
                return {"error": "Database not found"}
            
            conn = duckdb.connect(self.okx_db_path)
            
            # Get basic stats
            total_symbols = conn.execute("SELECT COUNT(DISTINCT symbol) FROM futures_candles").fetchone()[0]
            total_candles = conn.execute("SELECT COUNT(*) FROM futures_candles").fetchone()[0]
            
            # Get symbols with their candle counts
            symbol_stats = conn.execute("""
                SELECT 
                    symbol,
                    COUNT(*) as candle_count,
                    MIN(open_time) as first_candle,
                    MAX(open_time) as last_candle,
                    EXTRACT(epoch FROM (MAX(open_time) - MIN(open_time))) / 86400 as days_covered
                FROM futures_candles 
                GROUP BY symbol 
                ORDER BY candle_count DESC
            """).fetchall()
            
            # Analyze completeness
            symbols_with_min_data = 0
            symbols_with_7_days = 0
            
            for symbol, count, first, last, days in symbol_stats:
                if count >= 1440:  # At least 1 day of data
                    symbols_with_min_data += 1
                if days >= 6.9:  # Close to 7 days
                    symbols_with_7_days += 1
            
            conn.close()
            
            return {
                "total_symbols": total_symbols,
                "total_candles": total_candles,
                "symbols_with_min_data": symbols_with_min_data,
                "symbols_with_7_days": symbols_with_7_days,
                "completeness_percentage": (symbols_with_7_days / total_symbols * 100) if total_symbols > 0 else 0,
                "symbol_details": symbol_stats[:20]  # Top 20 symbols
            }
            
        except Exception as e:
            logger.error(f"Error getting OKX stats: {e}")
            return {"error": str(e)}
    
    def get_missing_symbols(self, exchange: str) -> List[str]:
        """Get symbols that have insufficient data"""
        try:
            if exchange.lower() == "binance":
                db_path = self.binance_db_path
            else:
                db_path = self.okx_db_path
            
            if not os.path.exists(db_path):
                return []
            
            conn = duckdb.connect(db_path)
            
            # Get symbols with less than 7 days of data
            missing_symbols = conn.execute("""
                SELECT symbol, COUNT(*) as candle_count
                FROM futures_candles 
                GROUP BY symbol 
                HAVING COUNT(*) < 10080  -- 7 days * 24 hours * 60 minutes
                ORDER BY candle_count ASC
            """).fetchall()
            
            conn.close()
            
            return [(symbol, count) for symbol, count in missing_symbols]
            
        except Exception as e:
            logger.error(f"Error getting missing symbols for {exchange}: {e}")
            return []
    
    def check_data_quality(self, exchange: str) -> Dict:
        """Check data quality for an exchange"""
        try:
            if exchange.lower() == "binance":
                db_path = self.binance_db_path
            else:
                db_path = self.okx_db_path
            
            if not os.path.exists(db_path):
                return {"error": "Database not found"}
            
            conn = duckdb.connect(db_path)
            
            # Check for data quality issues
            quality_checks = {
                "null_prices": conn.execute("SELECT COUNT(*) FROM futures_candles WHERE open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL").fetchone()[0],
                "zero_prices": conn.execute("SELECT COUNT(*) FROM futures_candles WHERE open = 0 OR high = 0 OR low = 0 OR close = 0").fetchone()[0],
                "invalid_ohlc": conn.execute("SELECT COUNT(*) FROM futures_candles WHERE high < low OR open > high OR open < low OR close > high OR close < low").fetchone()[0],
                "negative_volume": conn.execute("SELECT COUNT(*) FROM futures_candles WHERE volume < 0").fetchone()[0],
                "duplicate_timestamps": conn.execute("SELECT COUNT(*) - COUNT(DISTINCT symbol, open_time) FROM futures_candles").fetchone()[0]
            }
            
            conn.close()
            
            return quality_checks
            
        except Exception as e:
            logger.error(f"Error checking data quality for {exchange}: {e}")
            return {"error": str(e)}
    
    def generate_comprehensive_report(self) -> Dict:
        """Generate a comprehensive backfill verification report"""
        logger.info("Generating comprehensive backfill verification report...")
        
        report = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "verification_summary": {},
            "binance": {},
            "okx": {},
            "overall_status": "",
            "recommendations": []
        }
        
        # Get statistics for both exchanges
        logger.info("Checking Binance statistics...")
        binance_stats = self.get_binance_stats()
        report["binance"] = binance_stats
        
        logger.info("Checking OKX statistics...")
        okx_stats = self.get_okx_stats()
        report["okx"] = okx_stats
        
        # Get missing symbols
        logger.info("Checking for missing Binance symbols...")
        binance_missing = self.get_missing_symbols("binance")
        report["binance"]["missing_symbols"] = binance_missing[:50]  # Top 50 missing
        
        logger.info("Checking for missing OKX symbols...")
        okx_missing = self.get_missing_symbols("okx")
        report["okx"]["missing_symbols"] = okx_missing[:50]  # Top 50 missing
        
        # Check data quality
        logger.info("Checking Binance data quality...")
        binance_quality = self.check_data_quality("binance")
        report["binance"]["data_quality"] = binance_quality
        
        logger.info("Checking OKX data quality...")
        okx_quality = self.check_data_quality("okx")
        report["okx"]["data_quality"] = okx_quality
        
        # Calculate overall statistics
        total_symbols = 0
        total_complete = 0
        
        if "total_symbols" in binance_stats:
            total_symbols += binance_stats["total_symbols"]
            total_complete += binance_stats.get("symbols_with_7_days", 0)
        
        if "total_symbols" in okx_stats:
            total_symbols += okx_stats["total_symbols"]
            total_complete += okx_stats.get("symbols_with_7_days", 0)
        
        overall_completeness = (total_complete / total_symbols * 100) if total_symbols > 0 else 0
        
        report["verification_summary"] = {
            "total_symbols_across_exchanges": total_symbols,
            "total_symbols_with_7_days": total_complete,
            "overall_completeness_percentage": overall_completeness,
            "binance_missing_count": len(binance_missing),
            "okx_missing_count": len(okx_missing)
        }
        
        # Determine overall status
        if overall_completeness >= 95:
            report["overall_status"] = "EXCELLENT"
        elif overall_completeness >= 85:
            report["overall_status"] = "GOOD"
        elif overall_completeness >= 70:
            report["overall_status"] = "FAIR"
        else:
            report["overall_status"] = "NEEDS_WORK"
        
        # Generate recommendations
        recommendations = []
        
        if len(binance_missing) > 0:
            recommendations.append(f"Run backfill for {len(binance_missing)} missing Binance symbols")
        
        if len(okx_missing) > 0:
            recommendations.append(f"Run backfill for {len(okx_missing)} missing OKX symbols")
        
        if overall_completeness < 90:
            recommendations.append("Consider running full backfill to ensure complete coverage")
        
        # Check data quality issues
        for exchange, quality in [("Binance", binance_quality), ("OKX", okx_quality)]:
            if isinstance(quality, dict) and "error" not in quality:
                if quality.get("null_prices", 0) > 0:
                    recommendations.append(f"Fix {quality['null_prices']} null price entries in {exchange}")
                if quality.get("invalid_ohlc", 0) > 0:
                    recommendations.append(f"Fix {quality['invalid_ohlc']} invalid OHLC entries in {exchange}")
        
        if not recommendations:
            recommendations.append("All systems appear to be functioning correctly")
        
        report["recommendations"] = recommendations
        
        return report
    
    def save_report(self, report: Dict, filename: str = "backfill_verification_report.json"):
        """Save report to JSON file"""
        try:
            with open(filename, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            logger.info(f"Report saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving report: {e}")
    
    def print_summary(self, report: Dict):
        """Print a human-readable summary of the report"""
        print("\n" + "="*60)
        print("TAO-DETECTOR BACKFILL VERIFICATION REPORT")
        print("="*60)
        
        summary = report.get("verification_summary", {})
        
        print(f"Overall Status: {report.get('overall_status', 'UNKNOWN')}")
        print(f"Total Symbols: {summary.get('total_symbols_across_exchanges', 0)}")
        print(f"Symbols with 7+ Days: {summary.get('total_symbols_with_7_days', 0)}")
        print(f"Overall Completeness: {summary.get('overall_completeness_percentage', 0):.1f}%")
        
        print("\n" + "-"*40)
        print("EXCHANGE BREAKDOWN")
        print("-"*40)
        
        # Binance summary
        binance = report.get("binance", {})
        if "error" not in binance:
            print(f"Binance Futures:")
            print(f"  - Total Symbols: {binance.get('total_symbols', 0)}")
            print(f"  - Complete (7+ days): {binance.get('symbols_with_7_days', 0)}")
            print(f"  - Completeness: {binance.get('completeness_percentage', 0):.1f}%")
            
            # Show missing symbols details
            missing = binance.get('missing_symbols', [])
            print(f"  - Missing: {len(missing)}")
            if missing and len(missing) > 0:
                print("\n  Top missing Binance symbols:")
                for i, (symbol, count) in enumerate(missing[:5], 1):
                    days = count / (24 * 60) if count > 0 else 0
                    print(f"    {i}. {symbol}: {count} candles ({days:.1f} days)")
        else:
            print(f"Binance Futures: ERROR - {binance['error']}")
        
        # OKX summary
        okx = report.get("okx", {})
        if "error" not in okx:
            print(f"OKX Futures:")
            print(f"  - Total Symbols: {okx.get('total_symbols', 0)}")
            print(f"  - Complete (7+ days): {okx.get('symbols_with_7_days', 0)}")
            print(f"  - Completeness: {okx.get('completeness_percentage', 0):.1f}%")
            
            # Show missing symbols details
            missing = okx.get('missing_symbols', [])
            print(f"  - Missing: {len(missing)}")
            if missing and len(missing) > 0:
                print("\n  Top missing OKX symbols:")
                for i, (symbol, count) in enumerate(missing[:5], 1):
                    days = count / (24 * 60) if count > 0 else 0
                    print(f"    {i}. {symbol}: {count} candles ({days:.1f} days)")
        else:
            print(f"OKX Futures: ERROR - {okx['error']}")
        
        print("\n" + "-"*40)
        print("RECOMMENDATIONS")
        print("-"*40)
        
        for i, rec in enumerate(report.get("recommendations", []), 1):
            print(f"{i}. {rec}")
        
        print("\n" + "="*60)

def main():
    """Main function to run verification"""
    try:
        verifier = BackfillVerifier()
        
        print("Starting backfill verification...")
        report = verifier.generate_comprehensive_report()
        
        # Save detailed report
        verifier.save_report(report)
        
        # Print summary
        verifier.print_summary(report)
        
        # Return appropriate exit code
        overall_status = report.get("overall_status", "UNKNOWN")
        if overall_status in ["EXCELLENT", "GOOD"]:
            return 0
        elif overall_status == "FAIR":
            return 1
        else:
            return 2
            
    except Exception as e:
        logger.error(f"Fatal error in verification: {e}")
        return 3

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
