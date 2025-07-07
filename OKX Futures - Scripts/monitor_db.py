import time
from duckdb_utils import OKXDatabaseManager
from telegram_utils import send_telegram

def monitor_database():
    while True:
        try:
            # Check all coins database
            all_status = OKXDatabaseManager.get_database_status(context='all')
            new_status = OKXDatabaseManager.get_database_status(context='new')
            
            message = (
                "📊 Database Status Update:\n"
                f"All Coins: {all_status['row_count']} rows\n"
                f"New Coins: {new_status['row_count']} rows"
            )
            send_telegram(message)
            
            # Check every 6 hours
            time.sleep(6 * 3600)
            
        except Exception as e:
            send_telegram(f"⚠️ Database monitoring error: {str(e)}")
            time.sleep(3600)  # Wait 1 hour before retry

if __name__ == "__main__":
    monitor_database()