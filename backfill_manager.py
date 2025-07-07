import os
import sys
import subprocess
import argparse
import json
from datetime import datetime

def run_command(cmd, cwd=None):
    """Run a command and return the result"""
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            capture_output=True, 
            text=True, 
            cwd=cwd
        )
        return result.returncode, result.stdout, result.stderr
    except Exception as e:
        return 1, "", str(e)

def check_dependencies():
    """Check if required dependencies are installed"""
    print("Checking dependencies...")
    
    # Map package names for import vs pip install
    package_mapping = {
        'duckdb': 'duckdb',
        'pandas': 'pandas', 
        'aiohttp': 'aiohttp',
        'requests': 'requests',
        'pyyaml': 'yaml'  # pyyaml installs as 'yaml'
    }
    
    missing = []
    
    for pip_name, import_name in package_mapping.items():
        try:
            # Try to import directly in the current Python process
            __import__(import_name)
            print(f"✅ {pip_name}")
        except ImportError:
            print(f"❌ {pip_name}")
            missing.append(pip_name)
    
    if missing:
        print(f"❌ Missing packages: {', '.join(missing)}")
        print("Install with: pip install " + " ".join(missing))
        return False
    
    print("✅ All dependencies are installed")
    return True

def verify_current_status():
    """Verify the current backfill status"""
    print("\n📊 Checking current backfill status...")
    
    returncode, stdout, stderr = run_command("python verify_backfill.py")
    
    if returncode == 0:
        print("✅ Backfill verification completed - Status: EXCELLENT/GOOD")
    elif returncode == 1:
        print("⚠️  Backfill verification completed - Status: FAIR")
    elif returncode == 2:
        print("❌ Backfill verification completed - Status: NEEDS_WORK")
    else:
        print("🚨 Backfill verification failed")
        print(f"Error: {stderr}")
    
    return returncode

def run_full_backfill():
    """Run the complete backfill process"""
    print("\n🚀 Starting full backfill process...")
    print("This may take several hours depending on the number of symbols.")
    
    # Run the master backfill script
    returncode, stdout, stderr = run_command("python master_backfill.py")
    
    if returncode == 0:
        print("✅ Backfill completed successfully")
        
        # Try to load and display the report
        try:
            with open('backfill_report.json', 'r') as f:
                report = json.load(f)
            
            print("\n📈 Backfill Summary:")
            summary = report.get('summary', {})
            print(f"Total Processed: {summary.get('total_processed', 0)}")
            print(f"Total Backfilled: {summary.get('total_backfilled', 0)}")
            print(f"Total Errors: {summary.get('total_errors', 0)}")
            
        except Exception as e:
            print(f"Could not load backfill report: {e}")
    else:
        print("❌ Backfill failed")
        print(f"Error: {stderr}")
    
    return returncode

def run_binance_only():
    """Run backfill for Binance only"""
    print("\n🔸 Starting Binance-only backfill...")
    
    script_path = os.path.join("Binance Futures - Scripts", "binance_7days_downloader.py")
    returncode, stdout, stderr = run_command(f"python \"{script_path}\"")
    
    if returncode == 0:
        print("✅ Binance backfill completed")
    else:
        print("❌ Binance backfill failed")
        print(f"Error: {stderr}")
    
    return returncode

def run_okx_only():
    """Run backfill for OKX only"""
    print("\n🔸 Starting OKX-only backfill...")
    
    script_path = os.path.join("OKX Futures - Scripts", "okx_7days_downloader.py")
    returncode, stdout, stderr = run_command(f"python \"{script_path}\"")
    
    if returncode == 0:
        print("✅ OKX backfill completed")
    else:
        print("❌ OKX backfill failed")
        print(f"Error: {stderr}")
    
    return returncode

def show_status():
    """Show detailed status of databases"""
    print("\n📋 Detailed Database Status:")
    
    # Check if database files exist
    binance_db = os.path.join("Binance Futures - Scripts", "binance_futures_master.db")
    okx_db = os.path.join("OKX Futures - Scripts", "okx_futures_master.db")
    
    print(f"Binance DB: {'✅ Exists' if os.path.exists(binance_db) else '❌ Missing'}")
    print(f"OKX DB: {'✅ Exists' if os.path.exists(okx_db) else '❌ Missing'}")
    
    # Check CSV directories
    binance_csv = "Binance Futures - CSV"
    okx_csv = "OKX - CSV"
    
    if os.path.exists(binance_csv):
        binance_files = len([f for f in os.listdir(binance_csv) if f.endswith('.csv')])
        print(f"Binance CSV files: {binance_files}")
    else:
        print("Binance CSV: ❌ Directory missing")
    
    if os.path.exists(okx_csv):
        okx_files = len([f for f in os.listdir(okx_csv) if f.endswith('.csv')])
        print(f"OKX CSV files: {okx_files}")
    else:
        print("OKX CSV: ❌ Directory missing")

def main():
    parser = argparse.ArgumentParser(description="TAO-DETECTOR Backfill Management")
    parser.add_argument('action', choices=[
        'check', 'verify', 'backfill', 'binance', 'okx', 'status'
    ], help='Action to perform')
    
    args = parser.parse_args()
    
    print("🎯 TAO-DETECTOR Backfill Manager")
    print("="*50)
    
    # Check dependencies first for most actions
    if args.action in ['verify', 'backfill', 'binance', 'okx']:
        if not check_dependencies():
            return 1
    
    if args.action == 'check':
        return 0 if check_dependencies() else 1
        
    elif args.action == 'verify':
        return verify_current_status()
        
    elif args.action == 'backfill':
        returncode = run_full_backfill()
        if returncode == 0:
            print("\n📊 Running verification after backfill...")
            verify_current_status()
        return returncode
        
    elif args.action == 'binance':
        return run_binance_only()
        
    elif args.action == 'okx':
        return run_okx_only()
        
    elif args.action == 'status':
        show_status()
        return 0
    
    return 0

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠️  Operation cancelled by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n🚨 Fatal error: {e}")
        sys.exit(1)
