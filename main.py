import os
import sys
import time
import argparse

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

from core.scrapers.scraper_cmc import process_cmc_flows
from core.utils.helpers import setup_driver
from core.multi_etf_scraper import run as run_multi_scraper
from core import data_builder
from core.db_adapter import (
    init_database, close_database, start_session, end_session, is_db_enabled
)

def run_step(name, func, *args, **kwargs):
    """Execute a pipeline step and handle errors."""
    print("\n" + "#"*60)
    print(f" STEP: {name}")
    print("#"*60)
    try:
        result = func(*args, **kwargs)
        if result is False:
            print(f"\n[PIPELINE] STEP FAILED: {name}")
            return False
        if isinstance(result, tuple) and result[0] is False:
            print(f"\n[PIPELINE] STEP FAILED: {name} - Error: {result[1]}")
            return False
        print(f"\n[PIPELINE] STEP SUCCESS: {name}")
        return True
    except Exception as e:
        print(f"\n[PIPELINE] STEP EXCEPTION: {name} - {e}")
        return False

def main():
    """Main entry point for the BTC ETF Scraper pipeline."""
    parser = argparse.ArgumentParser(description="BTC ETF Data Pipeline")
    parser.add_argument("--all", action="store_true", help="Run all pipeline phases (default)")
    parser.add_argument("--sites", action="store_true", help="Run individual ETF site scrapers")
    parser.add_argument("--cmc", action="store_true", help="Run CoinMarketCap flows scraper")
    parser.add_argument("--build", action="store_true", help="Run Data Builder & Aggregator")
    parser.add_argument("--headless", action="store_true", default=True, help="Run browser in headless mode (default: True)")
    parser.add_argument("--no-headless", action="store_false", dest="headless", help="Run browser with visible window")
    parser.add_argument("--save-files", action="store_true", default=False, 
                        help="Save CSV/JSON files in addition to database (default: False when DB enabled)")
    
    args = parser.parse_args()
    
    # If no specific flags are provided, default to --all
    run_all = args.all or not (args.sites or args.cmc or args.build)
    
    # Initialize database connection (optional - continues if not configured)
    db_enabled = init_database()
    if db_enabled:
        print("\n[DB] ✅ Database enabled - data will be saved to PostgreSQL")
        if args.save_files:
            print("[DB] 📁 --save-files enabled - CSV/JSON backup will also be created")
        else:
            print("[DB] 📁 CSV/JSON files disabled (use --save-files to enable)")
        start_session()
    else:
        print("\n[DB] ⚠️  Database not configured - saving to CSV/JSON only")
        # Force file saving when DB is not available
        args.save_files = True
    
    # Set environment variable for helpers.py to check
    os.environ["ETF_SAVE_FILES"] = "1" if args.save_files else "0"
    
    start_time = time.time()
    print("="*60)
    print("      BTC ETF DATA PIPELINE - MODULAR EXECUTION")
    print("="*60)
    
    etfs_processed = 0
    etfs_failed = 0
    pipeline_success = True

    # Step 1: Individual ETF Scrapers (Grayscale, iShares, Fidelity, etc.)
    if run_all or args.sites:
        if not run_step("Individual Site Scrapers", run_multi_scraper, headless=args.headless):
            etfs_failed += 1
            pipeline_success = False
        else:
            etfs_processed += 1

    # Step 2: CoinMarketCap Flows Scraper
    if run_all or args.cmc:
        def run_cmc():
            driver = setup_driver(headless=args.headless)
            try:
                return process_cmc_flows(driver)
            finally:
                driver.quit()

        if not run_step("CoinMarketCap Flows Scraper", run_cmc):
            etfs_failed += 1
            pipeline_success = False
        else:
            etfs_processed += 1

    # Step 3: Data Builder (Aggregation and Processing)
    if run_all or args.build:
        if not run_step("Data Builder & Aggregator", data_builder.run):
            etfs_failed += 1
            pipeline_success = False
        else:
            etfs_processed += 1

    # Finalize database session
    if db_enabled:
        end_session(
            success=pipeline_success,
            processed=etfs_processed,
            failed=etfs_failed,
            error=None if pipeline_success else "One or more pipeline steps failed"
        )
        close_database()

    end_time = time.time()
    duration = end_time - start_time
    
    if pipeline_success:
        print("\n" + "="*60)
        print(f"PIPELINE COMPLETED SUCCESSFULLY in {duration:.2f} seconds.")
        print("="*60)
    else:
        print("\n" + "="*60)
        print(f"PIPELINE COMPLETED WITH ERRORS in {duration:.2f} seconds.")
        print("="*60)
        sys.exit(1)

if __name__ == "__main__":
    main()

