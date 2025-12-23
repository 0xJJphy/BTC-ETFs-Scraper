import os
import sys
import time
import argparse
from core.scrapers.scraper_cmc import process_cmc_flows
from core.utils.helpers import setup_driver
from core.multi_etf_scraper import run as run_multi_scraper
from core import data_builder

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
    
    args = parser.parse_args()
    
    # If no specific flags are provided, default to --all
    run_all = args.all or not (args.sites or args.cmc or args.build)
    
    start_time = time.time()
    print("="*60)
    print("      BTC ETF DATA PIPELINE - MODULAR EXECUTION")
    print("="*60)

    # Step 1: Individual ETF Scrapers (Grayscale, iShares, Fidelity, etc.)
    if run_all or args.sites:
        if not run_step("Individual Site Scrapers", run_multi_scraper, headless=args.headless):
            sys.exit(1)

    # Step 2: CoinMarketCap Flows Scraper
    if run_all or args.cmc:
        def run_cmc():
            driver = setup_driver(headless=args.headless)
            try:
                return process_cmc_flows(driver)
            finally:
                driver.quit()

        if not run_step("CoinMarketCap Flows Scraper", run_cmc):
            sys.exit(1)

    # Step 3: Data Builder (Aggregation and Processing)
    if run_all or args.build:
        if not run_step("Data Builder & Aggregator", data_builder.run):
            sys.exit(1)

    end_time = time.time()
    duration = end_time - start_time
    print("\n" + "="*60)
    print(f"PIPELINE COMPLETED SUCCESSFULLY in {duration:.2f} seconds.")
    print("="*60)

if __name__ == "__main__":
    main()
