import os
import sys
import time
from core.scrapers.scraper_cmc import process_cmc_flows
from core.utils.helpers import setup_driver
from core import multi_etf_scraper
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
    start_time = time.time()
    print("="*60)
    print("      BTC ETF DATA PIPELINE - SEQUENTIAL EXECUTION")
    print("="*60)

    # Step 1: Individual ETF Scrapers (Grayscale, iShares, Fidelity, etc.)
    if not run_step("Individual Site Scrapers", multi_etf_scraper.main):
        sys.exit(1)

    # Step 2: CoinMarketCap Flows Scraper
    output_cmc = os.path.join("etfs_data", "DATA_SCRAPPED", "CMC-FLOWS", "csv", "cmc_bitcoin_etf_flows_btc.csv")
    def run_cmc():
        driver = setup_driver(headless=True)
        try:
            return process_cmc_flows(driver, output_cmc)
        finally:
            driver.quit()

    if not run_step("CoinMarketCap Flows Scraper", run_cmc):
        sys.exit(1)

    # Step 3: Data Builder (Aggregation and Processing)
    if not run_step("Data Builder & Aggregator", data_builder.run):
        sys.exit(1)

    end_time = time.time()
    duration = end_time - start_time
    print("\n" + "="*60)
    print(f"PIPELINE COMPLETED SUCCESSFULLY in {duration:.2f} seconds.")
    print("="*60)

if __name__ == "__main__":
    main()
