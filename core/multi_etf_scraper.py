# multi_etf_scraper.py
# -------------------------------------------------------
# Multi-site Scraper (Refactored)
# -------------------------------------------------------

import os
import argparse
import datetime
from core.utils.helpers import (
    setup_driver, polite_sleep, SAVE_FORMAT, CSV_DIR, JSON_DIR, HEADLESS
)

# Import individual scrapers from the new core structure
from core.scrapers.scraper_grayscale import process_single_etf_grayscale, accept_cookies_grayscale
from core.scrapers.scraper_ishares import process_single_etf_ishares, accept_cookies_ishares
from core.scrapers.scraper_invesco import process_single_etf_invesco, accept_cookies_invesco
from core.scrapers.scraper_franklin import process_single_etf_franklin, accept_cookies_franklin
from core.scrapers.scraper_fidelity import process_single_etf_fidelity, accept_cookies_fidelity
from core.scrapers.scraper_vaneck import process_single_etf_vaneck, accept_cookies_vaneck
from core.scrapers.scraper_ark import process_single_etf_ark, accept_cookies_ark
from core.scrapers.scraper_coinshares import process_single_etf_coinshares, accept_cookies_coinshares
from core.scrapers.scraper_bosera import process_single_etf_bosera, accept_cookies_bosera
from core.scrapers.scraper_harvest import process_single_etf_harvest, accept_cookies_harvest
from core.scrapers.scraper_chinaamc import process_single_etf_chinaamc, accept_cookies_chinaamc
from core.scrapers.scraper_bitwise import process_single_etf_bitwise, accept_cookies_bitwise
from core.scrapers.scraper_wisdomtree import process_single_etf_wisdomtree, accept_cookies_wisdomtree

# Re-expose constants needed for fidelity_url
FIDELITY_START_DATE = os.getenv("ETF_FIDELITY_START_DATE", "11-Jan-2024")

def _today_fidelity_str():
    """Returns today's date in 'DD-Mon-YYYY' format for Fidelity URLs."""
    dt = datetime.datetime.now()
    months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    return f"{dt.day:02d}-{months[dt.month-1]}-{dt.year}"

def fidelity_url(start=FIDELITY_START_DATE, end=None, ticker="FBTC", sales="L"):
    """Constructs the Fidelity historical prices URL."""
    end = end or _today_fidelity_str()
    return (
        "https://www.fidelity.ca/en/historical-prices/"
        f"?historical={ticker}&sales-option={sales}&starting-date={start}&ending-date={end}"
    )

SITES_CONFIG = [
    {
        "name": "Grayscale",
        "url": "https://www.grayscale.com/resources",
        "etfs": [
            {
                "name": "Grayscale Bitcoin Mini Trust ETF",
                "search_terms": ["Bitcoin Mini Trust", "BTC", "Mini"],
                "output_filename": "btc_dailynav.xlsx",
                "process_config": {"sheet_to_keep": 0, "columns_to_keep": ["OTC Ticker","Date","Shares Outstanding","NAV Per Share","Market Price Per Share"]}
            },
            {
                "name": "Grayscale Bitcoin Trust ETF",
                "search_terms": ["Bitcoin Trust ETF","GBTC","Bitcoin Trust"],
                "output_filename": "gbtc_dailynav.xlsx",
                "process_config": {"sheet_to_keep": 0, "columns_to_keep": ["OTC Ticker","Date","Shares Outstanding","NAV Per Share","Market Price Per Share"]}
            }
        ]
    },
    {
        "name": "iShares",
        "url": "https://www.ishares.com/us/products/333011/ishares-bitcoin-trust-etf",
        "etfs": [
            {
                "name": "iShares Bitcoin Trust ETF",
                "search_terms": ["Data Download","Download","IBIT"],
                "output_filename": "ibit_dailynav.xlsx",
                "process_config": {"sheet_to_keep": 2, "columns_to_remove": ["Ex-Dividend","Ex-Dividends"]}
            }
        ]
    },
    {
        "name": "Invesco",
        "url": "https://www.invesco.com/us/financial-products/etfs/product-detail?ticker=BTCO",
        "etfs": [{"name": "Invesco Galaxy Bitcoin ETF (BTCO)", "output_filename": "btco_dailynav.xlsx"}]
    },
    {
        "name": "FranklinTempleton",
        "url": "https://www.franklintempleton.com/investments/options/exchange-traded-funds/products/39639/SINGLCLASS/franklin-bitcoin-etf/EZBC",
        "etfs": [{"name": "Franklin Bitcoin ETF (EZBC)", "output_filename": "ezbc_dailynav.xlsx"}]
    },
    {
        "name": "FidelityCA",
        "url": fidelity_url(),
        "etfs": [{"name": "Fidelity Advantage Bitcoin ETF (FBTC)", "output_filename": "fbtc_dailynav.xlsx"}]
    },
    {
        "name": "VanEck",
        "url": "https://www.vaneck.com/us/en/investments/bitcoin-etf-hodl/performance/",
        "etfs": [{"name": "VanEck Bitcoin ETF (HODL)", "output_filename": "hodl_dailynav.xlsx"}]
    },
    {
        "name": "ARK",
        "url": "https://www.ark-funds.com/funds/arkb",
        "etfs": [
            {
                "name": "ARK 21Shares Bitcoin ETF (ARKB)",
                "output_filename": "arkb_dailynav.xlsx",
                "api_url": "https://www.ark-funds.com/api/fund/nav-historical-change/1010?headingText=NAV%20Historical%20Change&overviewText=NAV%20and%20Market%20Price",
            }
        ]
    },
    {
        "name": "CoinShares",
        "url": "https://www.coinshares.com/",
        "etfs": [{"name": "Valkyrie Bitcoin ETF (BRRR) \u2013 CoinShares", "output_filename": "brrr_dailynav.xlsx"}]
    },
    {
        "name": "Bosera",
        "url": "https://www.bosera.com.hk/en-US/products/fund/detail/BTCL",
        "etfs": [{"name": "Bosera HashKey Bitcoin ETF (BTCL)", "output_filename": "bosera_dailynav.xlsx"}]
    },
    {
        "name": "HarvestHK",
        "url": "https://www.harvestglobal.com.hk/hgi/index.php/funds/passive/BTCETF#overview",
        "etfs": [{"name": "Harvest Bitcoin Spot ETF (BTCETF)", "output_filename": "harvest_dailynav.xlsx"}]
    },
    {
        "name": "ChinaAMC",
        "url": "https://www.chinaamc.com.hk/product/chinaamc-bitcoin-etf/",
        "etfs": [{"name": "ChinaAMC Bitcoin ETF (9042.HK)", "output_filename": "chinaamc_dailynav.xlsx"}]
    },
    {
        "name": "Bitwise",
        "url": "https://bitbetf.com/",
        "etfs": [{"name": "Bitwise Bitcoin ETF (BITB)", "output_filename": "bitb_dailynav.xlsx"}]
    },
    {
        "name": "WisdomTree",
        "url": "https://www.wisdomtree.com/investments/etfs/crypto/btcw",
        "etfs": [{"name": "WisdomTree Bitcoin Fund (BTCW)", "output_filename": "btcw_dailynav.xlsx"}]
    }
]

def accept_cookies_by_site(driver, name):
    """Dispatches cookie acceptance based on site name."""
    nm = name.lower()
    if nm == "grayscale": return accept_cookies_grayscale(driver)
    if nm == "ishares":   return accept_cookies_ishares(driver)
    if nm == "invesco":   return accept_cookies_invesco(driver)
    if nm == "franklintempleton": return accept_cookies_franklin(driver)
    if nm == "fidelityca": return accept_cookies_fidelity(driver)
    if nm == "vaneck":    return accept_cookies_vaneck(driver)
    if nm == "ark":       return accept_cookies_ark(driver)
    if nm == "coinshares":return accept_cookies_coinshares(driver)
    if nm == "bosera":    return accept_cookies_bosera(driver)
    if nm in ("harvesthk","harvest","harvestglobal"): return accept_cookies_harvest(driver)
    if nm == "chinaamc":  return accept_cookies_chinaamc(driver)
    if nm == "bitwise":   return accept_cookies_bitwise(driver)
    if nm == "wisdomtree": return accept_cookies_wisdomtree(driver)
    return False

def final_directory_cleanup():
    """Removes residual files in the output directories that are not part of the current configuration."""
    for target_dir in [CSV_DIR, JSON_DIR]:
        if not os.path.exists(target_dir): continue
        print(f"\n[CLEANUP-FINAL] Checking files in: {os.path.abspath(target_dir)}")
        allowed_bases = {site["etfs"][0]["output_filename"].split(".")[0] for site in SITES_CONFIG}
        # Special case for Grayscale which has two ETFs
        allowed_bases.add("gbtc_dailynav") 
        
        for fname in list(os.listdir(target_dir)):
            full = os.path.join(target_dir, fname)
            if not os.path.isfile(full): continue
            base, ext = os.path.splitext(fname)
            ext = ext.lstrip(".").lower()
            
            # Check if it should be kept
            is_allowed = (base in allowed_bases) and (ext in [SAVE_FORMAT, "json"])
            if not is_allowed:
                try:
                    os.remove(full)
                    print(f"[CLEANUP-FINAL] Removed residual: {full}")
                except: pass

def process_site(driver, site):
    """Processes all ETFs for a given provider/site."""
    name = site["name"]; url = site["url"]; etfs = site["etfs"]
    print("\n" + "="*60)
    print(f"PROCESSING SITE: {name}  (output .{SAVE_FORMAT})\nURL: {url}\nETFs: {len(etfs)}")
    print("="*60)
    res = {}
    try:
        driver.get(url); polite_sleep()
        accept_cookies_by_site(driver, name); polite_sleep()
        for etf in etfs:
            nm = name.lower()
            if nm == "ishares": ok, err = process_single_etf_ishares(driver, etf, url)
            elif nm == "grayscale": ok, err = process_single_etf_grayscale(driver, etf, url)
            elif nm == "invesco": ok, err = process_single_etf_invesco(driver, etf, url)
            elif nm == "franklintempleton": ok, err = process_single_etf_franklin(driver, etf, url)
            elif nm == "fidelityca": 
                url_dyn = fidelity_url()
                ok, err = process_single_etf_fidelity(driver, etf, url_dyn)
            elif nm == "vaneck": ok, err = process_single_etf_vaneck(driver, etf, url)
            elif nm == "ark": ok, err = process_single_etf_ark(driver, etf, url)
            elif nm == "coinshares": ok, err = process_single_etf_coinshares(driver, etf, url)
            elif nm == "bosera": ok, err = process_single_etf_bosera(driver, etf, url)
            elif nm in ("harvesthk","harvest","harvestglobal"): ok, err = process_single_etf_harvest(driver, etf, url)
            elif nm == "chinaamc": ok, err = process_single_etf_chinaamc(driver, etf, url)
            elif nm == "bitwise": ok, err = process_single_etf_bitwise(driver, etf, url)
            elif nm == "wisdomtree": ok, err = process_single_etf_wisdomtree(driver, etf, url)
            else: ok, err = False, "Unknown site scraper"
            res[etf["name"]] = (ok, err)
            polite_sleep()
        return res
    except Exception as e:
        print(f"[ERROR] Site {name}: {e}")
        for etf in etfs: res[etf["name"]] = (False, str(e))
        return res

def print_final_summary(all_results):
    """Prints a summarized report of the scraping operation."""
    print("\n" + "="*60)
    print("        FINAL SCRAPING SUMMARY")
    print("="*60)
    
    total_etfs = 0
    success_count = 0
    failures = []

    for site_name, etf_results in all_results.items():
        for etf_name, (ok, err) in etf_results.items():
            total_etfs += 1
            if ok:
                success_count += 1
                status = "✓ SUCCESS"
            else:
                status = "✗ FAILED "
                failures.append((etf_name, err))
            print(f"[{status}] {etf_name}")

    print("-" * 60)
    print(f"Overall Progress: {success_count}/{total_etfs} ETFs successfully processed.")
    
    if failures:
        print("\nDetailed Errors:")
        for etf_name, err in failures:
            print(f" - {etf_name}: {err}")
    else:
        print("\nAll scrapers completed successfully!")
    print("="*60 + "\n")

def run(headless=True, save_format=None):
    """Execution logic for multi-ETF scraping."""
    global SAVE_FORMAT, HEADLESS
    if save_format:
        SAVE_FORMAT = save_format
    
    os.makedirs(CSV_DIR, exist_ok=True)
    os.makedirs(JSON_DIR, exist_ok=True)
    
    all_results = {}
    try:
        for site in SITES_CONFIG:
            # We use a fresh driver for each site for maximum stability in CI.
            # Some sites (like Grayscale) will create their own dedicated driver 
            # and ignore this one, which is fine as long as we close what we open.
            driver = setup_driver(headless)
            try:
                all_results[site["name"]] = process_site(driver, site)
            finally:
                if driver:
                    try: driver.quit()
                    except: pass
                    
        final_directory_cleanup()
        print_final_summary(all_results)
        return True
    except Exception as e:
        print(f"[CRITICAL] Multi-scraper execution failed: {e}")
        return False

def main():
    """CLI entry point for multi-ETF scraping."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--format", choices=["csv","xlsx"], help="Output format (csv or xlsx)")
    parser.add_argument("--headless", action="store_true", default=True, help="Run in headless mode (default)")
    parser.add_argument("--no-headless", action="store_false", dest="headless", help="Run with visible window")
    args = parser.parse_args()
    
    return run(headless=args.headless, save_format=args.format)

if __name__ == "__main__":
    main()
