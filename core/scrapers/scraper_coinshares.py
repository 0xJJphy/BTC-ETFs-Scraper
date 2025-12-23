import os
import json
import time
import datetime
import pandas as pd
import yfinance as yf
from urllib.parse import urlencode, quote
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sys

# Add the project root to sys.path to allow absolute imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

try:
    from core.utils.helpers import (
        polite_sleep, save_dataframe, _try_click_any,
        setup_driver, SAVE_FORMAT
    )
except ImportError:
    # Fallback for standalone execution if sys.path trick fails
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../utils")))
    from helpers import (
        polite_sleep, save_dataframe, _try_click_any,
        setup_driver, SAVE_FORMAT
    )

COINSHARES_API_BASE   = "https://www-api.coinshares.com/api/v2/Widgets"
COINSHARES_API_KEY    = os.getenv("COINSHARES_API_KEY", "094DA478-140C-4E3E-B394-7A19BBE8326B")
COINSHARES_YF_TICKER  = os.getenv("COINSHARES_YF_TICKER", "BRRR")

def coinshares_api_url(names_csv: str) -> str:
    """Generates the URL for the CoinShares Widgets API."""
    qs = urlencode({"ApiKey": COINSHARES_API_KEY, "names": names_csv}, quote_via=quote, safe=",")
    return f"{COINSHARES_API_BASE}?{qs}"

def accept_cookies_coinshares(driver):
    """Handles the cookie consent choices on the CoinShares website."""
    return _try_click_any(driver, [
        "//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'allow all')]",
        "//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'allow selection')]",
        "//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'continue')]",
        "//button[contains(.,'ACCEPT ALL') or contains(.,'Accept all')]",
    ], wait_sec=12)

def _coinshares_open_api_tab_and_parse(driver, names_csv: str):
    """Opens a new browser tab to fetch and parse JSON from the Widgets API."""
    url = coinshares_api_url(names_csv)
    driver.switch_to.new_window('tab')
    driver.get(url)
    try:
        # Wait for the JSON to be displayed in the page's <pre> tag
        pre = WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.TAG_NAME, "pre")))
        raw = pre.text.strip()
    except:
        raw = driver.execute_script("return document.body ? document.body.innerText : '';").strip()
    
    if not raw:
        raise RuntimeError("Empty response from Widgets API.")
    
    try:
        return json.loads(raw)
    finally:
        try:
            driver.close()
            driver.switch_to.window(driver.window_handles[-1])
        except:
            pass

def _coinshares_find_series(payload):
    """Recursive helper to find the dataX/dataY series within the API response JSON."""
    if isinstance(payload, dict):
        if "series" in payload and isinstance(payload["series"], list) and payload["series"]:
            s0 = payload["series"][0]
            if isinstance(s0, dict) and "dataX" in s0 and "dataY" in s0:
                return s0
        for v in payload.values():
            s = _coinshares_find_series(v)
            if s is not None:
                return s
    elif isinstance(payload, list):
        for it in payload:
            s = _coinshares_find_series(it)
            if s is not None:
                return s
    return None

def process_single_etf_coinshares(driver, etf, site_url):
    """Orchestrates fetching CoinShares premium/discount data from their API and combining it with Yahoo Finance prices."""
    name = etf["name"]
    base = os.path.splitext(etf["output_filename"])[0]
    widgets_csv = etf.get("widget_names") or "VALKYRIE_PREMIUMDISCOUNT_BRRR"
    print(f"\n[ETF] Processing {name} (CoinShares - Widgets API + yfinance) -> output .{SAVE_FORMAT}")
    print("="*50)

    try:
        driver.get(site_url); polite_sleep()
        accept_cookies_coinshares(driver); polite_sleep()
    except Exception as e:
        print(f"[COINSHARES] Navigation warning: {e}")

    try:
        payload = _coinshares_open_api_tab_and_parse(driver, widgets_csv)
        print("[COINSHARES] SUCCESS Widgets JSON obtained")
    except Exception as e:
        msg = f"Widgets API Error: {e}"
        print(f"[COINSHARES] {msg}")
        return False, msg

    series = _coinshares_find_series(payload)
    if not series:
        msg = "dataX/dataY series not found in the API response."
        print(f"[COINSHARES] {msg}")
        return False, msg

    # Parse dates and premium percentages
    dates = pd.to_datetime(series["dataX"], errors="coerce")
    prem  = pd.to_numeric(series["dataY"], errors="coerce")
    df_pd = pd.DataFrame({"date": dates, "premium_pct": prem}).dropna()
    df_pd["date"] = df_pd["date"].dt.strftime("%Y%m%d")

    if df_pd.empty:
        msg = "The extracted data series is empty."
        print(f"[COINSHARES] {msg}")
        return False, msg

    # Fetch historical market prices from Yahoo Finance
    start = df_pd["date"].min()
    end   = df_pd["date"].max()
    try:
        y = yf.Ticker(COINSHARES_YF_TICKER).history(
            start=pd.to_datetime(start).date(),
            end=(pd.to_datetime(end).date() + datetime.timedelta(days=1)),
            interval="1d",
            auto_adjust=False
        )
        y = y.reset_index()
        date_col = "Date" if "Date" in y.columns else y.columns[0]
        y["date"] = pd.to_datetime(y[date_col]).dt.strftime("%Y%m%d")
        df_px = y[["date","Close"]].rename(columns={"Close": "market price"})
    except Exception as e:
        msg = f"YFinance history fetch failed: {e}"
        print(f"[COINSHARES] {msg}")
        return False, msg

    # Merge prices with premium/discount data to calculate NAV
    merged = df_pd.merge(df_px, on="date", how="inner")
    if merged.empty:
        msg = "No overlapping dates found between premium data and market prices."
        print(f"[COINSHARES] {msg}")
        return False, msg

    # Calculate NAV: NAV = MarketPrice / (1 + Premium%)
    merged["nav"] = merged["market price"] / (1.0 + merged["premium_pct"]/100.0)
    out = merged[["date","nav","market price"]].sort_values("date")

    try:
        save_dataframe(out, base, sheet_name="Historical")
        print(f"[SUCCESS] CoinShares processed ({name})")
        return True, None
    except Exception as e:
        msg = f"CoinShares processing error: {e}"
        print(f"[COINSHARES ERROR] {msg}")
        return False, msg

def main():
    """Standalone execution entry point."""
    etf = {"name": "Valkyrie Bitcoin ETF (BRRR) \u2013 CoinShares", "output_filename": "brrr_dailynav.xlsx"}
    site_url = "https://www.coinshares.com/"
    
    driver = setup_driver(headless=False)
    try:
        ok, err = process_single_etf_coinshares(driver, etf, site_url)
        if ok:
            print("[STANDALONE] CoinShares processed successfully.")
        else:
            print(f"[STANDALONE] CoinShares failed: {err}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
