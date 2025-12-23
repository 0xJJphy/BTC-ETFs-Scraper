import os
import json
import time
import pandas as pd
from urllib.parse import urlparse
import sys

# Add the project root to sys.path to allow absolute imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

try:
    from core.utils.helpers import (
        polite_sleep, _session_from_driver, browser_fetch_text,
        _retry_after_seconds, save_dataframe, setup_driver,
        _try_click_any, MAX_RETRIES, BACKOFF_BASE, BACKOFF_MAX,
        SAVE_FORMAT
    )
except ImportError:
    # Fallback for standalone execution if sys.path trick fails
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../utils")))
    from helpers import (
        polite_sleep, _session_from_driver, browser_fetch_text,
        _retry_after_seconds, save_dataframe, setup_driver,
        _try_click_any, MAX_RETRIES, BACKOFF_BASE, BACKOFF_MAX,
        SAVE_FORMAT
    )

def accept_cookies_ark(driver):
    """Handles the cookie consent banner on the ARK website."""
    return _try_click_any(driver, [
        "//button[contains(.,'Accept All') or contains(.,'I Accept') or contains(.,'Agree')]",
        "//button[@id='onetrust-accept-btn-handler']",
        "#onetrust-accept-btn-handler"
    ], wait_sec=10)

def process_single_etf_ark(driver, etf, site_url):
    """Processes historical data for an ARK ETF using their JSON API."""
    name = etf["name"]
    base = os.path.splitext(etf["output_filename"])[0]
    api_url = etf.get("api_url")
    print(f"\n[ETF] Processing {name} (ARK - JSON API) -> output .{SAVE_FORMAT}")
    print("="*50)

    try:
        driver.get(site_url); polite_sleep()
        accept_cookies_ark(driver); polite_sleep()
    except Exception as e:
        print(f"[ARK] Navigation warning: {e}")

    if not api_url:
        msg = "api_url not defined in config."
        print(f"[ARK] {msg}")
        return False, msg

    data = None

    # Step 1: Try fetching via browser to use existing session/stealth
    try:
        txt = browser_fetch_text(driver, api_url)
        data = json.loads(txt)
        print("[ARK] SUCCESS JSON obtained via browser")
    except Exception as e:
        print(f"[ARK] Browser fetch failed: {e}")

    # Step 2: Fallback to requests if browser fetch fails
    if data is None:
        sess = _session_from_driver(driver)
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/plain, */*",
            "Referer": site_url,
            "Origin": urlparse(site_url).scheme + "://" + urlparse(site_url).netloc,
            "X-Requested-With": "XMLHttpRequest",
            "Accept-Language": "en-US,en;q=0.9",
        }
        for attempt in range(MAX_RETRIES):
            try:
                polite_sleep()
                r = sess.get(api_url, headers=headers, timeout=60)
                if r.status_code in (429, 403, 503):
                    ra = _retry_after_seconds(r.headers.get("Retry-After"))
                    wait = ra if ra is not None else min(BACKOFF_MAX, (BACKOFF_BASE ** attempt) + random.random())
                    time.sleep(wait); continue
                r.raise_for_status()
                data = r.json()
                print("[ARK] SUCCESS JSON obtained via requests fallback")
                break
            except Exception as e:
                wait = min(BACKOFF_MAX, (BACKOFF_BASE ** attempt) + 1.0)
                time.sleep(wait)

    if data is None:
        msg = "Could not obtain JSON from ARK after multiple attempts."
        print(f"[ARK] {msg}")
        return False, msg

    # Parse JSON structure
    chart = data.get("chartData") or data.get("data") or []
    if isinstance(chart, dict) and "rows" in chart:
        chart = chart["rows"]

    rows = []
    for item in chart:
        try:
            nav = item.get("nav")
            mp  = item.get("marketPrice")
            ep = item.get("epochDateMilliSeconds") or item.get("epochDate")
            if ep is None:
                continue
            dt = pd.to_datetime(int(ep), unit="ms", utc=True).tz_convert(None)
            rows.append({"date": dt.strftime("%Y%m%d"), "nav": nav, "market price": mp})
        except:
            pass

    if not rows:
        msg = "No data rows extracted from JSON."
        print(f"[ARK] {msg}")
        return False, msg

    # Format DataFrame
    df = pd.DataFrame(rows)
    for c in ["nav","market price"]:
        df[c] = (df[c].astype(str)
                    .str.replace("$","", regex=False)
                    .str.replace(",","", regex=False)
                    .str.strip())
    df = df[["date","nav","market price"]]

    try:
        save_dataframe(df, base, sheet_name="Historical")
        print(f"[SUCCESS] ARK processed ({name})")
        return True, None
    except Exception as e:
        msg = f"ARK processing error: {e}"
        print(f"[ARK ERROR] {msg}")
        return False, msg

def main():
    """Standalone execution entry point."""
    etf = {
        "name": "ARK 21Shares Bitcoin ETF (ARKB)",
        "output_filename": "arkb_dailynav.xlsx",
        "api_url": "https://www.ark-funds.com/api/fund/nav-historical-change/1010?headingText=NAV%20Historical%20Change&overviewText=NAV%20and%20Market%20Price",
    }
    site_url = "https://www.ark-funds.com/funds/arkb"
    
    driver = setup_driver(headless=False)
    try:
        ok, err = process_single_etf_ark(driver, etf, site_url)
        if ok:
            print("[STANDALONE] ARK processed successfully.")
        else:
            print(f"[STANDALONE] ARK failed: {err}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
