import os
import sys
import time
import json
from pathlib import Path
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Add project root to sys.path to allow standalone execution
root_path = Path(__file__).resolve().parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

from core.utils.helpers import (
    polite_sleep, normalize_date_column, save_dataframe,
    _try_click_any, setup_driver, SAVE_FORMAT, OUTPUT_BASE_DIR
)

BTCW_HISTORY_API_URL = "https://www.wisdomtree.com/api/fund-history/48684713?view=navHistoryModal"


def accept_cookies_wisdomtree(driver):
    """Handle cookie consent banner and welcome modals on the WisdomTree website."""
    # 1. Cookie Consent
    _ = _try_click_any(driver, [
        "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
        "//a[@id='CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll']",
        "//button[@id='CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll']",
        "//a[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'allow all')]",
        "//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'allow all')]",
    ], wait_sec=10)
    polite_sleep()

    # 2. Welcome Modal / Country Selection
    _try_click_any(driver, [
        "//a[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'continue to us website')]",
        "//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'continue to us website')]",
        "a.button-link.login-btn.button.-secondary.continue-btn",
        "a.button.-secondary.continue-btn",
    ], wait_sec=8)
    polite_sleep()

    # 3. Investor Type Selection (if prompted to avoid Restricted Content)
    try:
        investor_choices = [
            "//span[contains(text(),'Individual Investor')]",
            "//button[contains(.,'Individual Investor')]",
            "//a[contains(.,'Individual Investor')]",
            "//span[contains(text(),'Financial Professional')]",
            "//button[contains(.,'Financial Professional')]"
        ]
        if _try_click_any(driver, investor_choices, wait_sec=5):
            print("[WISDOMTREE] Selected investor type.")
            _try_click_any(driver, ["//button[contains(.,'Enter Site') or contains(.,'Confirm') or contains(.,'Accept')]"], wait_sec=3)
            polite_sleep()
    except:
        pass


def _wait_for_cf_clearance(driver, timeout=20):
    """Waits for the Cloudflare clearance cookie to appear before the page's own data fetches will succeed."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            if any(c.get("name") == "cf_clearance" for c in driver.get_cookies()):
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def _wisdomtree_history_load_failed(driver):
    """Detects the dialog's own 'unable to load historical data' error state."""
    try:
        return bool(driver.find_elements(
            By.XPATH, "//*[contains(text(),'unable to load historical data')]"
        ))
    except Exception:
        return False


def _wisdomtree_click_history_trigger(driver):
    """Clicks the button that opens the History dialog. Returns True if a trigger was found and clicked."""
    link_selectors = [
        "//button[normalize-space(text())='View NAV, Market Price and Premium/Discount History']",
        "//button[contains(normalize-space(.),'View NAV') and contains(normalize-space(.),'History')]",
    ]
    for sel in link_selectors:
        try:
            link = WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, sel)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", link)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", link)
            print(f"[WISDOMTREE] Triggered history dialog via: {sel}")
            return True
        except Exception as ex:
            print(f"[WISDOMTREE] Selector failed: {sel} -> {ex}")
    return False


def _wisdomtree_open_history_modal(driver):
    """Open the NAV/Market Price/Premium-Discount History dialog on the WisdomTree page."""
    for attempt in range(2):
        if not _wisdomtree_click_history_trigger(driver):
            raise RuntimeError("WisdomTree: History dialog trigger not found.")

        print("[WISDOMTREE] Waiting for history dialog to appear...")
        WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "section[role='dialog']"))
        )

        try:
            WebDriverWait(driver, 25).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "section[role='dialog'] table tr[data-key]"))
            )
            print("[WISDOMTREE] Table rows detected in dialog.")
            return
        except Exception:
            if attempt == 0 and _wisdomtree_history_load_failed(driver):
                print("[WISDOMTREE] Dialog reported a load failure, closing and retrying...")
                try:
                    driver.find_element(By.XPATH, "//button[normalize-space(text())='Close']").click()
                except Exception:
                    pass
                time.sleep(3)
                continue
            raise

    raise RuntimeError("WisdomTree: History table did not load after retry.")


def _wisdomtree_fetch_history_api(driver):
    """Fetches NAV/market price history directly from WisdomTree's JSON API (reuses the browser's Cloudflare clearance)."""
    js = """
    const url = arguments[0];
    const done = arguments[1];
    fetch(url, {credentials: 'include', headers: {'accept': '*/*'}})
      .then(r => r.text().then(t => done(JSON.stringify({status: r.status, text: t}))))
      .catch(e => done(JSON.stringify({status: -1, text: 'FETCH_ERROR: ' + (e && e.message ? e.message : String(e))})));
    """
    raw = driver.execute_async_script(js, BTCW_HISTORY_API_URL)
    result = json.loads(raw)
    status = result.get("status")
    txt = result.get("text", "")
    print(f"[WISDOMTREE] API fetch status={status}, body_preview={txt[:200]!r}")
    if status != 200:
        raise RuntimeError(f"WisdomTree API returned status {status}")

    data = json.loads(txt)
    if not isinstance(data, list) or not data:
        raise RuntimeError("WisdomTree: API returned no data")

    rows = []
    for item in data:
        dt = item.get("dt")
        if not dt:
            continue
        rows.append({"date": dt, "nav": item.get("nav"), "market price": item.get("closePrice")})

    if not rows:
        raise RuntimeError("WisdomTree: Could not parse any rows from API response")

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y%m%d")
    for c in ["nav", "market price"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    print(f"[WISDOMTREE] Parsed {len(df)} rows from direct API fetch")
    return df[["date", "nav", "market price"]]


def _wisdomtree_parse_table(driver):
    """Parse the data table within the WisdomTree history dialog into a clean DataFrame."""
    rows = driver.find_elements(By.CSS_SELECTOR, "section[role='dialog'] table tr[data-key]")
    if not rows:
        raise RuntimeError("WisdomTree: History table not found or empty")

    data = []
    for r in rows:
        cols = r.find_elements(By.TAG_NAME, "td")
        if len(cols) < 3: continue
        # Extract text: Date, NAV, Market Price
        t1, t2, t3 = cols[0].text.strip(), cols[1].text.strip(), cols[2].text.strip()
        if not t1 or "Date" in t1: continue
        data.append([t1, t2, t3])

    if not data:
        raise RuntimeError("WisdomTree: Could not parse any data rows from history dialog")

    df = pd.DataFrame(data, columns=["date", "nav", "market price"])
    df = normalize_date_column(df)
    for c in ["nav", "market price"]:
        df[c] = (df[c].astype(str).str.replace("$", "", regex=False).str.replace(",", "", regex=False)
                    .str.replace("—", "", regex=False).str.strip())
        df[c] = pd.to_numeric(df[c], errors="coerce")

    print(f"[WISDOMTREE] Parsed {len(df)} rows from history dialog")
    return df[["date", "nav", "market price"]]


def process_single_etf_wisdomtree(driver, etf, site_url):
    """
    Main process to scrape a single WisdomTree ETF.
    
    IMPORTANT: This function ignores the passed driver and creates its own
    undetected-chromedriver instance to bypass Cloudflare bot detection.
    """
    name = etf["name"]
    base = os.path.splitext(etf["output_filename"])[0]
    print(f"\n[ETF] Processing {name} (WisdomTree – Modal NAV/Px/Discount) → output .{SAVE_FORMAT}")
    print("=" * 50)
    
    # WisdomTree requires undetected-chromedriver to bypass Cloudflare
    print("[WISDOMTREE] Creating dedicated undetected-chromedriver instance...")
    
    dedicated_driver = None
    try:
        # Auto-detect headless mode from environment
        headless = os.environ.get("DISPLAY") is None or os.environ.get("ETF_HEADLESS", "false").lower() == "true"
        
        # Use setup_driver from helpers to ensure Chrome version detection and anti-bot patches
        dedicated_driver = setup_driver(headless=headless)
        print(f"[WISDOMTREE] ✅ driver initialized via helpers (headless={headless})")
        
        dedicated_driver.get(site_url)
        polite_sleep()
        
        accept_cookies_wisdomtree(dedicated_driver)
        polite_sleep()

        got_clearance = _wait_for_cf_clearance(dedicated_driver, timeout=20)
        print(f"[WISDOMTREE] Cloudflare clearance cookie present: {got_clearance}")

        # Diagnostic: Screen after cookies
        shot_path = os.path.join(OUTPUT_BASE_DIR, "debug_wisdomtree_after_cookies.png")
        dedicated_driver.save_screenshot(shot_path)
        print(f"[WISDOMTREE] Screenshot after cookies: {shot_path}")

        # Step 1: Try the JSON API directly (reuses the browser's Cloudflare clearance cookie)
        df = None
        try:
            df = _wisdomtree_fetch_history_api(dedicated_driver)
        except Exception as e:
            print(f"[WISDOMTREE] Direct API fetch failed: {e} -> falling back to modal parsing")

        # Step 2: Fall back to opening the history dialog and parsing its table
        if df is None:
            _wisdomtree_open_history_modal(dedicated_driver)
            df = _wisdomtree_parse_table(dedicated_driver)

        save_dataframe(df, base, sheet_name="Historical")
        print(f"[SUCCESS] ✓ WisdomTree processed ({name})")
        return True, None
        
    except ImportError:
        msg = "undetected-chromedriver not installed. Run: pip install undetected-chromedriver"
        print(f"[WISDOMTREE ERROR] {msg}")
        return False, msg
    except Exception as e:
        msg = f"Error: {e}"
        print(f"[WISDOMTREE ERROR] {msg}")
        # Diagnostic: Screen on error
        try:
            shot_err = os.path.join(OUTPUT_BASE_DIR, f"debug_wisdomtree_error_{int(time.time())}.png")
            dedicated_driver.save_screenshot(shot_err)
            print(f"[WISDOMTREE] Error screenshot saved: {shot_err}")
        except: pass
        return False, msg
    finally:
        if dedicated_driver:
            try:
                dedicated_driver.quit()
                print("[WISDOMTREE] Dedicated driver closed")
            except:
                pass


def main():
    """Standalone execution entry point."""
    etf = {"name": "WisdomTree Bitcoin Fund (BTCW)", "output_filename": "btcw_dailynav.xlsx"}
    site_url = "https://www.wisdomtree.com/investments/etfs/crypto/btcw"

    ok, err = process_single_etf_wisdomtree(None, etf, site_url)
    if ok:
        print("[STANDALONE] WisdomTree processed successfully.")
    else:
        print(f"[STANDALONE] WisdomTree failed: {err}")


if __name__ == "__main__":
    main()
