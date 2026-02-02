import os
import re
import time
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import sys

# Import undetected-chromedriver for anti-bot bypass
try:
    import undetected_chromedriver as uc
    UC_AVAILABLE = True
except ImportError:
    UC_AVAILABLE = False
    print("[CMC WARNING] undetected_chromedriver not available, will use standard driver")

# Add the project root to sys.path to allow absolute imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

try:
    from core.utils.helpers import (
        polite_sleep, setup_driver, save_dataframe, 
        SAVE_FORMAT, CSV_DIR, JSON_DIR, _get_chrome_major_version,
        OUTPUT_BASE_DIR
    )
except ImportError:
    # Fallback for standalone execution if sys.path trick fails
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../utils")))
    from helpers import (
        polite_sleep, setup_driver, save_dataframe, 
        SAVE_FORMAT, CSV_DIR, JSON_DIR, _get_chrome_major_version,
        OUTPUT_BASE_DIR
    )

# CMC Specific Config
CMC_URL = "https://coinmarketcap.com/etf/bitcoin/"
X_NEW_TABS  = "//div[contains(@class,'NewTabs_base') and contains(@class,'variant-roundedsquare')]"
X_LI_FLOWS  = "//li[@data-index='tab-flow' or normalize-space()='Flows' or .//h5[normalize-space()='Flows']]"
X_LI_BTC    = "//li[contains(@data-index,'btc') or normalize-space()='BTC' or .//h5[normalize-space()='BTC']]"

ROWS_PER_PAGE_HINT  = 100
SCROLL_STEP         = 420
SCROLL_WAIT         = 0.15  # Reduced for faster scrolling
MAX_IDLE_LOOPS      = 10    # Reduced for faster completion

# Final date marker - when we see this date, we've reached the end
FINAL_DATE_MARKERS = ["Jan 11, 2024", "2024-01-11", "11/01/2024", "01/11/2024", "January 11, 2024"]


def accept_cookies_cmc(driver):
    """Handles the cookie consent banner on the CMC website with multiple label attempts."""
    labels = ["Accept","Accept All","Allow all","Allow All","Agree","I agree","Consent",
              "Aceptar","Aceptar todo","Consentir","Estoy de acuerdo"]
    end = time.time() + 8
    while time.time() < end:
        for t in labels:
            els = driver.find_elements(By.XPATH, f"//button[normalize-space()='{t}']") or \
                  driver.find_elements(By.XPATH, f"//*[self::button or self::span or self::div][normalize-space()='{t}']")
            if els:
                el = els[0]
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                try: el.click()
                except: driver.execute_script("arguments[0].click();", el)
                time.sleep(0.4)
                return True
        time.sleep(0.25)
    return False


def _click_hard(driver, el):
    """Forcefully clicks an element using multiple methods if standard click fails."""
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    time.sleep(0.15)
    try:
        el.click(); return
    except: pass
    driver.execute_script("window.scrollBy(0, -120);")
    time.sleep(0.15)
    try:
        el.click(); return
    except: pass
    driver.execute_script("arguments[0].click();", el)


def _wait_selected_in_container(container_el, text, timeout=6):
    """Waits until a specific tab is marked as selected within a container."""
    xp = f".//ul[@data-role='Tabs']//li[(normalize-space()='{text}' or contains(.,'{text}')) and (@aria-selected='true' or contains(@class,'selected'))]"
    end = time.time() + timeout
    while time.time() < end:
        if container_el.find_elements(By.XPATH, xp):
            return True
        time.sleep(0.1)
    return False


def select_flows_btc(driver, wait):
    """Navigates to the 'Flows' tab and selects 'BTC' currency on the CMC page."""
    flow_li = wait.until(EC.presence_of_element_located((By.XPATH, X_LI_FLOWS)))
    flow_container = flow_li.find_element(By.XPATH, f"ancestor::{X_NEW_TABS[2:]}[1]")
    _click_hard(driver, flow_li)
    _wait_selected_in_container(flow_container, "Flows", 6)
    btc_li = wait.until(EC.presence_of_element_located((By.XPATH, X_LI_BTC)))
    currency_container = btc_li.find_element(By.XPATH, f"ancestor::{X_NEW_TABS[2:]}[1]")
    _click_hard(driver, btc_li)
    _wait_selected_in_container(currency_container, "BTC", 6)


def set_rows_per_page(driver, wait, value=100):
    """Attempts to change the number of rows displayed per page in the CMC table."""
    toggle = None
    for xp in [
        "//span[contains(.,'Show rows')]/following::*[self::button or self::div][1]",
        "//*[(@role='button' or self::button or self::div) and @aria-haspopup='listbox']",
        "//*[self::button or self::div][contains(normalize-space(),'50') or contains(normalize-space(),'25') or contains(normalize-space(),'100')]"
    ]:
        els = driver.find_elements(By.XPATH, xp)
        if els:
            toggle = els[0]; break
    if not toggle: return
    _click_hard(driver, toggle); time.sleep(0.1)
    opt_xpath = (f"(//*[self::button or self::div or self::li or self::span]"
                 f"[contains(@class,'dropdown-item') or @role='option'][normalize-space()='{value}'])[1]")
    try:
        opt = wait.until(EC.presence_of_element_located((By.XPATH, opt_xpath)))
        _click_hard(driver, opt); time.sleep(0.2)
    except: pass


def _get_table(driver):
    """Locates the flows table on the page."""
    table = None
    for t in driver.find_elements(By.XPATH, "//table"):
        ths = t.find_elements(By.XPATH, ".//thead//th")
        if ths and any("Time" in th.text for th in ths):
            table = t; break
    if table is None:
        try: table = driver.find_element(By.XPATH, "//div[@role='table']")
        except: pass
    if table is None: raise RuntimeError("Could not find CMC flows table.")
    return table


def _get_headers(table):
    """Extracts column headers from the table."""
    headers = [th.text.strip() for th in table.find_elements(By.XPATH, ".//thead//th") if th.text.strip()]
    if not headers:
        headers = [c.text.strip() for c in table.find_elements(By.XPATH, ".//tbody/tr[1]/*")]
    headers = [" ".join(h.split()) for h in headers]
    return headers


def _get_first_date(table):
    """Gets the date string from the first row of the table."""
    try:
        el = table.find_element(By.XPATH, ".//tbody/tr[1]/*[1]")
        return el.text.strip()
    except: return None


def _wait_table_page_loaded(driver, wait, prev_first, timeout=10):
    """Waits for the table to refresh after navigation by checking if the first date changed."""
    end = time.time() + timeout
    while time.time() < end:
        try:
            table = _get_table(driver)
            cur = _get_first_date(table)
            if cur and cur != prev_first: return True
        except: pass
        time.sleep(0.15)
    return False


def _parse_visible_rows(table, headers):
    """Parses currently visible rows in the table into a list of dictionaries."""
    rows = []
    ncols = len(headers)
    for tr in table.find_elements(By.XPATH, ".//tbody/tr"):
        cells = tr.find_elements(By.XPATH, "./*")[:ncols]
        vals = [c.text.strip() for c in cells]
        if not vals: continue
        rows.append({headers[i]: vals[i] for i in range(len(vals))})
    return rows


def _scroll_over_table_and_collect(driver, table, headers, rows_target=9999):
    """Scrolls through the table to trigger lazy loading and collects all visible data."""
    rect = driver.execute_script("const r=arguments[0].getBoundingClientRect();return {top:r.top,height:r.height};", table)
    driver.execute_script("window.scrollBy(0, arguments[0]);", rect["top"] - 200)

    seen = set(); data = []; idle = 0; last_len = 0
    date_key = headers[0] if headers else None

    def add_new(vis):
        nonlocal data
        for r in vis:
            if not r: continue
            dt = r.get(date_key, "")
            if not dt or dt in seen: continue
            clean = {}
            for k, v in r.items():
                if k == date_key: clean[k] = v
                else:
                    vv = (v or "").replace(",", "").replace("+", "").strip()
                    if vv in ("", "--", "—"): clean[k] = None; continue
                    vv = re.sub(r"[^0-9\.\-]", "", vv)
                    try: clean[k] = float(vv)
                    except: clean[k] = v
            data.append(clean); seen.add(dt)

    add_new(_parse_visible_rows(table, headers))
    while len(data) < rows_target and idle < MAX_IDLE_LOOPS:
        driver.execute_script("window.scrollBy(0, arguments[0]);", SCROLL_STEP)
        time.sleep(SCROLL_WAIT)
        add_new(_parse_visible_rows(table, headers))
        if len(data) == last_len: idle += 1
        else: idle = 0; last_len = len(data)
    return data


def paginate_and_scrape_all(driver, wait, rows_per_page_hint=100, last_known_date=None):
    """
    Orchestrates pagination and data collection across all pages of the flows table.
    
    Args:
        driver: Selenium WebDriver
        wait: WebDriverWait instance
        rows_per_page_hint: Number of rows per page (default 100)
        last_known_date: If provided, stop when this date is found (incremental fetch)
    """
    try: set_rows_per_page(driver, wait, value=rows_per_page_hint)
    except Exception as e: print(f"[CMC] Rows toggle warning: {e}")
    
    # Format last_known_date for comparison
    last_date_markers = []
    if last_known_date:
        from datetime import date as date_type
        if isinstance(last_known_date, date_type):
            # Generate multiple formats for the same date
            last_date_markers = [
                last_known_date.strftime("%b %d, %Y"),     # "Jan 08, 2025"
                last_known_date.strftime("%B %d, %Y"),     # "January 08, 2025"
                last_known_date.strftime("%Y-%m-%d"),      # "2025-01-08"
                last_known_date.strftime("%d/%m/%Y"),      # "08/01/2025"
                last_known_date.strftime("%m/%d/%Y"),      # "01/08/2025"
            ]
            print(f"[CMC] Incremental mode: will stop at date {last_known_date}")

    all_rows = []
    seen_dates = set()
    page = 1

    while True:
        # Get table with retry for SPA transitions
        table = None
        for attempt in range(5):
            try:
                table = _get_table(driver)
                break
            except RuntimeError:
                if attempt < 4:
                    time.sleep(1.0)
                    continue
                else:
                    print("[CMC] Could not find table after retries.")
                    return all_rows
        
        headers = _get_headers(table)
        page_rows = _scroll_over_table_and_collect(driver, table, headers, rows_target=9999)

        date_key = headers[0] if headers else None
        dedup = []
        found_last_known = False
        
        for r in page_rows:
            if not r: continue
            if date_key and r.get(date_key) in seen_dates: continue
            dedup.append(r)
            if date_key: seen_dates.add(r.get(date_key))
            
            # Check if this row contains the last known date
            if date_key and last_date_markers:
                date_val = str(r.get(date_key, ""))
                if any(marker in date_val for marker in last_date_markers):
                    found_last_known = True

        print(f"[CMC] Page {page}: {len(dedup)} rows collected")
        all_rows.extend(dedup)
        
        # If we found the last known date, save this page and exit
        if found_last_known:
            print(f"[CMC] ✅ Reached last known date ({last_known_date}). Incremental fetch complete.")
            return all_rows
        
        # Check if we've reached the final date (Jan 11, 2024) - absolute end
        if date_key:
            for r in dedup:
                date_val = r.get(date_key, "")
                if any(marker in str(date_val) for marker in FINAL_DATE_MARKERS):
                    print(f"[CMC] Reached final date ({date_val}). Scraping complete.")
                    return all_rows

        # Scroll to bottom to ensure pagination controls are visible
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.3)

        next_btn = None
        next_selectors = [
            # Primary: anchor with aria-label="Next page" (actual CMC markup)
            "//a[@aria-label='Next page']",
            "//a[contains(@class,'chevron') and contains(@href,'page=')]",
            # Fallback: any element with Next page aria-label
            "//*[@aria-label='Next page']",
        ]
        
        for xp in next_selectors:
            els = driver.find_elements(By.XPATH, xp)
            if els:
                for el in els:
                    # Check if button is enabled (aria-disabled != 'true')
                    aria_disabled = el.get_attribute("aria-disabled")
                    if aria_disabled == "true":
                        print(f"[CMC DEBUG] Found disabled button: {xp}")
                        continue
                    if el.is_displayed():
                        next_btn = el
                        print(f"[CMC DEBUG] Found enabled Next button with selector: {xp}")
                        break
            if next_btn:
                break

        if not next_btn:
            print("[CMC] No more pages available (Next button not found or disabled).")
            break

        # Get current page indicator for reference
        prev_indicator = None
        try:
            indicator_els = driver.find_elements(By.XPATH, "//*[contains(text(),'Showing') and contains(text(),'out of')]")
            if indicator_els:
                prev_indicator = indicator_els[0].text.strip()
        except:
            pass
        
        prev_first = _get_first_date(table)
        
        # Click using ActionChains for more realistic click behavior
        print(f"[CMC DEBUG] Clicking Next for page {page + 1}...")
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", next_btn)
            time.sleep(0.25)
            ActionChains(driver).move_to_element(next_btn).pause(0.1).click().perform()
        except Exception as click_err:
            print(f"[CMC DEBUG] ActionChains failed, using JS click...")
            driver.execute_script("arguments[0].click();", next_btn)
        
        # Wait for page to update
        time.sleep(1.5)
        
        page_changed = False
        end_time = time.time() + 15
        
        while time.time() < end_time and not page_changed:
            try:
                # Check if page indicator changed
                indicator_els = driver.find_elements(By.XPATH, "//*[contains(text(),'Showing') and contains(text(),'out of')]")
                if indicator_els and prev_indicator:
                    cur_indicator = indicator_els[0].text.strip()
                    if cur_indicator != prev_indicator:
                        page_changed = True
                        break
                
                # Check if first date changed
                table = _get_table(driver)
                cur_first = _get_first_date(table)
                if cur_first and prev_first and cur_first != prev_first:
                    page_changed = True
                    break
                    
            except:
                time.sleep(0.3)
                continue
            
            time.sleep(0.25)
        
        if not page_changed:
            print("[CMC] Table did not refresh. End of data.")
            break
        
        # Scroll back to top of page so table is visible for next iteration
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.5)
        page += 1

    return all_rows


def process_cmc_flows(driver, base_name="cmc_bitcoin_etf_flows_btc"):
    """Main function to scrape CoinMarketCap Bitcoin ETF flows and save them to CSV, JSON and database."""
    print(f"\n[CMC] Scraping flows from {CMC_URL}")
    print("="*50)
    try:
        # Query last known date from database for incremental fetch
        last_known_date = None
        try:
            from core.db_adapter import is_db_enabled, get_last_cmc_flow_date
            if is_db_enabled():
                last_known_date = get_last_cmc_flow_date()
                if last_known_date:
                    print(f"[CMC] Last date in DB: {last_known_date}")
                else:
                    print("[CMC] No existing data in DB, will fetch all historical data")
        except ImportError:
            print("[CMC] Database not available, will fetch all historical data")
        except Exception as e:
            print(f"[CMC] Could not query last date from DB: {e}")
        
        driver.get(CMC_URL); polite_sleep()
        accept_cookies_cmc(driver); polite_sleep()
        
        wait = WebDriverWait(driver, 30)
        select_flows_btc(driver, wait)
        
        # Pass last_known_date for incremental fetch
        rows = paginate_and_scrape_all(driver, wait, ROWS_PER_PAGE_HINT, last_known_date=last_known_date)
        if not rows:
            return False, "No rows could be scraped from CoinMarketCap."
            
        df = pd.DataFrame(rows)
        # Standardize the first column (usually 'Time') to 'date'
        if not df.empty and len(df.columns) > 0:
            df.rename(columns={df.columns[0]: "date"}, inplace=True)
        
        # CMC flows MUST ALWAYS be saved to CSV because data_builder.py depends on it
        # This bypasses the ETF_SAVE_FILES setting intentionally
        os.makedirs(CSV_DIR, exist_ok=True)
        os.makedirs(JSON_DIR, exist_ok=True)
        
        csv_path = os.path.join(CSV_DIR, f"{base_name}.csv")
        json_path = os.path.join(JSON_DIR, f"{base_name}.json")
        
        # Save CSV (required for data_builder.py)
        df.to_csv(csv_path, index=False)
        print(f"[CMC] ✅ CSV saved: {csv_path} ({len(df)} rows)")
        
        # Save JSON
        try:
            df.to_json(json_path, orient="records", indent=2)
            print(f"[CMC] ✅ JSON saved: {json_path}")
        except Exception as e:
            print(f"[CMC] ⚠️  JSON save failed: {e}")
        
        # Save to database using the dedicated flows function
        try:
            from core.db_adapter import is_db_enabled, save_cmc_flows
            if is_db_enabled():
                count = save_cmc_flows(df)
                if count > 0:
                    print(f"[CMC] ✅ Saved {count} flow records to database")
        except ImportError:
            pass  # db_adapter not available
        except Exception as e:
            print(f"[CMC] ⚠️  Failed to save flows to database: {e}")
        
        return True, None
    except Exception as e:
        msg = f"CMC error: {e}"
        print(f"[CMC ERROR] {msg}")
        # Diagnostic: Screen on error
        try:
            shot_err = os.path.join(OUTPUT_BASE_DIR, f"debug_cmc_error_{int(time.time())}.png")
            driver.save_screenshot(shot_err)
            print(f"[CMC] Error screenshot saved: {shot_err}")
        except: pass
        return False, msg


def _setup_uc_driver(headless=False):
    """Create an undetected Chrome driver to bypass anti-bot detection."""
    # Using the centralized setup_driver from helpers
    driver = setup_driver(headless=headless)
    driver.set_window_size(1920, 1080)
    time.sleep(2)  # Allow driver to stabilize
    return driver


def main():
    """Standalone execution entry point using undetected-chromedriver."""
    driver = None
    try:
        if UC_AVAILABLE:
            print("[CMC] Using undetected-chromedriver for anti-bot bypass...")
            driver = _setup_uc_driver(headless=False)
        else:
            print("[CMC] Falling back to standard driver...")
            driver = setup_driver(headless=False)
        
        ok, err = process_cmc_flows(driver)
        if ok: print("[STANDALONE] CMC processed successfully.")
        else: print(f"[STANDALONE] CMC failed: {err}")
    except Exception as e:
        print(f"[CMC] Fatal error: {e}")
    finally:
        if driver:
            try:
                driver.close()
            except:
                pass
            try:
                # Suppress stderr during quit to hide Windows handle errors
                import sys as _sys
                stderr_backup = _sys.stderr
                _sys.stderr = open(os.devnull, 'w')
                driver.quit()
                _sys.stderr = stderr_backup
            except:
                pass


if __name__ == "__main__":
    main()
