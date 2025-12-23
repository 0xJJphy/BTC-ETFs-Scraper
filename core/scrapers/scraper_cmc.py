import os
import re
import time
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sys

# Add the project root to sys.path to allow absolute imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

try:
    from core.utils.helpers import (
        polite_sleep, setup_driver, save_dataframe, 
        SAVE_FORMAT, CSV_DIR, JSON_DIR
    )
except ImportError:
    # Fallback for standalone execution if sys.path trick fails
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../utils")))
    from helpers import (
        polite_sleep, setup_driver, save_dataframe, 
        SAVE_FORMAT, CSV_DIR, JSON_DIR
    )

# CMC Specific Config
CMC_URL = "https://coinmarketcap.com/etf/bitcoin/"
X_NEW_TABS  = "//div[contains(@class,'NewTabs_base') and contains(@class,'variant-roundedsquare')]"
X_LI_FLOWS  = "//li[@data-index='tab-flow' or normalize-space()='Flows' or .//h5[normalize-space()='Flows']]"
X_LI_BTC    = "//li[contains(@data-index,'btc') or normalize-space()='BTC' or .//h5[normalize-space()='BTC']]"

ROWS_PER_PAGE_HINT  = 100
SCROLL_STEP         = 420
SCROLL_WAIT         = 0.10
MAX_IDLE_LOOPS      = 12

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
    time.sleep(0.08)
    try:
        el.click(); return
    except: pass
    driver.execute_script("window.scrollBy(0, -120);")
    time.sleep(0.08)
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

def paginate_and_scrape_all(driver, wait, rows_per_page_hint=100):
    """Orchestrates pagination and data collection across all pages of the flows table."""
    try: set_rows_per_page(driver, wait, value=rows_per_page_hint)
    except Exception as e: print(f"[CMC] Rows toggle warning: {e}")

    all_rows = []
    seen_dates = set()
    page = 1

    while True:
        table = _get_table(driver)
        headers = _get_headers(table)
        page_rows = _scroll_over_table_and_collect(driver, table, headers, rows_target=9999)

        date_key = headers[0] if headers else None
        dedup = []
        for r in page_rows:
            if not r: continue
            if date_key and r.get(date_key) in seen_dates: continue
            dedup.append(r)
            if date_key: seen_dates.add(r.get(date_key))

        print(f"[CMC] Page {page}: {len(dedup)} rows collected")
        all_rows.extend(dedup)

        next_btn = None
        for xp in [
            "//ul[contains(@class,'pagination')]//*[contains(@class,'next') and not(contains(@class,'disabled'))]//*[self::a or self::button]",
            "//button[@aria-label='Next' and not(@disabled)]",
            "//a[contains(@class,'next') and not(contains(@class,'disabled'))]"
        ]:
            els = driver.find_elements(By.XPATH, xp)
            if els: next_btn = els[0]; break

        if not next_btn:
            print("[CMC] No more pages available.")
            break

        prev_first = _get_first_date(table)
        _click_hard(driver, next_btn)
        if not _wait_table_page_loaded(driver, wait, prev_first, timeout=10):
            print("[CMC] Table did not refresh after clicking 'Next'. Categorized as end of data.")
            break
        page += 1

    return all_rows

def process_cmc_flows(driver, base_name="cmc_bitcoin_etf_flows_btc"):
    """Main function to scrape CoinMarketCap Bitcoin ETF flows and save them to CSV and JSON."""
    print(f"\n[CMC] Scraping flows from {CMC_URL}")
    print("="*50)
    try:
        driver.get(CMC_URL); polite_sleep()
        accept_cookies_cmc(driver); polite_sleep()
        
        wait = WebDriverWait(driver, 30)
        select_flows_btc(driver, wait)
        
        rows = paginate_and_scrape_all(driver, wait, ROWS_PER_PAGE_HINT)
        if not rows:
            return False, "No rows could be scraped from CoinMarketCap."
            
        df = pd.DataFrame(rows)
        # Use common save helper for dual output
        save_dataframe(df, base_name)
        return True, None
    except Exception as e:
        msg = f"CMC error: {e}"
        print(f"[CMC ERROR] {msg}")
        return False, msg

def main():
    """Standalone execution entry point."""
    driver = setup_driver(headless=False)
    try:
        ok, err = process_cmc_flows(driver)
        if ok: print("[STANDALONE] CMC processed successfully.")
        else: print(f"[STANDALONE] CMC failed: {err}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
