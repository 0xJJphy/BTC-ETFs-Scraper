import os
import time
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from core.utils.helpers import (
    polite_sleep, normalize_date_column, save_dataframe,
    _try_click_any, setup_driver, SAVE_FORMAT
)

def accept_cookies_wisdomtree(driver):
    """Handle cookie consent banner on the WisdomTree website."""
    _ = _try_click_any(driver, [
        "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
        "//a[@id='CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll']",
        "//button[@id='CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll']",
        "//a[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'allow all')]",
        "//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'allow all')]",
    ], wait_sec=10)
    polite_sleep()

    clicked = _try_click_any(driver, [
        "//a[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'continue to us website')]",
        "//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'continue to us website')]",
        "a.button-link.login-btn.button.-secondary.continue-btn",
        "a.button.-secondary.continue-btn",
    ], wait_sec=8)
    polite_sleep()
    return clicked

def _wisdomtree_open_history_modal(driver):
    """Navigate and open the Premium/Discount History modal on the WisdomTree page."""
    try:
        sec = WebDriverWait(driver, 12).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.table-container, .section.details-section"))
        )
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", sec)
        time.sleep(0.4)
    except: pass

    link_css = ["a.fund-modal-trigger[data-href*='nav-premium-discount-history']", "a[data-href*='nav-premium-discount-history']"]
    link_xp = ["//a[contains(@data-href,'nav-premium-discount-history')]", "//a[contains(.,'Premium/Discount History')]"]

    if not _try_click_any(driver, link_css, wait_sec=10):
        if not _try_click_any(driver, link_xp, wait_sec=10):
            raise RuntimeError("WisdomTree: History link not found")

    WebDriverWait(driver, 12).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div.modal.fade.in, div.modal.show, .modal")))
    WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'modal')]//table//tr")))
    time.sleep(0.5)

def _wisdomtree_parse_table(driver):
    """Parse the data table within the WisdomTree modal into a clean DataFrame."""
    rows = driver.find_elements(By.XPATH, "//div[contains(@class,'modal')]//table//tr")
    if not rows or len(rows) < 2: raise RuntimeError("WisdomTree: Modal table is empty")
    data = []
    for r in rows[1:]:
        cols = r.find_elements(By.XPATH, ".//td")
        if len(cols) < 3: continue
        data.append([cols[0].text.strip(), cols[1].text.strip(), cols[2].text.strip()])
    if not data: raise RuntimeError("WisdomTree: No data rows found")
    df = pd.DataFrame(data, columns=["date","nav","market price"])
    df = normalize_date_column(df)
    for c in ["nav","market price"]:
        df[c] = (df[c].astype(str).str.replace("$","", regex=False).str.replace(",","", regex=False).str.strip())
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df[["date","nav","market price"]]

def process_single_etf_wisdomtree(driver, etf, site_url):
    """Main process to scrape a single WisdomTree ETF."""
    name = etf["name"]
    base = os.path.splitext(etf["output_filename"])[0]
    print(f"\n[ETF] Processing {name} (WisdomTree – Modal NAV/Px/Discount) → output .{SAVE_FORMAT}")
    print("="*50)
    try:
        driver.get(site_url); polite_sleep()
        accept_cookies_wisdomtree(driver); polite_sleep()
        _wisdomtree_open_history_modal(driver)
        df = _wisdomtree_parse_table(driver)
        save_dataframe(df, base, sheet_name="Historical")
        print(f"[SUCCESS] ✓ WisdomTree processed ({name})")
        return True, None
    except Exception as e:
        msg = f"Error: {e}"
        print(f"[WISDOMTREE ERROR] {msg}")
        return False, msg

def main():
    """Standalone execution for WisdomTree scraper."""
    etf = {"name": "WisdomTree Bitcoin Fund (BTCW)", "output_filename": "btcw_dailynav.xlsx"}
    site_url = "https://www.wisdomtree.com/investments/etfs/crypto/btcw"
    
    driver = setup_driver(headless=False)
    try:
        ok, err = process_single_etf_wisdomtree(driver, etf, site_url)
        if ok:
            print("[STANDALONE] WisdomTree processed successfully.")
        else:
            print(f"[STANDALONE] WisdomTree failed: {err}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
