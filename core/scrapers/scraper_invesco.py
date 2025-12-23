import os
import time
import json
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from core.utils.helpers import (
    polite_sleep, _session_from_driver, download_url_to_file,
    normalize_date_column, save_dataframe, _safe_remove,
    _find_col, _try_click_any, _yf_close_by_date,
    browser_fetch_text, setup_driver,
    CSV_DIR, SAVE_FORMAT, TIMEOUT
)

def accept_cookies_invesco(driver):
    """Handle cookie consent banner on the Invesco website."""
    return _try_click_any(driver, [
        "//button[@id='onetrust-accept-btn-handler']",
        "#onetrust-accept-btn-handler",
        "//button[contains(.,'Accept All')]",
        "//button[contains(.,'I Accept')]"
    ], wait_sec=10)

def click_individual_investor_span(driver, timeout=10):
    """Confirm user role as 'Individual Investor' to access pricing data."""
    try:
        WebDriverWait(driver, 6).until(
            EC.presence_of_element_located((By.XPATH, "//*[contains(., 'Confirm your role') or contains(., 'Individual Investor')]"))
        )
    except:
        return False
    paths = [
        "//span[contains(@class,'audience-selection__list__item__label__text') and normalize-space()='Individual Investor']",
        "//*[@role='dialog']//span[contains(@class,'audience-selection__list__item__label__text') and normalize-space()='Individual Investor']",
    ]
    for xp in paths:
        try:
            el = WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, xp)))
            btn = driver.execute_script("return arguments[0].closest('button') || arguments[0];", el)
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            driver.execute_script("arguments[0].click();", btn)
            polite_sleep()
            return True
        except:
            continue
    return False

def _invesco_click_text_scrolling(driver, text, timeout=8, step=700):
    """Scroll and click on an element containing specific text."""
    text = text.lower()
    end = time.time() + timeout
    js = """
        const want = arguments[0];
        const nodes = Array.from(document.querySelectorAll(
          "a,button,[role='button'],[role='tab'],div[role='button'],[tabindex]"
        ));
        const vis = el => { const r=el.getBoundingClientRect(), s=getComputedStyle(el);
            return r.width>1&&r.height>1&&s.visibility!=='hidden'&&s.display!=='none';};
        for (const el of nodes){
          const t=(el.innerText||el.textContent||'').trim().toLowerCase();
          if (t.includes(want) && vis(el)){
            try{el.scrollIntoView({block:'center'});}catch(e){}
            try{el.click();}catch(e){el.dispatchEvent(new MouseEvent('click',{bubbles:true}));}
            return true;
          }
        }
        return false;
    """
    while time.time() < end:
        if driver.execute_script(js, text):
            polite_sleep()
            return True
        driver.execute_script("window.scrollBy(0, arguments[0]);", step)
        time.sleep(0.35)
    return False

def process_single_etf_invesco(driver, etf, site_url):
    """Main process to scrape a single Invesco ETF using API endpoints."""
    name = etf["name"]
    base = os.path.splitext(etf["output_filename"])[0]
    print(f"\n[ETF] Processing {name} (Invesco – API Price History) → output .{SAVE_FORMAT}")
    print("="*50)

    try:
        driver.get(site_url); polite_sleep()
        accept_cookies_invesco(driver)
        click_individual_investor_span(driver)
        
        # Navigate to Price History
        if not _invesco_click_text_scrolling(driver, "Price history", timeout=6):
            _invesco_click_text_scrolling(driver, "Prices", timeout=4, step=0)
            _invesco_click_text_scrolling(driver, "Price history", timeout=6)

        # Get NAVs via fetch in browser context
        cusip = "46091J101"
        api_url = f"https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/{cusip}/navs"
        qs = "idType=cusip&productType=ETF"
        
        txt = browser_fetch_text(driver, f"{api_url}?{qs}")
        js_data = json.loads(txt)
        
        lcd = (isinstance(js_data, dict) and (js_data.get("lineChartData") or js_data.get("linechartdata"))) or []
        if not lcd:
            return False, "No lineChartData in Invesco API response"
            
        line = None
        for it in lcd:
            label = str(it.get("type","") or it.get("label","")).upper()
            if "NAV" in label:
                line = it; break
        if not line:
            line = lcd[0]
            
        rows = []
        for r in line.get("data", []):
            d_val = r.get("date")
            v_val = r.get("value")
            if d_val and v_val is not None:
                try:
                    dt = pd.to_datetime(d_val)
                    rows.append({"date": dt.strftime("%Y%m%d"), "nav": float(v_val)})
                except: pass

        df = pd.DataFrame(rows).sort_values("date")
        if df.empty:
            return False, "No data extracted from Invesco API"

        # Add market price from Yahoo Finance
        start_d, end_d = df["date"].min(), df["date"].max()
        px = _yf_close_by_date("BTCO", start_d, end_d)
        if not px.empty:
            df = df.merge(px, on="date", how="left")
        else:
            df["market price"] = pd.NA

        save_dataframe(df, base, sheet_name="Price History")
        print(f"[SUCCESS] ✓ Invesco processed ({name})")
        return True, None

    except Exception as e:
        msg = f"Invesco logic failed: {e}"
        print(f"[ERROR] {msg}")
        return False, msg

def main():
    """Standalone execution for Invesco scraper."""
    etf = {"name": "Invesco Galaxy Bitcoin ETF (BTCO)", "output_filename": "btco_dailynav.xlsx"}
    site_url = "https://www.invesco.com/us/financial-products/etfs/product-detail?ticker=BTCO"
    
    driver = setup_driver(headless=False)
    try:
        ok, err = process_single_etf_invesco(driver, etf, site_url)
        if ok:
            print("[STANDALONE] Invesco processed successfully.")
        else:
            print(f"[STANDALONE] Invesco failed: {err}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
