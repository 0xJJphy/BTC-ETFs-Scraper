import os
import time
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from core.utils.helpers import (
    polite_sleep, normalize_date_column, save_dataframe, _safe_remove,
    _try_click_any, setup_driver, CSV_DIR, JSON_DIR, SAVE_FORMAT
)

def accept_cookies_franklin(driver):
    """Handle cookie consent banner on the Franklin Templeton website."""
    return _try_click_any(driver, [
        "//button[@id='onetrust-accept-btn-handler']",
        "#onetrust-accept-btn-handler",
        "//button[contains(.,'Accept All')]",
        "//button[contains(.,'I Accept')]"
    ], wait_sec=10)

def find_pricing_xls_button_franklin(driver):
    """Find the XLS download button in the Pricing section of the Franklin page."""
    xps = [
        "//section[@id='pricing']//button[contains(., 'XLS')]",
        "//button[starts-with(@id,'pricingDownload')]",
        "//section[@id='pricing']//button[contains(@data-gtm-intent,'download_pricing')]",
        "//section[@id='pricing']//a[contains(.,'XLS')]",
    ]
    for xp in xps:
        try:
            el = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, xp)))
            return el
        except: pass
    return None

def parse_franklin_xlsx_to_df(xlsx_path):
    """Parse the downloaded Franklin Templeton XLSX file into a clean DataFrame."""
    raw = pd.read_excel(xlsx_path, sheet_name=0, header=None, dtype=str)
    header_idx = None
    for i in range(min(80, len(raw))):
        vals = [str(v).strip() for v in list(raw.iloc[i].fillna(""))]
        joined = "|".join(v.lower() for v in vals)
        if "date" in joined and "nav" in joined and "market price" in joined:
            header_idx = i
            break
    if header_idx is None:
        raise RuntimeError("Header not found in Franklin XLSX file")

    headers = [str(v).strip() for v in list(raw.iloc[header_idx].fillna(""))]
    data = raw.iloc[header_idx+1:].copy()
    data.columns = headers

    def _pick(cols, target):
        low = [str(c).strip().lower() for c in cols]
        t = target.lower()
        if t in low: return cols[low.index(t)]
        for i, l in enumerate(low):
            if t in l: return cols[i]
        return None

    cols = list(data.columns)
    date_col = _pick(cols, "Date")
    nav_col  = _pick(cols, "NAV")
    mkt_col  = _pick(cols, "Market Price")
    keep = [c for c in [date_col, nav_col, mkt_col] if c]
    df = data[keep].copy()
    df = df[~df[date_col].isna() & (df[date_col].astype(str).str.strip()!="")]

    rename = {}
    if date_col: rename[date_col] = "date"
    if nav_col:  rename[nav_col]  = "nav"
    if mkt_col:  rename[mkt_col]  = "market price"
    df = df.rename(columns=rename)

    raw_date = df["date"].astype(str).str.strip()
    dt = pd.to_datetime(raw_date, format="%m/%d/%Y", errors="coerce")
    if dt.isna().any():
        dt2 = pd.to_datetime(raw_date[dt.isna()], errors="coerce", infer_datetime_format=True)
        dt = dt.where(~dt.isna(), dt2)
    df["date"] = dt.dt.strftime("%Y%m%d").where(~dt.isna(), raw_date)

    for c in ["nav", "market price"]:
        if c in df.columns:
            df[c] = (df[c].astype(str)
                        .str.replace("$","", regex=False)
                        .str.replace(",","", regex=False)
                        .str.strip())
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def process_single_etf_franklin(driver, etf, site_url):
    """Main process to scrape a single Franklin Templeton ETF."""
    name = etf["name"]
    base = os.path.splitext(etf["output_filename"])[0]
    tmp_xlsx = os.path.join(CSV_DIR, base + "_tmp.xlsx")
    print(f"\n[ETF] Processing {name} (Franklin – Pricing XLS) → output .{SAVE_FORMAT}")
    print("="*50)

    try:
        driver.get(site_url); polite_sleep()
        accept_cookies_franklin(driver); polite_sleep()
    except Exception as e:
        print(f"[FRANKLIN] Navigation: {e}")

    try:
        el_section = WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.CSS_SELECTOR, "section#pricing")))
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el_section)
        polite_sleep()
    except Exception as e:
        print(f"[FRANKLIN] section#pricing not found: {e}")

    btn = find_pricing_xls_button_franklin(driver)
    if not btn:
        msg = "XLS button not found in Pricing section."
        print(f"[FRANKLIN] {msg}")
        return False, msg

    try:
        try: btn.click()
        except: driver.execute_script("arguments[0].click();", btn)
        start = time.time()
        download_dir = os.path.abspath(CSV_DIR)
        while time.time() - start < 45: # TIMEOUT
            files = [f for f in os.listdir(download_dir) if not f.endswith(".crdownload")]
            if files:
                newest = max(files, key=lambda f: os.path.getctime(os.path.join(download_dir, f)))
                pth = os.path.join(download_dir, newest)
                if os.path.getsize(pth) > 0 and pth.lower().endswith((".xlsx", ".xls")):
                    if os.path.exists(tmp_xlsx):
                        try: os.remove(tmp_xlsx)
                        except: pass
                    os.rename(pth, tmp_xlsx)
                    break
            time.sleep(1)
    except Exception as e:
        msg = f"Download error: {e}"
        print(f"[FRANKLIN] {msg}")
        return False, msg

    if not os.path.exists(tmp_xlsx):
        msg = "XLSX file not obtained."
        print(f"[FRANKLIN] {msg}")
        return False, msg

    try:
        df = parse_franklin_xlsx_to_df(tmp_xlsx)
        df = normalize_date_column(df)
        save_dataframe(df, base, sheet_name="Historical")
        _safe_remove(tmp_xlsx)
        print(f"[SUCCESS] ✓ Franklin processed ({name})")
        return True, None
    except Exception as e:
        msg = f"XLSX processing error: {e}"
        print(f"[FRANKLIN] {msg}")
        _safe_remove(tmp_xlsx)
        return False, msg

def main():
    """Standalone execution for Franklin scraper."""
    etf = {"name": "Franklin Bitcoin ETF (EZBC)", "output_filename": "ezbc_dailynav.xlsx"}
    site_url = "https://www.franklintempleton.com/investments/options/exchange-traded-funds/products/39639/SINGLCLASS/franklin-bitcoin-etf/EZBC"
    
    driver = setup_driver(headless=False)
    try:
        driver.get(site_url)
        polite_sleep()
        accept_cookies_franklin(driver)
        ok, err = process_single_etf_franklin(driver, etf, site_url)
        if ok:
            print("[STANDALONE] Franklin processed successfully.")
        else:
            print(f"[STANDALONE] Franklin failed: {err}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
