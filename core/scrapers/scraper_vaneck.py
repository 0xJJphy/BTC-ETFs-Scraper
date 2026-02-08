import os
import time
import pandas as pd
from urllib.parse import urljoin
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from core.utils.helpers import (
    polite_sleep, _session_from_driver, download_url_to_file,
    normalize_date_column, save_dataframe, _safe_remove,
    _try_click_any, setup_driver, CSV_DIR, JSON_DIR, SAVE_FORMAT, TIMEOUT
)

def accept_cookies_vaneck(driver):
    """Handle cookie consent banner on the VanEck website."""
    return _try_click_any(driver, [
        "//button[@id='onetrust-accept-btn-handler']",
        "#onetrust-accept-btn-handler",
        "//button[contains(.,'Accept All')]",
        "//button[contains(.,'I Accept')]",
        "//button[contains(.,'Agree')]"
    ], wait_sec=10)

def find_download_link_vaneck(driver):
    """Locate the historical prices download link on the VanEck HODL page."""
    xps = [
        "//a[contains(@href,'/investments/bitcoin-etf-hodl/downloads/fundhistoprices')]",
        "//a[contains(.,'XLS') and contains(@class,'download')]"
    ]
    for xp in xps:
        try:
            el = WebDriverWait(driver, 12).until(EC.element_to_be_clickable((By.XPATH, xp)))
            href = el.get_attribute("href") or ""
            return el, href
        except: pass
    return None, None

def parse_vaneck_hodl_xlsx_to_df(xlsx_path):
    """Parse the downloaded VanEck HODL XLSX file into a clean DataFrame."""
    raw = pd.read_excel(xlsx_path, sheet_name=0, header=None, dtype=str)
    header_idx = None
    for i in range(min(120, len(raw))):
        vals = [str(v).strip() for v in list(raw.iloc[i].fillna(""))]
        j = "|".join(v.lower() for v in vals)
        if "date" in j and "nav" in j and ("last trade" in j or "last price" in j):
            header_idx = i
            break
    if header_idx is None:
        raise RuntimeError("Header not found in VanEck XLSX file")

    headers = [str(v).strip() for v in list(raw.iloc[header_idx].fillna(""))]
    data = raw.iloc[header_idx+1:].copy()
    data.columns = headers

    def _pick(cols, targets):
        low = [str(c).strip().lower() for c in cols]
        for t in targets:
            t_l = t.lower()
            if t_l in low: return cols[low.index(t_l)]
            for i, l in enumerate(low):
                if t_l in l: return cols[i]
        return None

    date_col = _pick(list(data.columns), ["Date"])
    nav_col  = _pick(list(data.columns), ["NAV"])
    last_col = _pick(list(data.columns), ["Last Trade", "Last Price"])
    keep = [c for c in [date_col, nav_col, last_col] if c]
    df = data[keep].copy()
    df = df[~df[date_col].isna() & (df[date_col].astype(str).str.strip()!="")]

    df = df.rename(columns={date_col:"date", nav_col:"nav", last_col:"market price"})

    raw_date = df["date"].astype(str).str.strip()
    dt = pd.to_datetime(raw_date, errors="coerce")
    df["date"] = dt.dt.strftime("%Y%m%d").where(~dt.isna(), raw_date)

    for c in ["nav", "market price"]:
        if c in df.columns:
            df[c] = (df[c].astype(str)
                        .str.replace("$","", regex=False)
                        .str.replace(",","", regex=False)
                        .str.strip())
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df[["date","nav","market price"]]

def process_single_etf_vaneck(driver, etf, site_url):
    """Main process to scrape a single VanEck ETF."""
    name = etf["name"]
    base = os.path.splitext(etf["output_filename"])[0]
    tmp_xlsx = os.path.join(CSV_DIR, base + "_tmp.xlsx")
    print(f"\n[ETF] Processing {name} (VanEck – HODL XLSX) → output .{SAVE_FORMAT}")
    print("="*50)

    try:
        driver.get(site_url); polite_sleep()
        accept_cookies_vaneck(driver); polite_sleep()
    except Exception as e:
        print(f"[VANECK] Navigation: {e}")

    el, href = find_download_link_vaneck(driver)
    if not href:
        href = "/us/en/investments/bitcoin-etf-hodl/downloads/fundhistoprices/"
    if not href.startswith("http"):
        href = urljoin("https://www.vaneck.com", href)

    session = _session_from_driver(driver)
    ok = download_url_to_file(href, site_url, tmp_xlsx,
                              accept="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel,*/*",
                              session=session)
    if not ok and el:
        print("[VANECK] Attempting Selenium download...")
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            polite_sleep()
            try: el.click()
            except: driver.execute_script("arguments[0].click();", el)
            start = time.time()
            while time.time() - start < TIMEOUT:
                files = [f for f in os.listdir(os.path.abspath(CSV_DIR)) if not f.endswith(".crdownload")]
                if files:
                    newest = max(files, key=lambda f: os.path.getctime(os.path.join(CSV_DIR, f)))
                    pth = os.path.join(CSV_DIR, newest)
                    if os.path.getsize(pth) > 0 and pth.lower().endswith((".xlsx",".xls")):
                        if os.path.exists(tmp_xlsx):
                            try: os.remove(tmp_xlsx)
                            except: pass
                        os.rename(pth, tmp_xlsx)
                        ok = True
                        break
                time.sleep(1)
        except Exception as e:
            print(f"[VANECK] Selenium download: {e}")

    if not ok or not os.path.exists(tmp_xlsx):
        msg = "XLSX file not obtained."
        print(f"[VANECK] {msg}")
        return False, msg

    try:
        df = parse_vaneck_hodl_xlsx_to_df(tmp_xlsx)
        df = normalize_date_column(df)
        save_dataframe(df, base, sheet_name="Historical")
        _safe_remove(tmp_xlsx)
        print(f"[SUCCESS] ✓ VanEck processed ({name})")
        return True, None
    except Exception as e:
        msg = f"XLSX processing error: {e}"
        print(f"[VANECK] {msg}")
        _safe_remove(tmp_xlsx)
        return False, msg

def main():
    """Standalone execution for VanEck scraper."""
    etf = {"name": "VanEck Bitcoin ETF (HODL)", "output_filename": "hodl_dailynav.xlsx"}
    site_url = "https://www.vaneck.com/us/en/investments/bitcoin-etf-hodl/performance/"
    
    driver = setup_driver(headless=False)
    try:
        driver.get(site_url)
        polite_sleep()
        accept_cookies_vaneck(driver)
        ok, err = process_single_etf_vaneck(driver, etf, site_url)
        if ok:
            print("[STANDALONE] VanEck processed successfully.")
        else:
            print(f"[STANDALONE] VanEck failed: {err}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
