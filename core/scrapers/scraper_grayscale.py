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
    _find_col, _try_click_any, setup_driver, OUTPUT_DIR, SAVE_FORMAT, TIMEOUT
)

def accept_cookies_grayscale(driver):
    """Handle cookie consent banner on the Grayscale website."""
    return _try_click_any(driver, [
        "//button[contains(@id,'CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll')]",
        "//button[contains(text(),'Allow all')]",
        "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll"
    ], wait_sec=10)

def find_etf_row_grayscale(driver, etf):
    """Find the specific ETF row in the Grayscale resources table."""
    terms = etf["search_terms"]
    xps = []
    for t in terms:
        xps += [f"//tr[contains(.,'{t}')]", f"//table//tr[contains(.,'{t}')]",
                f"//tr[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'{t.lower()}')]"]
    for xp in xps:
        try:
            rows = driver.find_elements(By.XPATH, xp)
            for r in rows:
                txt = (r.text or "").lower()
                if any(t.lower() in txt for t in terms):
                    return r
        except: pass
    return None

def find_xlsx_link_in_row(driver, row):
    """Locate the Excel download link within a specific table row."""
    for xp in [".//a[contains(@href,'xls')]",
               ".//a[contains(@href,'xlsx')]",
               ".//a[contains(text(),'Excel')]",
               ".//a[contains(@class,'download')]",
               ".//td//a[contains(@href,'download')]"]:
        try:
            links = row.find_elements(By.XPATH, xp)
            for a in links:
                href = a.get_attribute("href") or ""
                if href:
                    return a, href
        except: pass
    return None, None

def standardize_grayscale(df):
    """Standardize column names for Grayscale DataFrames."""
    ticker_cols = [c for c in df.columns if "ticker" in str(c).lower()]
    if ticker_cols:
        df = df.drop(columns=ticker_cols, errors="ignore")

    date_col   = _find_col(df, ["date", "as of", "as_of"])
    nav_col    = _find_col(df, ["nav per share", "nav"])
    shares_col = _find_col(df, ["shares outstanding"])
    mkt_col    = _find_col(df, ["market price per share", "market price"])

    ordered = [c for c in [date_col, nav_col, mkt_col, shares_col ] if c and c in df.columns]
    if not ordered:
        return df
    df = df[ordered].copy()
    rename_map = {}
    if date_col:   rename_map[date_col]   = "date"
    if nav_col:    rename_map[nav_col]    = "nav"
    if shares_col: rename_map[shares_col] = "shares outstanding"
    if mkt_col:    rename_map[mkt_col]    = "market price"
    return df.rename(columns=rename_map)

def process_single_etf_grayscale(driver, etf, site_url):
    """Main process to scrape a single Grayscale ETF."""
    name = etf["name"]
    base = os.path.splitext(etf["output_filename"])[0]
    tmp_source = os.path.join(OUTPUT_DIR, base + "_source.xlsx")
    print(f"\n[ETF] Processing {name} (Grayscale)  → output .{SAVE_FORMAT}")
    print("="*50)

    from_row = find_etf_row_grayscale(driver, etf)
    if not from_row:
        msg = "ETF not found in table."
        print(f"[ERROR] {msg}")
        return False, msg
    link, href = find_xlsx_link_in_row(driver, from_row)
    if not link or not href:
        msg = "XLSX link not found in row."
        print(f"[ERROR] {msg}")
        return False, msg
    if not href.startswith("http"):
        href = urljoin(site_url, href)

    try:
        session = _session_from_driver(driver)
        ok = download_url_to_file(
            href, site_url, tmp_source,
            accept="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel,*/*",
            session=session
        )
        if not ok:
            raise RuntimeError("Direct download session failed")
        print(f"[DOWNLOAD] Grayscale → {tmp_source}")
    except Exception as e:
        print(f"[DOWNLOAD] Direct download failed ({e}), attempting Selenium click…")
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", link)
        polite_sleep()
        try: link.click()
        except: driver.execute_script("arguments[0].click();", link)
        start = time.time()
        tmp_source_dl = None
        while time.time() - start < TIMEOUT:
            files = [f for f in os.listdir(os.path.abspath(OUTPUT_DIR)) if not f.endswith(".crdownload")]
            if files:
                newest = max(files, key=lambda f: os.path.getctime(os.path.join(os.path.abspath(OUTPUT_DIR), f)))
                pth = os.path.join(os.path.abspath(OUTPUT_DIR), newest)
                if os.path.getsize(pth) > 0:
                    tmp_source_dl = pth; break
            time.sleep(1)
        if not tmp_source_dl:
            msg = "Could not download from Grayscale."
            print(f"[ERROR] {msg}")
            return False, msg
        tmp_source = tmp_source_dl

    cfg = etf.get("process_config", {})
    try:
        with pd.ExcelFile(tmp_source, engine="openpyxl") as xls:
            sheets = xls.sheet_names
            idx = cfg.get("sheet_to_keep", 0);  idx = idx if idx < len(sheets) else 0
            sheet = sheets[idx]
            df = pd.read_excel(xls, sheet_name=sheet)

        keep = cfg.get("columns_to_keep") or []
        drop = cfg.get("columns_to_remove") or []
        if keep:
            sel = []
            for t in keep:
                if t in df.columns: sel.append(t)
                else:
                    low = t.lower()
                    sim = next((c for c in df.columns if low in str(c).lower() or str(c).lower() in low), None)
                    if sim: sel.append(sim)
            df_out = df[sel] if sel else df
        else:
            df_out = df
        if drop:
            to_drop = [c for c in df_out.columns if any(p.lower() in str(c).lower() for p in drop)]
            if to_drop:
                df_out = df_out.drop(columns=to_drop)

        df_out = standardize_grayscale(df_out)
        df_out = normalize_date_column(df_out)

        save_dataframe(df_out, base, sheet_name="Historical")
        _safe_remove(tmp_source)

        print(f"[SUCCESS] ✓ Grayscale processed ({name})")
        return True, None
    except Exception as e:
        msg = f"Grayscale processing error: {e}"
        print(f"[ERROR] {msg}")
        _safe_remove(tmp_source)
        return False, msg

def main():
    """Standalone execution for Grayscale scraper."""
    etf = {
        "name": "Grayscale Bitcoin Trust ETF",
        "search_terms": ["Bitcoin Trust ETF","GBTC","Bitcoin Trust"],
        "output_filename": "gbtc_dailynav.xlsx",
        "process_config": {"sheet_to_keep": 0, "columns_to_keep": ["OTC Ticker","Date","Shares Outstanding","NAV Per Share","Market Price Per Share"]}
    }
    site_url = "https://www.grayscale.com/resources"
    
    driver = setup_driver(headless=False)
    try:
        driver.get(site_url)
        polite_sleep()
        accept_cookies_grayscale(driver)
        ok, err = process_single_etf_grayscale(driver, etf, site_url)
        if ok:
            print("[STANDALONE] Grayscale processed successfully.")
        else:
            print(f"[STANDALONE] Grayscale failed: {err}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
