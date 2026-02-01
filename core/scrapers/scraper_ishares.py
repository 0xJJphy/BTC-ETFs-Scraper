import os
import re
import time
import pandas as pd
import xml.etree.ElementTree as ET
from selenium.webdriver.common.by import By

from core.utils.helpers import (
    polite_sleep, _session_from_driver, download_url_to_file,
    normalize_date_column, save_dataframe, _safe_remove,
    _find_col, _try_click_any, _yf_close_by_date,
    setup_driver, CSV_DIR, JSON_DIR, SAVE_FORMAT, TIMEOUT,
    OUTPUT_BASE_DIR
)

_SS_NS = {'ss': 'urn:schemas-microsoft-com:office:spreadsheet'}

def accept_cookies_ishares(driver):
    """Handle cookie consent banner on the iShares website."""
    return _try_click_any(driver, [
        "//button[@id='onetrust-accept-btn-handler']",
        "#onetrust-accept-btn-handler",
        "//button[contains(text(),'Accept All')]",
        "//button[contains(text(),'Accept all')]"
    ], wait_sec=10)

def find_download_link_ishares(driver):
    """Locate the data download link on the iShares ETF page."""
    for xp in [
        "//a[contains(.,'Data Download')]",
        "//button[contains(.,'Data Download')]",
        "//a[contains(@href,'download')]"
    ]:
        try:
            el = driver.find_element(By.XPATH, xp)
            href = el.get_attribute("href") or ""
            if href:
                if "fileType=" in href:
                    href = re.sub(r'fileType=[^&]*', 'fileType=xls', href)
                else:
                    href += ("&" if "?" in href else "?") + "fileType=xls"
                return el, href
        except: pass
    return None, None

def _read_spreadsheetml_text(path):
    """Read the SpreadsheetML (XML-based XLS) file text content."""
    with open(path, "rb") as f: raw = f.read()
    i = raw.find(b"<")
    if i>0: raw = raw[i:]
    for enc in ("utf-8-sig","utf-8"):
        try: return raw.decode(enc)
        except: pass
    return raw.decode("utf-8", errors="ignore")

def parse_ishares_spreadsheetml_to_df(xls_path, target_sheet_index=2, drop_patterns=("Ex-Dividend","Ex Dividend")):
    """Parse iShares SpreadsheetML XLS into a clean DataFrame."""
    text = _read_spreadsheetml_text(xls_path)
    # Basic sanitize
    text = re.sub(r'\s+ss:HRef="[^"]*"', '', text)
    text = re.sub(r'\s+HRef="[^"]*"', '', text)
    text = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;|#\d+;)', '&amp;', text)
    text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', text)
    
    root = ET.fromstring(text)
    ws = root.findall(".//ss:Worksheet", _SS_NS) or root.findall(".//Worksheet")
    names = []
    for w in ws:
        names.append(w.get(f"{{{_SS_NS['ss']}}}Name") or w.get("Name") or "Sheet")
        
    if "Historical" in names:
        target_sheet_index = names.index("Historical")
    if target_sheet_index >= len(ws):
        target_sheet_index = 0
    sheet = ws[target_sheet_index]

    rows = sheet.findall(".//ss:Row", _SS_NS) or sheet.findall(".//Row")
    vals = []
    for r in rows:
        cells = r.findall(".//ss:Cell", _SS_NS) or r.findall(".//Cell")
        v = []
        for c in cells:
            d = c.findall(".//ss:Data", _SS_NS) or c.findall(".//Data")
            v.append((d[0].text or "").strip() if d else "")
        vals.append(v)

    hdr_idx = 0
    for i, r in enumerate(vals[:15]):
        j = "|".join(x.lower() for x in r)
        if "as of" in j and "nav per share" in j and "shares outstanding" in j:
            hdr_idx = i
            break
            
    headers = vals[hdr_idx]
    data = vals[hdr_idx+1:]
    fixed = []
    for r in data:
        if any(str(x).strip() for x in r):
            if len(r) < len(headers): r = r + [""]*(len(headers)-len(r))
            else: r = r[:len(headers)]
            fixed.append(r)
    df = pd.DataFrame(fixed, columns=headers)

    if drop_patterns:
        drops = []
        for pat in drop_patterns:
            drops += [c for c in df.columns if pat.lower() in str(c).lower()]
        if drops: df = df.drop(columns=drops, errors="ignore")

    ren = {}
    for c in df.columns:
        lc = str(c).lower().strip()
        if lc in ("as of", "as_of"): ren[c] = "date"
        elif lc == "nav per share": ren[c] = "nav"
        elif lc == "shares outstanding": ren[c] = "shares outstanding"
    return df.rename(columns=ren)

def process_single_etf_ishares(driver, etf, site_url):
    """Main process to scrape a single iShares ETF."""
    name = etf["name"]
    base = os.path.splitext(etf["output_filename"])[0]
    temp_xls = os.path.join(CSV_DIR, base + "_tmp.xls")
    print(f"\n[ETF] Processing {name} (iShares – SpreadsheetML 2003) → output .{SAVE_FORMAT}")
    print("="*50)

    el, href = find_download_link_ishares(driver)
    if not href:
        msg = "Download link not found for IBIT."
        return False, msg

    sess = _session_from_driver(driver)
    success = download_url_to_file(href, site_url, temp_xls,
                                   accept="application/vnd.ms-excel,application/octet-stream,*/*",
                                   session=sess)

    if not success and el:
        print("[INFO] Attempting with Selenium click...")
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            polite_sleep()
            try: el.click()
            except: driver.execute_script("arguments[0].click();", el)
            start = time.time()
            while time.time() - start < TIMEOUT:
                files = [f for f in os.listdir(CSV_DIR) if not f.endswith(".crdownload")]
                if files:
                    newest = max(files, key=lambda f: os.path.getctime(os.path.join(CSV_DIR, f)))
                    pth = os.path.join(CSV_DIR, newest)
                    if os.path.getsize(pth) > 0 and pth.lower().endswith(".xls"):
                        if os.path.exists(temp_xls): os.remove(temp_xls)
                        os.rename(pth, temp_xls)
                        success = True
                        break
                time.sleep(1)
        except Exception as e:
            print(f"[ERROR] Selenium download: {e}")

    if not success or not os.path.exists(temp_xls):
        return False, "Could not obtain iShares .xls file."

    try:
        df = parse_ishares_spreadsheetml_to_df(temp_xls)
        
        # Normalize
        target_date = _find_col(df, ["date"])
        if target_date:
            df["date"] = pd.to_datetime(df[target_date], errors="coerce").dt.strftime("%Y%m%d")
            
        for c in ["nav", "shares outstanding"]:
            col = _find_col(df, [c])
            if col:
                df[col] = (df[col].astype(str).str.replace("$","",regex=False).str.replace(",","",regex=False).str.strip())
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Add market price (Yahoo Finance)
        date_col = _find_col(df, ["date"])
        if date_col:
            start_d, end_d = df[date_col].min(), df[date_col].max()
            px = _yf_close_by_date("IBIT", start_d, end_d)
            if not px.empty:
                df = df.merge(px, on=date_col, how="left")

        # Cleanup columns
        cols = ["date", "nav", "market price", "shares outstanding"]
        keep = [c for c in cols if c in df.columns]
        df = df[keep]

        save_dataframe(df, base, sheet_name="Historical")
        _safe_remove(temp_xls)
        print(f"[SUCCESS] ✓ iShares processed ({name})")
        return True, None

    except Exception as e:
        msg = f"SpreadsheetML processing failed: {e}"
        print(f"[ERROR] {msg}")
        # Diagnostic: Screen on error
        try:
            shot_err = os.path.join(OUTPUT_BASE_DIR, f"debug_ishares_error_{int(time.time())}.png")
            driver.save_screenshot(shot_err)
            print(f"[ISHARES] Error screenshot saved: {shot_err}")
        except: pass
        _safe_remove(temp_xls)
        return False, msg

def main():
    """Standalone execution for iShares scraper."""
    etf = {
        "name": "iShares Bitcoin Trust ETF",
        "search_terms": ["Data Download","Download","IBIT"],
        "output_filename": "ibit_dailynav.xlsx",
        "process_config": {"sheet_to_keep": 2, "columns_to_remove": ["Ex-Dividend","Ex-Dividends"]}
    }
    site_url = "https://www.ishares.com/us/products/333011/ishares-bitcoin-trust-etf"
    
    driver = setup_driver(headless=False)
    try:
        driver.get(site_url)
        polite_sleep()
        accept_cookies_ishares(driver)
        ok, err = process_single_etf_ishares(driver, etf, site_url)
        if ok:
            print("[STANDALONE] iShares processed successfully.")
        else:
            print(f"[STANDALONE] iShares failed: {err}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
