import os
import time
import zipfile
import pandas as pd
import xml.etree.ElementTree as ET
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from core.utils.helpers import (
    polite_sleep, normalize_date_column, save_dataframe, _safe_remove,
    _try_click_any, setup_driver, CSV_DIR, SAVE_FORMAT
)

_XLSX_NS = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
            "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships"}

def _col_letters_to_idx(letters):
    """Convert Excel column letters (e.g., 'A', 'AB') to a 0-based index."""
    idx = 0
    for ch in letters:
        idx = idx * 26 + (ord(ch) - 64)
    return idx - 1

def _xlsx_read_rows_basic(xlsx_path):
    """Low-level reading of an XLSX file using basic XML parsing to avoid openpyxl issues."""
    rows = []
    with zipfile.ZipFile(xlsx_path) as z:
        wb = ET.fromstring(z.read("xl/workbook.xml"))
        first_sheet = wb.find(".//x:sheets/x:sheet", _XLSX_NS)
        rid = first_sheet.attrib["{"+_XLSX_NS["r"]+"}id"]
        rels = ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))
        target = None
        for r in rels.findall(".//{http://schemas.openxmlformats.org/package/2006/relationships}Relationship"):
            if r.attrib.get("Id") == rid:
                target = r.attrib["Target"]
                break
        sheet_path = "xl/" + (target if target else "worksheets/sheet1.xml")
        shared = []
        if "xl/sharedStrings.xml" in z.namelist():
            sst = ET.fromstring(z.read("xl/sharedStrings.xml"))
            for si in sst.findall(".//x:si", _XLSX_NS):
                parts = [t.text or "" for t in si.findall(".//x:t", _XLSX_NS)]
                shared.append("".join(parts))
        sh = ET.fromstring(z.read(sheet_path))
        for row in sh.findall(".//x:sheetData/x:row", _XLSX_NS):
            vals = []
            for c in row.findall("x:c", _XLSX_NS):
                ref = c.attrib.get("r", "")
                letters = "".join([ch for ch in ref if ch.isalpha()]) or "A"
                idx = _col_letters_to_idx(letters)
                while len(vals) <= idx:
                    vals.append("")
                t = c.attrib.get("t")
                v = c.find("x:v", _XLSX_NS)
                is_t = c.find("x:is/x:t", _XLSX_NS)
                if t == "s" and v is not None:
                    try:
                        vals[idx] = shared[int(v.text)]
                    except:
                        vals[idx] = ""
                elif t == "inlineStr" and is_t is not None:
                    vals[idx] = is_t.text or ""
                else:
                    vals[idx] = (v.text if v is not None else "") or ""
            rows.append(vals)
    return rows

def accept_cookies_fidelity(driver):
    """Handle cookie consent banner on the Fidelity website."""
    clicked = _try_click_any(driver, [
        "//button[@id='onetrust-accept-btn-handler']",
        "#onetrust-accept-btn-handler",
        "//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'accept all')]",
        "//button[contains(.,'I Accept')]",
        "//button[contains(.,'Accept all cookies')]",
        "//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'continue')]",
    ], wait_sec=12)
    if not clicked:
        try:
            driver.execute_script("""
                const ids=['onetrust-consent-sdk','onetrust-banner-sdk'];
                ids.forEach(id=>{ const el=document.getElementById(id); if(el){el.style.display='none';}});
                const s=document.querySelector('.onetrust-pc-dark-filter'); if(s){s.style.display='none';}
            """)
            polite_sleep()
            return True
        except:
            return False
    return True

def find_download_button_fidelity(driver):
    """Find the XLSX download button on the Fidelity page."""
    xps = [
        "//button[@data-action='download_xlsx_historical']",
        "//button[contains(.,'Download XLSX')]",
    ]
    for xp in xps:
        try:
            el = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH, xp)))
            return el
        except: pass
    return None

def parse_fidelity_xlsx_to_df(xlsx_path):
    """Parse the downloaded Fidelity XLSX file into a clean DataFrame."""
    try:
        raw = pd.read_excel(xlsx_path, sheet_name=0, header=None, dtype=str, engine="openpyxl")
    except Exception:
        rows = _xlsx_read_rows_basic(xlsx_path)
        raw = pd.DataFrame(rows, dtype=str)

    header_idx = None
    for i in range(min(200, len(raw))):
        vals = [str(v).strip() for v in list(raw.iloc[i].fillna(""))]
        joined = "|".join(v.lower() for v in vals)
        if "date" in joined and "nav" in joined and ("market pr" in joined or "market price" in joined):
            header_idx = i
            break
    if header_idx is None:
        raise RuntimeError("Header not found in Fidelity XLSX file")

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
    mkt_col  = _pick(list(data.columns), ["Market price", "Market pr"])
    keep = [c for c in [date_col, nav_col, mkt_col] if c]
    df = data[keep].copy()
    df = df[~df[date_col].isna() & (df[date_col].astype(str).str.strip()!="")]

    df = df.rename(columns={date_col: "date", nav_col: "nav", mkt_col: "market price"})
    raw_date = df["date"].astype(str).str.strip()
    dt = pd.to_datetime(raw_date, errors="coerce", dayfirst=True)
    if dt.isna().any():
        dt2 = pd.to_datetime(raw_date[dt.isna()], errors="coerce")
        dt = dt.where(~dt.isna(), dt2)
    df["date"] = dt.dt.strftime("%Y%m%d").where(~dt.isna(), raw_date)

    for c in ["nav", "market price"]:
        if c in df.columns:
            df[c] = (df[c].astype(str)
                        .str.replace("$","", regex=False)
                        .str.replace(",","", regex=False)
                        .str.strip())
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df[["date","nav","market price"]]

def process_single_etf_fidelity(driver, etf, site_url):
    """Main process to scrape a single Fidelity ETF."""
    name = etf["name"]
    base = os.path.splitext(etf["output_filename"])[0]
    tmp_xlsx = os.path.join(CSV_DIR, base + "_tmp.xlsx")
    print(f"\n[ETF] Processing {name} (Fidelity – Download XLSX) → output .{SAVE_FORMAT}")
    print("="*50)

    try:
        driver.get(site_url); polite_sleep()
        accept_cookies_fidelity(driver); polite_sleep()
    except Exception as e:
        print(f"[FIDELITY] Navigation: {e}")

    accept_cookies_fidelity(driver); polite_sleep()

    btn = find_download_button_fidelity(driver)
    if not btn:
        msg = "Download XLSX button not found."
        print(f"[FIDELITY] {msg}")
        return False, msg

    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
        polite_sleep()
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
        print(f"[FIDELITY] {msg}")
        return False, msg

    if not os.path.exists(tmp_xlsx):
        msg = "XLSX file not obtained."
        print(f"[FIDELITY] {msg}")
        return False, msg

    try:
        df = parse_fidelity_xlsx_to_df(tmp_xlsx)
        df = normalize_date_column(df)
        save_dataframe(df, base, sheet_name="Historical")
        _safe_remove(tmp_xlsx)
        print(f"[SUCCESS] ✓ Fidelity processed ({name})")
        return True, None
    except Exception as e:
        msg = f"XLSX processing error: {e}"
        print(f"[FIDELITY] {msg}")
        _safe_remove(tmp_xlsx)
        return False, msg

def main():
    """Standalone execution for Fidelity scraper."""
    etf = {"name": "Fidelity Advantage Bitcoin ETF (FBTC)", "output_filename": "fbtc_dailynav.xlsx"}
    from datetime import datetime
    today = datetime.now().strftime("%d-%b-%Y")
    site_url = f"https://www.fidelity.ca/en/historical-prices/?historical=FBTC&sales-option=L&starting-date=11-Jan-2024&ending-date={today}"
    
    driver = setup_driver(headless=False)
    try:
        driver.get(site_url)
        polite_sleep()
        accept_cookies_fidelity(driver)
        ok, err = process_single_etf_fidelity(driver, etf, site_url)
        if ok:
            print("[STANDALONE] Fidelity processed successfully.")
        else:
            print(f"[STANDALONE] Fidelity failed: {err}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
