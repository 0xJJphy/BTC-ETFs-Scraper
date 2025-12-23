import os
import time
import glob
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sys

# Add the project root to sys.path to allow absolute imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

try:
    from core.utils.helpers import (
        polite_sleep, save_dataframe, _safe_remove,
        _try_click_any, setup_driver, CSV_DIR, SAVE_FORMAT
    )
except ImportError:
    # Fallback for standalone execution if sys.path trick fails
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../utils")))
    from helpers import (
        polite_sleep, save_dataframe, _safe_remove,
        _try_click_any, setup_driver, CSV_DIR, SAVE_FORMAT
    )

def accept_cookies_bosera(driver):
    """Handles cookie consent and ensures the page is scrolled to trigger necessary elements."""
    try:
        driver.execute_script("window.scrollTo(0, 0);")
        for _ in range(10):
            driver.execute_script("window.scrollBy(0, Math.floor(window.innerHeight * 0.9));")
            time.sleep(0.25)
        time.sleep(0.8)
    except: pass

    if _try_click_any(driver, [
        "#onetrust-accept-btn-handler",
        "//button[@id='onetrust-accept-btn-handler']",
    ], wait_sec=12):
        return True

    if _try_click_any(driver, [
        "//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'accept')]",
        "//a[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'accept')]",
        "//button[contains(.,'同意') or contains(.,'接受') or contains(.,'同意全部')]",
    ], wait_sec=8):
        return True

    try:
        # Fallback to hide overlays via JS
        driver.execute_script("""
            (function(){
                var ids = ['onetrust-banner-sdk','onetrust-consent-sdk'];
                ids.forEach(function(id){
                    var el = document.getElementById(id);
                    if (el) { el.style.display='none'; }
                });
                var sel = document.querySelector('.onetrust-overlay, .ot-floating-button');
                if (sel) { sel.style.display='none'; }
            })();
        """)
        return True
    except:
        return False

def click_download_button_bosera(driver):
    """Locates and clicks the historical NAV download button on the Bosera website."""
    try:
        perf = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//h2[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'performance')]"))
        )
        driver.execute_script("arguments[0].scrollIntoView({block:'start'});", perf)
        time.sleep(0.4)
    except: pass

    xp = ("//a[contains(@href,'/api/fundinfo/exporthisnavexcel.do') "
          "and contains(@href,'fundCode=BTCL')]")
    css = "a[href*='/api/fundinfo/exporthisnavexcel.do'][href*='fundCode=BTCL']"

    try:
        el = WebDriverWait(driver, 12).until(EC.element_to_be_clickable((By.CSS_SELECTOR, css)))
        try: el.click()
        except: driver.execute_script("arguments[0].click();", el)
        return True
    except:
        try:
            el = WebDriverWait(driver, 12).until(EC.element_to_be_clickable((By.XPATH, xp)))
            try: el.click()
            except: driver.execute_script("arguments[0].click();", el)
            return True
        except:
            return False

def wait_for_download(download_dir: str, timeout: int = 60) -> str:
    """Waits for the Bosera file download to complete and stabilize."""
    end = time.time() + timeout
    last_size = None
    stable = 0
    while time.time() < end:
        xlsx = sorted(glob.glob(os.path.join(download_dir, "*.xlsx")), key=os.path.getmtime)
        xls  = sorted(glob.glob(os.path.join(download_dir, "*.xls")),  key=os.path.getmtime)
        temp = glob.glob(os.path.join(download_dir, "*.crdownload"))
        cand = (xlsx or xls)
        if cand:
            p = cand[-1]
            size = os.path.getsize(p)
            stable = stable + 1 if size == last_size else 0
            last_size = size
            if stable >= 2 and size > 0 and not temp:
                return p
        time.sleep(0.5)
    raise TimeoutError("Bosera download did not finish within the expected timeframe.")

def parse_bosera_usd_counter(xlsx_path: str) -> pd.DataFrame:
    """Parses the USD Counter sheet from the downloaded Bosera Excel file."""
    sheets = pd.read_excel(xlsx_path, sheet_name=None, header=None, dtype=str, engine="openpyxl")
    usd_name = None
    for nm in sheets.keys():
        if "usd" in str(nm).lower():
            usd_name = nm
            break
    if usd_name is None:
        usd_name = list(sheets.keys())[1] if len(sheets) > 1 else list(sheets.keys())[0]

    raw = sheets[usd_name].fillna("")
    header_idx = None
    for i in range(min(60, len(raw))):
        row_vals = [str(v).strip() for v in list(raw.iloc[i])]
        joined = "|".join(v.lower() for v in row_vals)
        if "date" in joined and ("market" in joined and "price" in joined) and "nav" in joined:
            header_idx = i
            break
    if header_idx is None:
        raise RuntimeError("Could not find header row (USD Counter) in Bosera file.")

    headers = [str(v).strip() for v in list(raw.iloc[header_idx])]
    df = raw.iloc[header_idx + 1:].copy()
    df.columns = headers

    def pick(colnames, targets):
        low = [str(c).strip().lower() for c in colnames]
        for t in targets:
            t_l = t.lower()
            if t_l in low: return colnames[low.index(t_l)]
            for i, l in enumerate(low):
                if t_l in l: return colnames[i]
        return None

    date_col = pick(df.columns, ["Date", "Date (yyyy/mm/dd)", "date (yyyy/mm/dd)"])
    nav_col  = pick(df.columns, ["NAV"])
    mkt_col  = pick(df.columns, ["Market Price", "Closing Market Price", "Market price"])

    keep = [c for c in [date_col, nav_col, mkt_col] if c]
    if len(keep) < 3:
        raise RuntimeError(f"Missing required columns in Bosera file: {list(df.columns)}")

    out = df[keep].copy()
    out = out[~out[date_col].astype(str).str.strip().eq("")]
    out = out.rename(columns={date_col: "date", nav_col: "nav", mkt_col: "market price"})

    raw_date = out["date"].astype(str).str.strip()
    dt = pd.to_datetime(raw_date, errors="coerce", infer_datetime_format=True)
    out["date"] = dt.dt.strftime("%Y%m%d").where(~dt.isna(), raw_date)

    for c in ["nav", "market price"]:
        out[c] = (out[c].astype(str)
                        .str.replace("$","", regex=False)
                        .str.replace(",","", regex=False)
                        .str.strip())

    out = out[out["nav"].str.match(r"^-?\d+(\.\d+)?$", na=False)]
    return out[["date","nav","market price"]].reset_index(drop=True)

def process_single_etf_bosera(driver, etf, site_url):
    """Processes historical data for Bosera ETF by downloading and parsing an Excel file."""
    name = etf["name"]
    base = os.path.splitext(etf["output_filename"])[0]
    print(f"\n[ETF] Processing {name} (Bosera - USD Counter XLSX) -> output .{SAVE_FORMAT}")
    print("="*50)

    try:
        driver.get(site_url); polite_sleep()
        accept_cookies_bosera(driver); polite_sleep()
    except Exception as e:
        print(f"[BOSERA] Navigation/cookies warning: {e}")

    if not click_download_button_bosera(driver):
        msg = "Could not click download button."
        print(f"[BOSERA] {msg}")
        return False, msg

    try:
        dl = wait_for_download(os.path.abspath(CSV_DIR), timeout=60)
        print(f"[BOSERA] SUCCESS Downloaded: {dl}")
        df = parse_bosera_usd_counter(dl)
        save_dataframe(df, base, sheet_name="USD Counter")
        _safe_remove(dl)
        print(f"[SUCCESS] Bosera processed ({name})")
        return True, None
    except Exception as e:
        msg = f"Bosera error: {e}"
        print(f"[BOSERA ERROR] {msg}")
        return False, msg

def main():
    """Standalone execution entry point."""
    etf = {"name": "Bosera HashKey Bitcoin ETF (BTCL)", "output_filename": "bosera_dailynav.xlsx"}
    site_url = "https://www.bosera.com.hk/en-US/products/fund/detail/BTCL"
    
    driver = setup_driver(headless=False)
    try:
        ok, err = process_single_etf_bosera(driver, etf, site_url)
        if ok:
            print("[STANDALONE] Bosera processed successfully.")
        else:
            print(f"[STANDALONE] Bosera failed: {err}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
