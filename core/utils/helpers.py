# helpers.py
# -------------------------------------------------------
# Common utilities for the BTC ETF Scraper project
# -------------------------------------------------------

import os
import re
import time
import random
import datetime
import requests
import pandas as pd
import json
import glob
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlparse, urlencode, quote
from openpyxl import load_workbook
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from subprocess import DEVNULL
import yfinance as yf

# ======================== BASE CONFIGURATION ========================

# Default output directory for individual ETF CSVs
OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../etfs_data/DATA_SCRAPPED/ETF-DIRECT-NAV/csv"))
HEADLESS   = False
TIMEOUT    = 45

# --- Script-editable variable (3rd priority) ---
SAVE_FORMAT_SETTING = "csv"   # "xlsx" or "csv"

# --- Throttling/Backoff (adjustable via ENV) ---
REQUEST_BASE_DELAY = float(os.getenv("ETF_REQUEST_DELAY", "3.0"))
REQUEST_JITTER     = float(os.getenv("ETF_REQUEST_JITTER", "2.0"))
MAX_RETRIES        = int(os.getenv("ETF_MAX_RETRIES", "5"))
BACKOFF_BASE       = float(os.getenv("ETF_BACKOFF_BASE", "2.0"))
BACKOFF_MAX        = float(os.getenv("ETF_BACKOFF_MAX", "60"))

# Final SAVE_FORMAT (ENV and CLI have priority)
SAVE_FORMAT = SAVE_FORMAT_SETTING
_env_fmt = os.environ.get("ETF_SAVE_FORMAT", "").lower().strip()
if _env_fmt in ("csv", "xlsx"):
    SAVE_FORMAT = _env_fmt

def polite_sleep():
    """Adds a random delay between requests to avoid being blocked."""
    time.sleep(max(0.0, REQUEST_BASE_DELAY + random.uniform(0, REQUEST_JITTER)))

def _retry_after_seconds(val):
    """Parses Retry-After header which can be seconds or a date string."""
    if not val: return None
    try:
        return float(val)
    except:
        try:
            dt = parsedate_to_datetime(val)
            now = datetime.datetime.now(datetime.timezone.utc)
            return max(0.0, (dt - now).total_seconds())
        except:
            return None

def _session_from_driver(driver):
    """Creates a requests Session with cookies inherited from the Selenium driver."""
    s = requests.Session()
    for c in driver.get_cookies():
        try:
            s.cookies.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path", "/"))
        except:
            s.cookies.set(c["name"], c["value"])
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    return s

def browser_fetch_text(driver, url, accept="application/json, text/plain, */*"):
    """Fetches text content from a URL inside the browser's context to use its session/headers."""
    js = """
    const url = arguments[0];
    const accept = arguments[1];
    const done = arguments[2];
    fetch(url, {credentials: 'include', headers: {'accept': accept}})
      .then(r => r.text())
      .then(t => done(t))
      .catch(e => done('ERROR:' + (e && e.message ? e.message : e)));
    """
    txt = driver.execute_async_script(js, url, accept)
    if isinstance(txt, str) and txt.startswith("ERROR:"):
        raise RuntimeError(txt)
    return txt

def setup_driver(headless=False):
    """Initializes a Chrome WebDriver with stealth settings and download preferences."""
    opts = Options()
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation","enable-logging"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    prefs = {
        "download.default_directory": OUTPUT_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    opts.add_experimental_option("prefs", prefs)
    try:
        service = Service(log_output=DEVNULL)
        d = webdriver.Chrome(options=opts, service=service)
    except TypeError:
        d = webdriver.Chrome(options=opts)
    d.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
    return d

def _try_click_any(driver, selectors, wait_sec=8):
    """Attempts to click any of the provided selectors (CSS or XPATH)."""
    for sel in selectors:
        try:
            by = By.CSS_SELECTOR if sel.startswith("#") else By.XPATH
            btn = WebDriverWait(driver, wait_sec).until(EC.element_to_be_clickable((by, sel)))
            try: btn.click()
            except: driver.execute_script("arguments[0].click();", btn)
            polite_sleep()
            return True
        except: pass
    return False

def _harvest_find_click_any(driver, selectors, by="css", wait=10, scroll=True, sleep_after=0.4):
    """Specific clicker helper for Harvest (and potentially others) with scroll options."""
    for sel in selectors:
        try:
            if by == "css":
                el = WebDriverWait(driver, wait).until(EC.element_to_be_clickable((By.CSS_SELECTOR, sel)))
            else:
                el = WebDriverWait(driver, wait).until(EC.element_to_be_clickable((By.XPATH, sel)))
            if scroll:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                time.sleep(0.2)
            try:
                el.click()
            except Exception:
                driver.execute_script("arguments[0].click();", el)
            time.sleep(sleep_after)
            return True
        except Exception:
            pass
    return False

def download_url_to_file(url, referer, output_path, accept="*/*", session=None):
    """Downloads a file from a URL using requests, with retry and backoff logic."""
    sess = session or requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": accept,
        "Referer": referer,
        "Origin": urlparse(referer).scheme + "://" + urlparse(referer).netloc if referer else "",
        "Accept-Language": "en-US,en;q=0.9",
    }
    for attempt in range(MAX_RETRIES):
        try:
            polite_sleep()
            r = sess.get(url, headers=headers, stream=True, timeout=60, allow_redirects=True)
            print(f"[HTTP] GET {url} status={r.status_code}")
            if r.status_code in (429, 403, 503):
                ra = _retry_after_seconds(r.headers.get("Retry-After"))
                wait = ra if ra is not None else min(BACKOFF_MAX, (BACKOFF_BASE ** attempt) + random.random())
                print(f"[BACKOFF] status={r.status_code} -> sleep {wait:.1f}s (attempt {attempt+1}/{MAX_RETRIES})")
                time.sleep(wait); continue
            r.raise_for_status()
            os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
            with open(output_path, "wb") as f:
                for ch in r.iter_content(8192):
                    if ch: f.write(ch)
            print(f"[DOWNLOAD] SUCCESS Saved: {output_path} ({os.path.getsize(output_path)} bytes)")
            return True
        except Exception as e:
            wait = min(BACKOFF_MAX, (BACKOFF_BASE ** attempt) + random.random())
            print(f"[DOWNLOAD] Error (attempt {attempt+1}/{MAX_RETRIES}): {e} -> sleep {wait:.1f}s")
            time.sleep(wait)
    return False

def _find_col(df, candidates):
    """Finds a column in a DataFrame that matches any of the candidate names (case-insensitive fuzzy match)."""
    cols = list(df.columns)
    low = [str(c).strip().lower() for c in cols]
    for cand in candidates:
        cand_l = cand.lower()
        if cand_l in low:
            return cols[low.index(cand_l)]
        for i, l in enumerate(low):
            if cand_l in l:
                return cols[i]
    return None

def normalize_date_column(df):
    """Normalizes the date column to YYYYMMDD format."""
    target = _find_col(df, ["date", "as of", "as_of"])
    if not target:
        return df
    try:
        dt = pd.to_datetime(df[target], errors="coerce", infer_datetime_format=True)
        df[target] = dt.dt.strftime("%Y%m%d").where(~dt.isna(), df[target])
    except:
        pass
    return df

def _safe_remove(path, tries=3, delay=0.4):
    """Safely removes a file with retries in case of locking."""
    for _ in range(tries):
        try:
            if os.path.exists(path):
                os.remove(path)
                print(f"[CLEANUP] Removed: {path}")
            return
        except PermissionError:
            time.sleep(delay)
        except:
            return

def _yf_close_by_date(ticker, start_yyyymmdd, end_yyyymmdd):
    """Fetches historical close prices from Yahoo Finance for a specific date range."""
    try:
        start_d = pd.to_datetime(start_yyyymmdd).date()
        end_d = (pd.to_datetime(end_yyyymmdd).date() + datetime.timedelta(days=1))
        y = yf.Ticker(ticker).history(start=start_d, end=end_d, interval="1d", auto_adjust=False)
        if y is None or y.empty:
            return pd.DataFrame(columns=["date","market price"])
        y = y.reset_index()
        date_col = "Date" if "Date" in y.columns else y.columns[0]
        y["date"] = pd.to_datetime(y[date_col]).dt.strftime("%Y%m%d")
        df_px = y[["date","Close"]].rename(columns={"Close":"market price"})
        df_px["market price"] = (df_px["market price"].astype(str)
                                 .str.replace("$","", regex=False)
                                 .str.replace(",","", regex=False)
                                 .str.strip())
        return df_px
    except:
        return pd.DataFrame(columns=["date","market price"])

def save_dataframe(df, base_name, sheet_name="Historical"):
    """Saves a DataFrame to the output directory in either CSV or XLSX format."""
    ext = SAVE_FORMAT
    out_path = os.path.join(OUTPUT_DIR, f"{base_name}.{ext}")
    if os.path.exists(out_path):
        try: os.remove(out_path)
        except: pass

    if ext == "xlsx":
        with pd.ExcelWriter(out_path, engine="openpyxl") as w:
            df.to_excel(w, sheet_name=sheet_name, index=False)
        print(f"[SAVE] SUCCESS XLSX saved: {out_path}")
    else:
        df.to_csv(out_path, index=False)
        print(f"[SAVE] SUCCESS CSV saved: {out_path}")
    return out_path
