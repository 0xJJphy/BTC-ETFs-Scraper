# helpers.py
# -------------------------------------------------------
# Common utilities for the BTC ETF Scraper project
# Actualizado para Docker con Xvfb + undetected-chromedriver
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
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import yfinance as yf
import subprocess

# ======================== BASE CONFIGURATION ========================

OUTPUT_BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../etfs_data"))
CSV_DIR  = os.path.join(OUTPUT_BASE_DIR, "csv")
JSON_DIR = os.path.join(OUTPUT_BASE_DIR, "json")
HEADLESS   = False
TIMEOUT    = 45

# --- Script-editable variable ---
SAVE_FORMAT_SETTING = "csv"

# --- Throttling/Backoff (adjustable via ENV) ---
REQUEST_BASE_DELAY = float(os.getenv("ETF_REQUEST_DELAY", "3.0"))
REQUEST_JITTER     = float(os.getenv("ETF_REQUEST_JITTER", "2.0"))
MAX_RETRIES        = int(os.getenv("ETF_MAX_RETRIES", "5"))
BACKOFF_BASE       = float(os.getenv("ETF_BACKOFF_BASE", "2.0"))
BACKOFF_MAX        = float(os.getenv("ETF_BACKOFF_MAX", "60"))

# Driver preference: "undetected" or "standard"
DRIVER_MODE = os.getenv("ETF_DRIVER_MODE", "undetected").lower()

# Final SAVE_FORMAT (ENV has priority)
SAVE_FORMAT = SAVE_FORMAT_SETTING
_env_fmt = os.environ.get("ETF_SAVE_FORMAT", "").lower().strip()
if _env_fmt in ("csv", "xlsx"):
    SAVE_FORMAT = _env_fmt


# ======================== ENVIRONMENT DETECTION ========================

def _is_display_available() -> bool:
    """Detecta si hay un display disponible (Xvfb o real)"""
    display = os.environ.get("DISPLAY")
    if not display:
        return False
    
    # Verificar que Xvfb está corriendo si es :99
    if ":99" in display:
        try:
            import subprocess
            result = subprocess.run(
                ["pgrep", "-x", "Xvfb"], 
                capture_output=True, 
                timeout=5
            )
            return result.returncode == 0
        except Exception:
            # Si no podemos verificar, asumimos que está disponible
            return True
    return True


def _is_docker() -> bool:
    """Detecta si estamos corriendo en Docker"""
    return (
        os.path.exists("/.dockerenv") or 
        os.environ.get("DOCKER_CONTAINER") == "true" or
        os.path.exists("/app/Dockerfile")
    )


# ======================== TIMING UTILITIES ========================

def polite_sleep():
    """Adds a random delay between requests to avoid being blocked."""
    delay = max(0.0, REQUEST_BASE_DELAY + random.uniform(0, REQUEST_JITTER))
    time.sleep(delay)


def _retry_after_seconds(val):
    """Parses Retry-After header which can be seconds or a date string."""
    if not val:
        return None
    try:
        return float(val)
    except ValueError:
        try:
            dt = parsedate_to_datetime(val)
            now = datetime.datetime.now(datetime.timezone.utc)
            return max(0.0, (dt - now).total_seconds())
        except Exception:
            return None


# ======================== SESSION MANAGEMENT ========================

def _session_from_driver(driver):
    """Creates a requests Session with cookies inherited from the Selenium driver."""
    s = requests.Session()
    for c in driver.get_cookies():
        try:
            s.cookies.set(
                c["name"], 
                c["value"], 
                domain=c.get("domain"), 
                path=c.get("path", "/")
            )
        except Exception:
            s.cookies.set(c["name"], c["value"])
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    })
    return s


def browser_fetch_text(driver, url, accept="application/json, text/plain, */*"):
    """Fetches text content from a URL inside the browser's context."""
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


# ======================== DRIVER SETUP ========================

def setup_driver(headless=None):
    """
    Inicializa el WebDriver con la mejor estrategia disponible:
    1. undetected-chromedriver (si está disponible)
    2. Selenium estándar con patches anti-detección
    
    Detecta automáticamente si usar headless o Xvfb.
    """
    os.makedirs(CSV_DIR, exist_ok=True)
    os.makedirs(JSON_DIR, exist_ok=True)
    
    # Determinar si usar headless
    if headless is None:
        # Auto-detectar: usar GUI si hay display disponible (Xvfb)
        has_display = _is_display_available()
        headless = not has_display
        
        if has_display:
            print(f"[DRIVER] Display detectado ({os.environ.get('DISPLAY')}), usando modo GUI")
        else:
            print("[DRIVER] Sin display, usando modo headless")
    
    # Intentar undetected-chromedriver primero
    if DRIVER_MODE == "undetected":
        driver = _setup_undetected_driver(headless)
        if driver:
            return driver
        print("[DRIVER] undetected-chromedriver falló, usando Selenium estándar")
    
    # Fallback a Selenium estándar
    return _setup_standard_driver(headless)


def _get_chrome_major_version():
    """Detecta la versión principal (major) de Chrome instalada en el sistema."""
    # 1. Intentar en Windows vía Registro
    if os.name == 'nt':
        try:
            import winreg
            keys = [
                (winreg.HKEY_CURRENT_USER, r"Software\Google\Chrome\BLBeacon"),
                (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\Google Chrome"),
                (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Google\Chrome\BLBeacon")
            ]
            for hkey, path in keys:
                try:
                    with winreg.OpenKey(hkey, path) as key:
                        version, _ = winreg.QueryValueEx(key, "version")
                        major = int(version.split(".")[0])
                        return major
                except:
                    continue
        except Exception:
            pass

    # 2. Intentar vía línea de comandos (Linux/Mac/Windows)
    commands = [
        ["google-chrome", "--version"],
        ["google-chrome-stable", "--version"],
        ["chromium", "--version"],
        ["chromium-browser", "--version"],
        # Windows alternativo
        ["powershell", "-command", "(Get-Item 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe').VersionInfo.ProductVersion"],
        ["powershell", "-command", "(Get-Item 'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe').VersionInfo.ProductVersion"]
    ]
    
    for cmd in commands:
        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('utf-8')
            # Buscar el primer número largo (ej: 121.0.6167.184 o solo 121)
            match = re.search(r'(\d+)\.', output) or re.search(r'(\d+)', output)
            if match:
                return int(match.group(1))
        except Exception:
            continue

    return None


def _setup_undetected_driver(headless: bool):
    """Configura undetected-chromedriver"""
    try:
        import undetected_chromedriver as uc
    except ImportError:
        print("[DRIVER] undetected-chromedriver no instalado")
        return None
    
    try:
        options = uc.ChromeOptions()
        
        # Argumentos básicos
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-infobars")
        
        # Configurar directorio de descargas
        prefs = {
            "download.default_directory": os.path.abspath(CSV_DIR),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "plugins.always_open_pdf_externally": True,
        }
        options.add_experimental_option("prefs", prefs)
        
        # Detectar versión para evitar desajustes de ChromeDriver
        major_version = _get_chrome_major_version()
        if major_version:
            print(f"[DRIVER] Versión detectada de Chrome: {major_version}")
        
        # Crear driver
        driver = uc.Chrome(
            options=options,
            headless=headless,
            use_subprocess=True,
            version_main=major_version,  # Usar versión detectada
        )
        
        # Configurar timeouts
        driver.set_page_load_timeout(60)
        driver.implicitly_wait(10)
        
        print(f"[DRIVER] ✅ undetected-chromedriver iniciado (headless={headless})")
        return driver
        
    except Exception as e:
        print(f"[DRIVER] Error con undetected-chromedriver: {e}")
        return None


def _setup_standard_driver(headless: bool):
    """Configura Selenium estándar con patches anti-detección"""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from subprocess import DEVNULL
    
    opts = Options()
    
    # Argumentos anti-detección
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    opts.add_experimental_option("useAutomationExtension", False)
    
    # Argumentos básicos
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-infobars")
    
    # User agent
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    )
    
    if headless:
        opts.add_argument("--headless=new")
    
    # Configurar directorio de descargas
    prefs = {
        "download.default_directory": os.path.abspath(CSV_DIR),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "plugins.always_open_pdf_externally": True,
    }
    opts.add_experimental_option("prefs", prefs)
    
    try:
        service = Service(log_output=DEVNULL)
        driver = webdriver.Chrome(options=opts, service=service)
    except TypeError:
        driver = webdriver.Chrome(options=opts)
    
    # Patches adicionales anti-detección
    driver.execute_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
    )
    
    try:
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {
            "userAgent": (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
            )
        })
    except Exception:
        pass  # CDP no disponible en algunas versiones
    
    # Configurar timeouts
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(10)
    
    print(f"[DRIVER] ✅ Selenium estándar iniciado (headless={headless})")
    return driver


# ======================== CLICK HELPERS ========================

def _try_click_any(driver, selectors, wait_sec=8):
    """Attempts to click any of the provided selectors (CSS or XPATH)."""
    for sel in selectors:
        try:
            by = By.CSS_SELECTOR if sel.startswith("#") else By.XPATH
            btn = WebDriverWait(driver, wait_sec).until(
                EC.element_to_be_clickable((by, sel))
            )
            try:
                btn.click()
            except Exception:
                driver.execute_script("arguments[0].click();", btn)
            polite_sleep()
            return True
        except Exception:
            pass
    return False


def _harvest_find_click_any(driver, selectors, by="css", wait=10, scroll=True, sleep_after=0.4):
    """Specific clicker helper for Harvest (and potentially others) with scroll options."""
    for sel in selectors:
        try:
            if by == "css":
                el = WebDriverWait(driver, wait).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, sel))
                )
            else:
                el = WebDriverWait(driver, wait).until(
                    EC.element_to_be_clickable((By.XPATH, sel))
                )
            if scroll:
                driver.execute_script(
                    "arguments[0].scrollIntoView({block:'center'});", el
                )
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


# ======================== DOWNLOAD UTILITIES ========================

def download_url_to_file(url, referer, output_path, accept="*/*", session=None):
    """Downloads a file from a URL using requests, with retry and backoff logic."""
    sess = session or requests.Session()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        ),
        "Accept": accept,
        "Referer": referer,
        "Origin": (
            urlparse(referer).scheme + "://" + urlparse(referer).netloc 
            if referer else ""
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            polite_sleep()
            r = sess.get(
                url, 
                headers=headers, 
                stream=True, 
                timeout=60, 
                allow_redirects=True
            )
            print(f"[HTTP] GET {url[:80]}... status={r.status_code}")
            
            if r.status_code in (429, 403, 503):
                ra = _retry_after_seconds(r.headers.get("Retry-After"))
                wait = ra if ra is not None else min(
                    BACKOFF_MAX, 
                    (BACKOFF_BASE ** attempt) + random.random()
                )
                print(
                    f"[BACKOFF] status={r.status_code} -> sleep {wait:.1f}s "
                    f"(attempt {attempt+1}/{MAX_RETRIES})"
                )
                time.sleep(wait)
                continue
            
            r.raise_for_status()
            
            os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
            with open(output_path, "wb") as f:
                for ch in r.iter_content(8192):
                    if ch:
                        f.write(ch)
            
            print(
                f"[DOWNLOAD] SUCCESS Saved: {output_path} "
                f"({os.path.getsize(output_path)} bytes)"
            )
            return True
            
        except Exception as e:
            wait = min(BACKOFF_MAX, (BACKOFF_BASE ** attempt) + random.random())
            print(
                f"[DOWNLOAD] Error (attempt {attempt+1}/{MAX_RETRIES}): {e} "
                f"-> sleep {wait:.1f}s"
            )
            time.sleep(wait)
    
    return False


# ======================== DATA FRAME UTILITIES ========================

def _find_col(df, candidates):
    """Finds a column in a DataFrame that matches any of the candidate names."""
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
    except Exception:
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
        except Exception:
            return


def _yf_close_by_date(ticker, start_yyyymmdd, end_yyyymmdd):
    """Fetches historical close prices from Yahoo Finance for a specific date range."""
    try:
        start_d = pd.to_datetime(start_yyyymmdd).date()
        end_d = pd.to_datetime(end_yyyymmdd).date() + datetime.timedelta(days=1)
        
        y = yf.Ticker(ticker).history(
            start=start_d, 
            end=end_d, 
            interval="1d", 
            auto_adjust=False
        )
        
        if y is None or y.empty:
            return pd.DataFrame(columns=["date", "market price"])
        
        y = y.reset_index()
        date_col = "Date" if "Date" in y.columns else y.columns[0]
        y["date"] = pd.to_datetime(y[date_col]).dt.strftime("%Y%m%d")
        
        df_px = y[["date", "Close"]].rename(columns={"Close": "market price"})
        df_px["market price"] = (
            df_px["market price"]
            .astype(str)
            .str.replace("$", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        return df_px
        
    except Exception:
        return pd.DataFrame(columns=["date", "market price"])


# ======================== SAVE UTILITIES ========================

def save_dataframe(df, base_name, sheet_name="Historical"):
    """
    Saves a DataFrame to the database (if enabled) and optionally to CSV/JSON files.
    
    Behavior:
    - Always saves to database if DATABASE_URL is configured
    - Saves to CSV/JSON files only if ETF_SAVE_FILES=1 (set via --save-files flag)
    """
    # Check if file saving is enabled (controlled by --save-files flag in main.py)
    save_files = os.environ.get("ETF_SAVE_FILES", "1") == "1"
    
    # Try to save to database first
    try:
        from core.db_adapter import is_db_enabled, save_etf_dataframe
        if is_db_enabled():
            count = save_etf_dataframe(df, base_name)
            if count > 0:
                print(f"[DB] ✅ Saved {count} rows for {base_name}")
    except ImportError:
        pass  # db_adapter not available, continue with file saving
    except Exception as e:
        print(f"[DB] ⚠️  Failed to save to database: {e}")
    
    # If file saving is disabled, return early
    if not save_files:
        return None
    
    # Save to files (CSV/XLSX and JSON)
    os.makedirs(CSV_DIR, exist_ok=True)
    os.makedirs(JSON_DIR, exist_ok=True)
    
    ext = SAVE_FORMAT
    csv_path = os.path.join(CSV_DIR, f"{base_name}.{ext}")
    json_path = os.path.join(JSON_DIR, f"{base_name}.json")
    
    # Remove existing files
    for path in [csv_path, json_path]:
        if os.path.exists(path):
            try:
                os.remove(path)
            except Exception:
                pass
    
    # Save formatted file (CSV or XLSX)
    if ext == "xlsx":
        with pd.ExcelWriter(csv_path, engine="openpyxl") as w:
            df.to_excel(w, sheet_name=sheet_name, index=False)
        print(f"[SAVE] SUCCESS XLSX saved: {csv_path}")
    else:
        df.to_csv(csv_path, index=False)
        print(f"[SAVE] SUCCESS CSV saved: {csv_path}")
    
    # Save JSON file
    try:
        df.to_json(json_path, orient="records", indent=2)
        print(f"[SAVE] SUCCESS JSON saved: {json_path}")
    except Exception as e:
        print(f"[SAVE] ERROR JSON failed: {e}")
    
    return csv_path

