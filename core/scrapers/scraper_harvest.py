import os
import time
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

from core.utils.helpers import (
    polite_sleep, _session_from_driver, download_url_to_file,
    normalize_date_column, save_dataframe, _safe_remove,
    _harvest_find_click_any, setup_driver, CSV_DIR, JSON_DIR, SAVE_FORMAT
)

HARVEST_URL = "https://www.harvestglobal.com.hk/hgi/index.php/funds/passive/BTCETF#overview"

def accept_cookies_harvest(driver):
    """Handle cookie consent banner on the Harvest Global website."""
    try:
        banner_present = bool(driver.find_elements(By.ID, "onetrust-banner-sdk")) \
                         or bool(driver.find_elements(By.ID, "CybotCookiebotDialog"))
    except:
        banner_present = False

    if not banner_present:
        return False

    ok = _harvest_find_click_any(driver,
        ["#onetrust-accept-btn-handler","button#onetrust-accept-btn-handler"], by="css", wait=6
    ) or _harvest_find_click_any(driver, [
        "//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'accept all')]",
        "//a[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'accept all')]",
        "//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'accept')]",
        "//a[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'accept')]",
        "//button[contains(.,'同意') or contains(.,'接受') or contains(.,'Aceptar') or contains(.,'ACEPTAR')]",
    ], by="xpath", wait=6)

    if not ok:
        try:
            driver.execute_script("""
                (function(){
                    const hide = (el)=>{ if(el){ el.dataset._prevDisplay = el.style.display; el.style.display='none'; } };
                    ['onetrust-banner-sdk','onetrust-consent-sdk','CybotCookiebotDialog'].forEach(id=>{
                        const n=document.getElementById(id); hide(n);
                    });
                    document.querySelectorAll('.ot-sdk-container,.cookie,.cookies,.cookie-banner,.cookiebar')
                        .forEach(n=>hide(n));
                })();
            """)
        except: pass
    return ok

def _harvest_hide_cookie_banners(driver):
    """Hide any cookie banners using JavaScript to prevent interception."""
    try:
        driver.execute_script("""
            (function(){
                const hide = (el)=>{ if(el){ el.dataset._prevDisplay = el.style.display; el.style.display='none'; } };
                ['onetrust-banner-sdk','onetrust-consent-sdk','CybotCookiebotDialog'].forEach(id=>{
                    const n=document.getElementById(id); hide(n);
                });
                document.querySelectorAll('.ot-sdk-container,.cookie,.cookies,.cookie-banner,.cookiebar')
                    .forEach(n=>hide(n));
            })();
        """)
    except: pass

def harvest_select_site_hk(driver):
    """Handle the 'Select site' modal specifically for the Hong Kong region."""
    print("[HARVEST] Checking 'Select site' modal...")
    try:
        WebDriverWait(driver, 6).until(
            EC.presence_of_element_located(
                (By.XPATH,
                 "//div[contains(@class,'box-content') and .//h3[contains(.,'Hong Kong')]]"
                 " | //div[contains(.,'Select site') and contains(@class,'box-content')]")
            )
        )
    except:
        print("[HARVEST] Modal not detected.")
        return

    _harvest_hide_cookie_banners(driver)

    hk_locators = [
        (By.XPATH, "//h3[contains(.,'Hong Kong')]/following::a[contains(@class,'button')][1]"),
        (By.XPATH, "//div[contains(@class,'span3') or contains(@class,'col')]"
                   "[.//img[contains(@alt,'Hong Kong') or contains(@src,'hk')]]//a[contains(@class,'button')]"),
        (By.CSS_SELECTOR, "div.box-content a.button-box.loadingBtn"),
        (By.XPATH, "(//a[contains(@class,'button') and contains(.,'Visit')])[2]"),
    ]
    hk_btn = None
    for by, sel in hk_locators:
        try:
            btn = WebDriverWait(driver, 6).until(EC.element_to_be_clickable((by, sel)))
            if btn and btn.is_displayed():
                hk_btn = btn
                break
        except: continue

    if not hk_btn:
        print("[HARVEST] 'Hong Kong' button not found.")
        return

    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", hk_btn)
        time.sleep(0.2)
        ActionChains(driver).move_to_element(hk_btn).pause(0.1).click(hk_btn).perform()
        print("[HARVEST] Clicked 'Visit Site (Hong Kong)'.")
    except:
        try: hk_btn.click()
        except: driver.execute_script("arguments[0].click();", hk_btn)

    try:
        WebDriverWait(driver, 12).until(
            EC.any_of(
                EC.invisibility_of_element_located((By.XPATH, "//div[contains(@class,'box-content') and .//h3]")),
                EC.url_contains("/hgi/index.php")
            )
        )
    except: pass

    driver.get(HARVEST_URL)
    time.sleep(1.0)

def harvest_select_usd_tab(driver):
    """Select the USD currency tab on the Harvest overview page."""
    print("[HARVEST] Selecting USD tab...")
    try:
        sec = WebDriverWait(driver, 12).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "section#overview, section .overviewInfo, article"))
        )
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", sec)
        time.sleep(0.3)
    except: pass

    try:
        already = driver.find_elements(By.XPATH, "//span[contains(@class,'current') and normalize-space()='USD']")
        if already:
            print("[HARVEST] USD tab already active.")
            return True
    except: pass

    sels = [
        "span[ng-click*=\"marketInformation('USD')\"]",
        "span[ng-click*='marketInformation(\"USD\")']",
        "//span[contains(@ng-click,'USD')]",
        "//span[normalize-space()='USD' and @ng-click]",
        "//h3[contains(.,'Market Information')]/following::span[normalize-space()='USD'][1]",
    ]

    if _harvest_find_click_any(driver, sels[:2], by="css", wait=8) or \
       _harvest_find_click_any(driver, sels[2:4], by="xpath", wait=6) or \
       _harvest_find_click_any(driver, [sels[4]], by="xpath", wait=6):
        print("[HARVEST] USD tab selected.")
        return True

    print("[HARVEST] Could not select USD tab (proceeding anyway).")
    return False

def harvest_get_download_href(driver):
    """Extract the XLS download URL from the Harvest page."""
    print("[HARVEST] Searching for XLS link...")
    xpaths = [
        "//a[contains(@href,'hgi-web/excels/nav/') and contains(@href,'BTCETF_') and contains(@href,'USD') and contains(@href,'NAV.xls')]",
        "//a[contains(@href,'s3.ap-southeast-1.amazonaws.com') and contains(@href,'BTCETF') and contains(@href,'USD') and contains(@href,'NAV.xls')]",
        "//a[contains(@href,'BTCETF') and contains(@href,'USD') and (contains(@href,'.xls') or contains(@href,'.xlsx'))]",
    ]
    for xp in xpaths:
        try:
            el = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, xp)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            time.sleep(0.2)
            href = el.get_attribute("href") or ""
            if href:
                return href
        except: continue
    return None

def parse_harvest_xls_to_df(xls_path):
    """Parse the downloaded Harvest XLS file into a clean DataFrame."""
    try:
        raw = pd.read_excel(xls_path, sheet_name="English", header=None, dtype=str, engine="xlrd")
    except:
        try:
            raw = pd.read_excel(xls_path, sheet_name=0, header=None, dtype=str, engine="xlrd")
        except:
            raw = pd.read_excel(xls_path, sheet_name=0, header=None, dtype=str, engine="openpyxl")

    header_idx = None
    for i in range(min(120, len(raw))):
        vals = [str(v).strip() for v in list(raw.iloc[i].fillna(""))]
        j = "|".join(v.lower() for v in vals)
        if ("date" in j) and ("nav per unit (usd)" in j) and ("market closing price (usd)" in j):
            header_idx = i
            break

    if header_idx is None:
        raise RuntimeError("Header row not found in Harvest XLS file.")

    headers = [str(v).strip() for v in list(raw.iloc[header_idx].fillna(""))]
    data = raw.iloc[header_idx + 1:].copy()
    data.columns = headers

    def _pick(cols, target):
        low = [c.lower().strip() for c in cols]
        t = target.lower().strip()
        if t in low: return cols[low.index(t)]
        for i, l in enumerate(low):
            if t in l: return cols[i]
        return None

    date_col = _pick(list(data.columns), "Date")
    nav_col = _pick(list(data.columns), "NAV per unit (USD)")
    px_col = _pick(list(data.columns), "Market Closing Price (USD)")

    if not all([date_col, nav_col, px_col]):
        raise RuntimeError(f"Required columns not found.")

    df = data[[date_col, nav_col, px_col]].copy()
    df = df[~df[date_col].isna() & (df[date_col].astype(str).str.strip() != "")]
    df = df.rename(columns={date_col: "date", nav_col: "nav", px_col: "market price"})

    raw_date = df["date"].astype(str).str.strip()
    dt = pd.to_datetime(raw_date, errors="coerce", dayfirst=True)
    if dt.isna().any():
        dt2 = pd.to_datetime(raw_date[dt.isna()], errors="coerce")
        dt = dt.where(~dt.isna(), dt2)
    df["date"] = dt.dt.strftime("%Y%m%d").where(~dt.isna(), raw_date)

    for c in ["nav", "market price"]:
        df[c] = (df[c].astype(str).str.replace("$", "", regex=False).str.replace(",", "", regex=False).str.strip())
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df[["date", "nav", "market price"]]

def process_single_etf_harvest(driver, etf, site_url):
    """Main process to scrape a single Harvest Global ETF."""
    name = etf["name"]
    base = os.path.splitext(etf["output_filename"])[0]
    tmp_xls = os.path.join(CSV_DIR, base + "_tmp.xls")
    print(f"\n[ETF] Processing {name} (Harvest – BTCETF USD XLS) → output .{SAVE_FORMAT}")
    print("="*50)

    try:
        driver.get(site_url); polite_sleep()
    except Exception as e:
        print(f"[HARVEST] Initial navigation: {e}")

    try:
        harvest_select_site_hk(driver); polite_sleep()
    except: pass

    try:
        accept_cookies_harvest(driver); polite_sleep()
    except: pass

    try:
        harvest_select_usd_tab(driver); polite_sleep()
    except: pass

    href = None
    try:
        href = harvest_get_download_href(driver)
    except: pass

    if not href:
        msg = "Download link not found."
        return False, msg

    try:
        sess = _session_from_driver(driver)
        ok = download_url_to_file(
            href, site_url, tmp_xls,
            accept="application/vnd.ms-excel,application/octet-stream,*/*",
            session=sess
        )
        if not ok:
            msg = "Download failed."
            return False, msg

        df = parse_harvest_xls_to_df(tmp_xls)
        df = normalize_date_column(df)
        save_dataframe(df, base, sheet_name="USD")
        _safe_remove(tmp_xls)
        print(f"[SUCCESS] ✓ Harvest processed ({name})")
        return True, None
    except Exception as e:
        msg = f"Error: {e}"
        print(f"[HARVEST] {msg}")
        _safe_remove(tmp_xls)
        return False, msg

def main():
    """Standalone execution for Harvest scraper."""
    etf = {"name": "Harvest Bitcoin Spot ETF (BTCETF)", "output_filename": "harvest_dailynav.xlsx"}
    site_url = "https://www.harvestglobal.com.hk/hgi/index.php/funds/passive/BTCETF#overview"
    
    driver = setup_driver(headless=False)
    try:
        ok, err = process_single_etf_harvest(driver, etf, site_url)
        if ok:
            print("[STANDALONE] Harvest processed successfully.")
        else:
            print(f"[STANDALONE] Harvest failed: {err}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
