import os
import time
import pandas as pd
import yfinance as yf
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sys

# Add the project root to sys.path to allow absolute imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

try:
    from core.utils.helpers import (
        polite_sleep, save_dataframe, _try_click_any,
        setup_driver, SAVE_FORMAT
    )
except ImportError:
    # Fallback for standalone execution if sys.path trick fails
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../utils")))
    from helpers import (
        polite_sleep, save_dataframe, _try_click_any,
        setup_driver, SAVE_FORMAT
    )

CHINAAMC_HK_TICKER = "9042.HK"

# JavaScript to extract data points directly from ECharts instance
JS_GET_ALL_POINTS_CHINAAMC = r"""
(function(){
  function normRow(x, y){
    let date = null;
    if (x != null){
      if (typeof x === 'number'){
        try{ date = new Date(x).toISOString().slice(0,10); }catch(e){}
      } else {
        const s = String(x).trim().replace(/\//g,'-');
        const m = s.match(/(20\d{2}-\d{2}-\d{2})/);
        date = m ? m[1] : null;
      }
    }
    return {date, nav: (y!=null? String(y): null)};
  }
  function normSeriesData(arr){
    const out=[];
    for (const p of arr){
      if (Array.isArray(p)){ out.push({x:p[0], y:p[1]}); }
      else if (p && typeof p==='object' && 'value' in p){
        if (Array.isArray(p.value)) out.push({x:p.value[0], y:p.value[1]});
        else out.push({x:null, y:p.value});
      } else if (typeof p==='number'){ out.push({x:null, y:p}); }
    }
    return out;
  }
  let root = document.querySelector("div[_echarts_instance_]") || null;
  if (!root){
    const cvs = Array.from(document.querySelectorAll("canvas"))
      .filter(c=>c.offsetWidth>0 && c.offsetHeight>0);
    for (const c of cvs){
      let p=c; for (let i=0;i<6 && p;i++){ if (p.getAttribute && p.getAttribute("_echarts_instance_")){ root=p; break; } p=p.parentElement; }
      if (root) break;
    }
  }
  if (!root || !window.echarts || !window.echarts.getInstanceByDom) return null;

  const ec = window.echarts.getInstanceByDom(root);
  if (!ec || !ec.getOption) return null;
  const opt = ec.getOption();

  const xa = (opt.xAxis && opt.xAxis.length) ? opt.xAxis[0] : null;
  const xdata = xa && xa.data ? xa.data : null;
  const ser = (opt.series && opt.series.length) ? opt.series[0] : null;
  if (!ser || !ser.data) return null;

  const pts = normSeriesData(ser.data);
  const rows=[];
  for (let i=0;i<pts.length;i++){
    const x = (pts[i].x != null) ? pts[i].x : (xdata ? xdata[i] : null);
    const y = pts[i].y;
    const r = normRow(x,y);
    if (r.date && r.nav) rows.push(r);
  }
  if (rows.length) return rows;

  const n = Math.max(ser.data.length, xdata ? xdata.length : 0);
  const out=[];
  for (let i=0;i<n; i++){
    try{
      ec.dispatchAction({type:'showTip', seriesIndex:0, dataIndex:i});
      const p = pts[i] || {};
      const x = (p.x != null) ? p.x : (xdata ? xdata[i] : null);
      const y = (p.y != null) ? p.y : null;
      const r = normRow(x,y);
      if (r.date && r.nav) out.push(r);
    }catch(e){}
  }
  return out.length ? out : null;
})()
"""

# JavaScript to sweep the chart with mouse movements and read the tooltip
JS_MOUSE_SWEEP_AND_READ_CHINAAMC = r"""
const step = arguments[0] || 6;
const maxSteps = arguments[1] || 2000;
function findInteractiveCanvas(){
  const all = Array.from(document.querySelectorAll('canvas'))
    .filter(c=>c.offsetWidth>0 && c.offsetHeight>0);
  const zr = all.find(c=>c.getAttribute && c.getAttribute('data-zr-dom-id'));
  return zr || all[0] || null;
}
function tooltipText(){
  const candidates = Array.from(document.querySelectorAll("div[style*='position: absolute']"))
    .filter(d=>/z-index/i.test(d.getAttribute('style')||'') && d.innerText && d.offsetWidth>0 && d.offsetHeight>0);
  if (!candidates.length) return '';
  return candidates[candidates.length-1].innerText.trim();
}
function readRowFromTooltip(txt){
  const mDate = txt.match(/\b(20\d{2}-\d{2}-\d{2})\b/);
  const nums = txt.replace(/,/g,'').match(/-?\d+(?:\.\d+)?/g);
  if (!mDate || !nums) return null;
  return {date: mDate[1], nav: nums[nums.length-1]};
}
const cv = findInteractiveCanvas();
if (!cv) return [];
const rect = cv.getBoundingClientRect();
const y = Math.floor(rect.top + rect.height*0.80);
const left = Math.floor(rect.left + rect.width*0.03);
const right = Math.floor(rect.right - rect.width*0.03);

const rows = [];
const seen = new Set();
for (let x=left, i=0; x<=right && i<maxSteps; x+=Math.max(1,step), i++){
  const ev = new MouseEvent('mousemove', {clientX:x, clientY:y, bubbles:true, cancelable:true, view:window});
  cv.dispatchEvent(ev);
  const start = performance.now();
  while (performance.now() - start < 12) {}
  const txt = tooltipText();
  const row = readRowFromTooltip(txt);
  if (row && !seen.has(row.date)){
    seen.add(row.date);
    rows.push(row);
  }
}
return rows;
"""

def accept_cookies_chinaamc(driver):
    """Handles the cookie consent banner on the ChinaAMC website."""
    clicked = _try_click_any(driver, [
        "#onetrust-accept-btn-handler",
        "//button[@id='onetrust-accept-btn-handler']",
        "//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'accept all')]",
        "//button[contains(.,'同意') or contains(.,'接受')]",
    ], wait_sec=10)
    if not clicked:
        try:
            # Fallback: Hide the consent banners via JS
            driver.execute_script("""
              (function(){
                ['onetrust-banner-sdk','onetrust-consent-sdk'].forEach(id=>{
                  const n=document.getElementById(id); if(n){ n.style.display='none'; }
                });
              })();
            """)
            polite_sleep()
            return True
        except:
            return False
    return True

def _chinaamc_click_historical_navs(driver):
    """Attempts to click the 'Historical NAVs' tab on the page."""
    selectors = [
        (By.CSS_SELECTOR, "div.fund-tabs-content-wrapper--items [data-content*='content_nav']"),
        (By.XPATH, "//div[contains(@class,'fund-tabs-content-wrapper')]//div[contains(@class,'items-item')][contains(@data-content,'content_nav')]"),
        (By.XPATH, "//span[normalize-space()='Historical NAVs']/ancestor::div[contains(@class,'items-item')]"),
    ]
    for by, sel in selectors:
        try:
            el = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((by, sel)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            driver.execute_script("arguments[0].click();", el)
            time.sleep(0.5)
            return True
        except: continue
    return False

def _chinaamc_try_extract_via_echarts(driver):
    """Tries to extract chart data directly from the injected ECharts object."""
    try:
        rows = driver.execute_script(JS_GET_ALL_POINTS_CHINAAMC)
        return rows if rows else []
    except Exception as e:
        print(f"[ChinaAMC JS ECharts] Error: {e}")
        return []

def _chinaamc_sweep_with_js_mousemove(driver, step_px=6):
    """Sweeps the chart area with virtual mouse movements to trigger and read tooltips."""
    try:
        rows = driver.execute_script(JS_MOUSE_SWEEP_AND_READ_CHINAAMC, int(step_px), 4000)
        return rows if rows else []
    except Exception as e:
        print(f"[ChinaAMC JS mousemove] Error: {e}")
        return []

def _chinaamc_add_market_price(df: pd.DataFrame, ticker: str = CHINAAMC_HK_TICKER) -> pd.DataFrame:
    """Adds historical market prices from Yahoo Finance to the NAV data."""
    if df.empty or "date" not in df.columns:
        df["market price"] = pd.Series(dtype="float")
        return df

    dmin = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce").min()
    dmax = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce").max()
    if pd.isna(dmin):
        df["market price"] = pd.Series(dtype="float")
        return df

    try:
        hist = yf.Ticker(ticker).history(
            start=(dmin - pd.Timedelta(days=2)).date(),
            end=(dmax + pd.Timedelta(days=2)).date(),
            interval="1d",
            auto_adjust=False
        ).reset_index()
        if not hist.empty:
            date_col = "Date" if "Date" in hist.columns else hist.columns[0]
            hist["date"] = pd.to_datetime(hist[date_col]).dt.strftime("%Y%m%d")
            price_map = dict(zip(hist["date"], hist["Close"]))
            df["market price"] = df["date"].map(price_map).astype("float")
    except Exception as e:
        print(f"[ChinaAMC yfinance] Warning: {e}")

    # Try to add intraday price for today if missing
    try:
        today_hk = pd.Timestamp.now(tz="Asia/Hong_Kong").strftime("%Y%m%d")
        intraday = yf.Ticker(ticker).history(period="1d", interval="1m", auto_adjust=False)
        if not intraday.empty:
            last_px = float(intraday["Close"].dropna().iloc[-1])
            if (df["date"] == today_hk).any():
                df.loc[df["date"] == today_hk, "market price"] = last_px
            else:
                df = pd.concat([df, pd.DataFrame([{"date": today_hk, "market price": last_px}])], ignore_index=True)
    except: pass

    return df

def process_single_etf_chinaamc(driver, etf, site_url):
    """Orchestrates the scraping of ChinaAMC ETF data via chart extraction and Yahoo Finance."""
    name = etf["name"]
    base = os.path.splitext(etf["output_filename"])[0]
    print(f"\n[ETF] Processing {name} (ChinaAMC - ECharts/tooltip + yfinance) -> output .{SAVE_FORMAT}")
    print("="*50)
    try:
        driver.get(site_url); polite_sleep()
        accept_cookies_chinaamc(driver); polite_sleep()
        driver.execute_script("window.scrollBy(0, 800);")
        time.sleep(0.6)
        _chinaamc_click_historical_navs(driver)
        rows = _chinaamc_try_extract_via_echarts(driver)
        if not rows:
            rows = _chinaamc_sweep_with_js_mousemove(driver, step_px=4)
        if not rows:
            msg = "No data extracted from ChinaAMC charts."
            return False, msg

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y%m%d")
        df["nav"]  = pd.to_numeric(df["nav"], errors="coerce")
        df = df.dropna(subset=["date","nav"]).sort_values("date").reset_index(drop=True)

        df = _chinaamc_add_market_price(df, CHINAAMC_HK_TICKER)
        keep = ["date", "nav", "market price"]
        for c in keep:
            if c not in df.columns:
                df[c] = pd.NA
        df = df[keep]

        save_dataframe(df, base, sheet_name="Historical")
        print(f"[SUCCESS] ChinaAMC processed ({name})")
        return True, None
    except Exception as e:
        msg = f"ChinaAMC error: {e}"
        print(f"[ChinaAMC ERROR] {msg}")
        return False, msg

def main():
    """Standalone execution entry point."""
    etf = {"name": "ChinaAMC Bitcoin ETF (9042.HK)", "output_filename": "chinaamc_dailynav.xlsx"}
    site_url = "https://www.chinaamc.com.hk/product/chinaamc-bitcoin-etf/"
    
    driver = setup_driver(headless=False)
    try:
        ok, err = process_single_etf_chinaamc(driver, etf, site_url)
        if ok:
            print("[STANDALONE] ChinaAMC processed successfully.")
        else:
            print(f"[STANDALONE] ChinaAMC failed: {err}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
