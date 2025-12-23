import os
import re
import time
import datetime
import pandas as pd
import yfinance as yf
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
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

BITWISE_YF_TICKER = "BITB"
_DATE_RE = re.compile(r"([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})")
_NUM_RE = re.compile(r"(-?\d+(?:\.\d+)?)")

def accept_cookies_bitwise(driver):
    """Handles the cookie consent banner on the Bitwise website."""
    clicked = _try_click_any(driver, [
        "//button[@id='onetrust-accept-btn-handler']",
        "#onetrust-accept-btn-handler",
        "//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'accept all')]",
        "//button[contains(.,'I Accept') or contains(.,'Accept all') or contains(.,'Allow all')]",
    ], wait_sec=10)
    if not clicked:
        try:
            # Fallback: Hide the consent SDK via JS if clicking fails
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

def _bitwise_find_chart_svg(driver):
    """Finds the Highcharts SVG element on the page."""
    sels = ["svg.highcharts-root", ".highcharts-container svg", "[data-highcharts-chart] svg"]
    for sel in sels:
        try:
            svg = driver.find_element(By.CSS_SELECTOR, sel)
            if svg.is_displayed(): return svg
        except: continue
    raise RuntimeError("Could not find Highcharts SVG in Bitwise page.")

def _bitwise_get_plot_bounds(driver, svg_element):
    """Calculates the plot area boundaries of the Highcharts chart."""
    bounds = driver.execute_script("""
        const svg = arguments[0];
        const plotBg = svg.querySelector('rect.highcharts-plot-background');
        if (plotBg) {
            const bbox = plotBg.getBBox();
            return { x: bbox.x, y: bbox.y, width: bbox.width, height: bbox.height };
        } else {
            const svgRect = svg.getBoundingClientRect();
            return { x: svgRect.width * 0.1, y: svgRect.height * 0.1, width: svgRect.width * 0.8, height: svgRect.height * 0.7 };
        }
    """, svg_element)
    return bounds

def _bitwise_find_zero_line_y(driver, svg_element, plot_bounds):
    """Locates the Y-coordinate of the zero line in the chart."""
    return driver.execute_script("""
        const svg = arguments[0];
        const bounds = arguments[1];
        const texts = Array.from(svg.querySelectorAll('text'));
        for (const text of texts) {
            const content = text.textContent.trim();
            if (content === '0' || content === '0.0') {
                const bbox = text.getBBox();
                const textY = bbox.y + bbox.height / 2;
                if (textY >= bounds.y && textY <= bounds.y + bounds.height) return textY;
            }
        }
        return bounds.y + bounds.height / 2;
    """, svg_element, plot_bounds)

def _bitwise_read_tooltip_text(driver, svg_element):
    """Extracts the tooltip text when hovering over the chart."""
    txt = driver.execute_script("""
        const root = arguments[0];
        const selectors = ['g[class*="tooltip"]', 'g.highcharts-label', 'g[class*="highcharts-label"]', 'text[class*="tooltip"]'];
        for (const selector of selectors) {
            const elements = Array.from(root.querySelectorAll(selector));
            const visible = elements.filter(el => {
                try {
                    const style = window.getComputedStyle(el);
                    const bbox = el.getBBox();
                    return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0' && bbox.width > 0 && bbox.height > 0;
                } catch(e) { return false; }
            });
            if (visible.length > 0) {
                const text = visible[visible.length - 1].textContent || '';
                if (text.trim()) return text.trim();
            }
        }
        return '';
    """, svg_element)
    if txt and txt.strip(): return txt.strip()
    return driver.execute_script("""
        const divs = Array.from(document.querySelectorAll('div.highcharts-tooltip, div[class*="tooltip"]'))
            .filter(d => d.offsetWidth > 0 && d.offsetHeight > 0 && window.getComputedStyle(d).display !== 'none');
        if (divs.length) return (divs[divs.length-1].innerText || '').trim();
        return '';
    """).strip()

def _bitwise_parse_tooltip(text):
    """Parses date and basis points from the raw tooltip text."""
    if not text: return None
    m_date = _DATE_RE.search(text)
    if not m_date: return None
    nums = _NUM_RE.findall(text.replace(",", ""))
    if not nums: return None
    return m_date.group(1), float(nums[-1])

def _bitwise_sweep_chart(driver, svg_element, step=1):
    """Performs a horizontal sweep across the chart to collect all data points from tooltips."""
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", svg_element)
    time.sleep(0.5)
    bounds = _bitwise_get_plot_bounds(driver, svg_element)
    zero_y = _bitwise_find_zero_line_y(driver, svg_element, bounds)
    start_x, end_x, target_y = int(bounds['x']), int(bounds['x'] + bounds['width']), int(zero_y)
    rows, seen = [], set()
    for x in range(start_x, end_x + 1, max(1, step)):
        try:
            ActionChains(driver).move_to_element_with_offset(svg_element, x, target_y).perform()
            driver.execute_script("""
                const svg = arguments[0]; const x = arguments[1]; const y = arguments[2];
                const rect = svg.getBoundingClientRect();
                const clientX = rect.left + x; const clientY = rect.top + y;
                ['mouseover', 'mousemove', 'mouseenter'].forEach(et => {
                    svg.dispatchEvent(new MouseEvent(et, {clientX, clientY, bubbles:true, cancelable:true, view:window}));
                });
            """, svg_element, x, target_y)
            time.sleep(0.08)
            parsed = _bitwise_parse_tooltip(_bitwise_read_tooltip_text(driver, svg_element))
            if parsed:
                d_readable, bps = parsed
                d = pd.to_datetime(d_readable, errors="coerce")
                if pd.notna(d):
                    key = d.strftime("%Y%m%d")
                    if key not in seen:
                        seen.add(key)
                        rows.append({"date": key, "bps": bps})
        except: continue
    if not rows: raise RuntimeError("Failed to extract data points from Bitwise chart tooltip.")
    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

def _bitwise_attach_market_and_nav(df_bps, ticker):
    """Downloads historical market prices from YFinance and calculates estimated NAV based on BPS."""
    if df_bps.empty: return pd.DataFrame(columns=["date", "nav", "market price"])
    dmin, dmax = pd.to_datetime(df_bps["date"]).min().date(), pd.to_datetime(df_bps["date"]).max().date()
    hist = yf.Ticker(ticker).history(start=(dmin - datetime.timedelta(days=2)), end=(dmax + datetime.timedelta(days=2)), interval="1d", auto_adjust=False)
    if hist is None or hist.empty: raise RuntimeError(f"YFinance returned no data for '{ticker}'.")
    px = hist.reset_index()
    date_col = "Date" if "Date" in px.columns else px.columns[0]
    px["date"] = pd.to_datetime(px[date_col]).dt.strftime("%Y%m%d")
    px = px[["date", "Close"]].rename(columns={"Close": "market price"})
    merged = df_bps.merge(px, on="date", how="left")
    merged["nav"] = merged.apply(lambda r: float(r["market price"]) / (1.0 + float(r["bps"]) / 10000.0) if pd.notna(r["market price"]) and pd.notna(r["bps"]) else float("nan"), axis=1)
    return merged[["date", "nav", "market price"]].dropna(subset=["market price"]).reset_index(drop=True)

def process_single_etf_bitwise(driver, etf, site_url):
    """Orchestrates the scraping of Bitwise ETF data from the chart and Yahoo Finance."""
    name = etf["name"]
    base = os.path.splitext(etf["output_filename"])[0]
    print(f"\n[ETF] Processing {name} (Bitwise - Highcharts tooltip + yfinance) -> output .{SAVE_FORMAT}")
    print("="*50)
    try:
        driver.get(site_url); polite_sleep()
        accept_cookies_bitwise(driver); polite_sleep()
        driver.execute_script("window.scrollBy(0, 600);")
        time.sleep(1.0)
        svg = _bitwise_find_chart_svg(driver)
        df_bps = _bitwise_sweep_chart(driver, svg, step=1)
        if df_bps.empty:
            msg = "Empty BPS data from chart sweep."
            return False, msg
        df_out = _bitwise_attach_market_and_nav(df_bps, BITWISE_YF_TICKER)
        if df_out.empty:
            msg = "Empty output after attaching market prices."
            return False, msg
        save_dataframe(df_out, base, sheet_name="Historical")
        print(f"[SUCCESS] Bitwise processed ({name})")
        return True, None
    except Exception as e:
        msg = f"Bitwise error: {e}"
        print(f"[BITWISE ERROR] {msg}")
        return False, msg

def main():
    """Standalone execution entry point."""
    etf = {"name": "Bitwise Bitcoin ETF (BITB)", "output_filename": "bitb_dailynav.xlsx"}
    site_url = "https://bitbetf.com/"
    
    driver = setup_driver(headless=False)
    try:
        ok, err = process_single_etf_bitwise(driver, etf, site_url)
        if ok:
            print("[STANDALONE] Bitwise processed successfully.")
        else:
            print(f"[STANDALONE] Bitwise failed: {err}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
