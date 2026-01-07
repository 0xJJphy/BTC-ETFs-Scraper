import os
import time
import glob
import requests
import pandas as pd
from typing import Optional, Tuple

# ============================================================
# Bosera HashKey Bitcoin ETF Scraper
# ============================================================
# 
# Simplified version using direct API download instead of Selenium.
# The API endpoint provides the Excel file directly without browser interaction.
#
# API URL: https://www.bosera.com.hk/api/fundinfo/exporthisnavexcel.do?language=en&fundCode=BTCL
#
# ============================================================

try:
    from core.utils.helpers import (
        polite_sleep, save_dataframe, _safe_remove,
        CSV_DIR, SAVE_FORMAT
    )
except ImportError:
    import sys
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
    from core.utils.helpers import (
        polite_sleep, save_dataframe, _safe_remove,
        CSV_DIR, SAVE_FORMAT
    )

# ======================== CONSTANTS ========================

BOSERA_API_URL = "https://www.bosera.com.hk/api/fundinfo/exporthisnavexcel.do"
DEFAULT_FUND_CODE = "BTCL"
DEFAULT_LANGUAGE = "en"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)


# ======================== COMPATIBILITY STUB ========================

def accept_cookies_bosera(driver):
    """
    Stub function for compatibility with multi_etf_scraper.py.
    Not used in the direct API download approach.
    """
    # No-op: Direct API download doesn't need cookie handling
    return True


# ======================== DIRECT API DOWNLOAD ========================

def download_bosera_excel(
    fund_code: str = DEFAULT_FUND_CODE,
    language: str = DEFAULT_LANGUAGE,
    output_dir: str = None,
    timeout: int = 60
) -> Optional[str]:
    """
    Download historical NAV Excel file directly from Bosera API.
    
    Args:
        fund_code: Fund code (default: BTCL)
        language: Language code (default: en)
        output_dir: Directory to save the file
        timeout: Request timeout in seconds
    
    Returns:
        Path to downloaded file, or None if failed
    """
    if output_dir is None:
        output_dir = CSV_DIR
    
    os.makedirs(output_dir, exist_ok=True)
    
    url = f"{BOSERA_API_URL}?language={language}&fundCode={fund_code}"
    output_path = os.path.join(output_dir, f"bosera_{fund_code}_temp.xlsx")
    
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/vnd.ms-excel, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": f"https://www.bosera.com.hk/en-US/products/fund/detail/{fund_code}",
    }
    
    print(f"[BOSERA] Downloading from API: {url}")
    
    try:
        polite_sleep()
        response = requests.get(url, headers=headers, timeout=timeout, stream=True)
        
        print(f"[BOSERA] Response status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"[BOSERA ERROR] API returned status {response.status_code}")
            return None
        
        # Check content type
        content_type = response.headers.get("Content-Type", "")
        if "html" in content_type.lower():
            print(f"[BOSERA ERROR] Received HTML instead of Excel (possible block)")
            return None
        
        # Save file
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        file_size = os.path.getsize(output_path)
        print(f"[BOSERA] Downloaded: {output_path} ({file_size} bytes)")
        
        if file_size < 1000:
            print(f"[BOSERA ERROR] File too small, likely an error page")
            _safe_remove(output_path)
            return None
        
        return output_path
        
    except requests.exceptions.Timeout:
        print(f"[BOSERA ERROR] Request timed out after {timeout}s")
        return None
    except requests.exceptions.RequestException as e:
        print(f"[BOSERA ERROR] Request failed: {e}")
        return None
    except Exception as e:
        print(f"[BOSERA ERROR] Download failed: {e}")
        _safe_remove(output_path)
        return None


# ======================== EXCEL PARSING ========================

def parse_bosera_usd_counter(xlsx_path: str) -> pd.DataFrame:
    """
    Parse the USD Counter sheet from the downloaded Bosera Excel file.
    
    Args:
        xlsx_path: Path to the Excel file
    
    Returns:
        DataFrame with columns: date, nav, market price
    """
    sheets = pd.read_excel(xlsx_path, sheet_name=None, header=None, dtype=str, engine="openpyxl")
    
    # Find USD sheet
    usd_name = None
    for nm in sheets.keys():
        if "usd" in str(nm).lower():
            usd_name = nm
            break
    
    if usd_name is None:
        usd_name = list(sheets.keys())[1] if len(sheets) > 1 else list(sheets.keys())[0]
    
    print(f"[BOSERA] Using sheet: {usd_name}")
    
    raw = sheets[usd_name].fillna("")
    
    # Find header row
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
            if t_l in low:
                return colnames[low.index(t_l)]
            for i, l in enumerate(low):
                if t_l in l:
                    return colnames[i]
        return None
    
    date_col = pick(df.columns, ["Date", "Date (yyyy/mm/dd)", "date (yyyy/mm/dd)"])
    nav_col = pick(df.columns, ["NAV"])
    mkt_col = pick(df.columns, ["Market Price", "Closing Market Price", "Market price"])
    
    keep = [c for c in [date_col, nav_col, mkt_col] if c]
    if len(keep) < 3:
        raise RuntimeError(f"Missing required columns in Bosera file: {list(df.columns)}")
    
    out = df[keep].copy()
    out = out[~out[date_col].astype(str).str.strip().eq("")]
    out = out.rename(columns={date_col: "date", nav_col: "nav", mkt_col: "market price"})
    
    # Normalize dates
    raw_date = out["date"].astype(str).str.strip()
    dt = pd.to_datetime(raw_date, errors="coerce", infer_datetime_format=True)
    out["date"] = dt.dt.strftime("%Y%m%d").where(~dt.isna(), raw_date)
    
    # Clean numeric columns
    for c in ["nav", "market price"]:
        out[c] = (out[c].astype(str)
                  .str.replace("$", "", regex=False)
                  .str.replace(",", "", regex=False)
                  .str.strip())
    
    # Filter valid numeric NAV values
    out = out[out["nav"].str.match(r"^-?\d+(\.\d+)?$", na=False)]
    
    print(f"[BOSERA] Parsed {len(out)} rows from USD Counter sheet")
    return out[["date", "nav", "market price"]].reset_index(drop=True)


# ======================== MAIN PROCESS ========================

def process_single_etf_bosera(driver, etf: dict, site_url: str) -> Tuple[bool, Optional[str]]:
    """
    Process historical data for Bosera ETF using direct API download.
    
    Note: The 'driver' parameter is kept for compatibility with the main pipeline,
    but is not used in this simplified version.
    
    Args:
        driver: Selenium WebDriver (unused, kept for compatibility)
        etf: ETF configuration dict
        site_url: Site URL (unused, using API directly)
    
    Returns:
        Tuple of (success, error_message)
    """
    name = etf["name"]
    base = os.path.splitext(etf["output_filename"])[0]
    print(f"\n[ETF] Processing {name} (Bosera - Direct API Download) -> output .{SAVE_FORMAT}")
    print("=" * 50)
    
    # Step 1: Download Excel directly from API
    xlsx_path = download_bosera_excel(fund_code=DEFAULT_FUND_CODE)
    
    if not xlsx_path or not os.path.exists(xlsx_path):
        msg = "Failed to download Excel from Bosera API"
        print(f"[BOSERA ERROR] {msg}")
        return False, msg
    
    try:
        # Step 2: Parse USD Counter sheet
        df = parse_bosera_usd_counter(xlsx_path)
        
        # Step 3: Save data
        save_dataframe(df, base, sheet_name="USD Counter")
        
        # Step 4: Cleanup
        _safe_remove(xlsx_path)
        
        print(f"[SUCCESS] ✓ Bosera processed ({name})")
        return True, None
        
    except Exception as e:
        msg = f"Bosera processing error: {e}"
        print(f"[BOSERA ERROR] {msg}")
        _safe_remove(xlsx_path)
        return False, msg


# ======================== STANDALONE EXECUTION ========================

def main():
    """Standalone execution entry point (no Selenium required)."""
    etf = {
        "name": "Bosera HashKey Bitcoin ETF (BTCL)",
        "output_filename": "bosera_dailynav.xlsx"
    }
    site_url = "https://www.bosera.com.hk/en-US/products/fund/detail/BTCL"
    
    # Pass None as driver since we don't need it
    ok, err = process_single_etf_bosera(None, etf, site_url)
    
    if ok:
        print("\n[STANDALONE] Bosera processed successfully.")
    else:
        print(f"\n[STANDALONE] Bosera failed: {err}")


if __name__ == "__main__":
    main()
