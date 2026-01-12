import os
import re
import json
import time
from datetime import datetime, timedelta, date

import numpy as np
import pandas as pd
import yfinance as yf
import holidays

# ======================== PATHS / FILES =========================
BASE_DIR        = "./etfs_data"
CSV_DIR         = os.path.join(BASE_DIR, "csv")
JSON_DIR        = os.path.join(BASE_DIR, "json")
FINAL_DIR       = os.path.join(BASE_DIR, "etfs_completo")

OUTPUT_CSV      = os.path.join(CSV_DIR, "cmc_bitcoin_etf_flows_btc.csv")
OUTPUT_JSON     = os.path.join(JSON_DIR, "cmc_bitcoin_etf_flows_btc.json")

COMPLETE_FILE   = os.path.join(FINAL_DIR, "bitcoin_etf_completo.csv")
STRUCT_JSON     = os.path.join(FINAL_DIR, "bitcoin_etf_completo_estructurado.json")

ETF_DIRECT_DIR  = os.getenv("ETF_DIRECT_DIR", CSV_DIR)

# ======================== UTILITIES ===============================
TICKER_MAP = {
    "BTCO": "BTCO",
    "IBIT": "IBIT",
    "BITB": "BITB",
    "GBTC": "GBTC",
    "FBTC": "FBTC.TO",
    "HODL": "HODL",
    "BRRR": "BRRR",
    "EZBC": "EZBC",
    "ARKB": "ARKB",
    "BTCW": "BTCW",
    "BTC":  "BTC",      # Grayscale Bitcoin Mini Trust (US)
    "CHINAAMC":        "9042.HK",
    "BOSERA&HASHKEY":  "9008.HK",
    "HARVEST":         "9439.HK",
    "BTC-PRICE":       "BTC-USD",  # Coinbase pricing via Yahoo Finance
}

US_ETFS = {"BTCO","IBIT","BITB","GBTC","FBTC","HODL","BRRR","EZBC","ARKB","BTCW","BTC"}
HK_ETFS = {"CHINAAMC","BOSERA&HASHKEY","HARVEST"}
ETF_LIST = list(US_ETFS | HK_ETFS)

# ======================== HOLDINGS SEEDS (BTC) ==================
INITIAL_HOLDINGS_BTC = {
    "GBTC": 621_499.0,
    "BTC":  26_935.0
}

# Flags
OVERRIDE_TODAY_NAV = True        # Allow overwriting today's data from dailynav
ESTIMATE_SHARES_FOR_HK = False   # Avoid mixing HKD in shares calculation

# ======================== UTILITIES ===============================
def safe_read_csv(path: str) -> pd.DataFrame:
    """Read a CSV file safely handling potential errors."""
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path, low_memory=False)
    except Exception:
        return pd.read_csv(path)

def to_date_str(ts) -> str:
    """Normalize timestamp to YYYY-MM-DD string."""
    return pd.to_datetime(ts, errors="coerce").strftime("%Y-%m-%d")

def market_of_etf(etf: str) -> str:
    """Identify the market (US/HK) for a given ETF."""
    return "HK" if etf in HK_ETFS else "US"

def is_trading_day_market(d: date, market: str) -> bool:
    """Determine if 'd' is a business day in the specified market."""
    if isinstance(d, (pd.Timestamp, datetime)): 
        d = d.date()
    if d.weekday() >= 5:  # Saturday/Sunday
        return False
    if market == "US":
        return d not in holidays.UnitedStates(years=d.year)
    if market == "HK":
        return d not in holidays.HongKong(years=d.year)
    return True

def last_trading_day_before(d: date, market: str) -> date:
    """Go back to find the last trading day in the specified market."""
    if isinstance(d, (pd.Timestamp, datetime)):
        d = d.date()
    cur = d - timedelta(days=1)
    for _ in range(15):  # Max 15 days back
        if is_trading_day_market(cur, market):
            return cur
        cur -= timedelta(days=1)
    return d

def detect_etf_first_flow_date(df: pd.DataFrame, etf: str) -> date:
    """Detect the first date with a valid (non-zero) flow for an ETF."""
    if etf not in df.columns:
        return None
    etf_data = df[(df[etf].notna()) & (df[etf] != 0)]
    if etf_data.empty:
        return None
    first_flow_date = pd.to_datetime(etf_data['date']).min()
    detected_date = first_flow_date.date()
    print(f"[ETF-START] {etf}: First flow detected on {detected_date}")
    return detected_date

def get_etf_active_range(df: pd.DataFrame) -> dict:
    """Detect the active date range for each ETF based on flows."""
    etf_ranges = {}
    for etf in ETF_LIST:
        start_date = detect_etf_first_flow_date(df, etf)
        etf_ranges[etf] = start_date
        if start_date:
            print(f"[ETF-RANGE] {etf}: Active since {start_date}")
        else:
            print(f"[ETF-RANGE] {etf}: No flows detected")
    return etf_ranges

def add_missing_calendar_days(df: pd.DataFrame) -> pd.DataFrame:
    """
    Insert all calendar dates between the minimum date and today.
    Does not fill values; only creates rows for later propagation.
    """
    if df.empty or "date" not in df.columns:
        return df
    
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    
    if out["date"].isna().all():
        return df
    
    start = out["date"].min().date()
    end   = max(out["date"].max().date(), datetime.now().date())
    
    print(f"[CALENDAR] Adding missing days from {start} to {end}")
    
    full_idx = pd.date_range(start=start, end=end, freq="D")
    out = (out.set_index("date")
              .reindex(full_idx)
              .rename_axis("date")
              .reset_index())
    
    print(f"[CALENDAR] Expanded DataFrame to {len(out)} rows (full days)")
    return out

# ======================== ETF-DIRECT LOADER ========================
def _detect_etf_from_etfdirect_filename(fname: str):
    """Identify which ETF a file belongs to based on its name."""
    base = os.path.basename(fname).lower()
    m = re.match(r"([a-z0-9\-\&\_]+)_dailynav\.csv$", base)
    if not m: return None
    slug = m.group(1)
    slug_map = {
        "btco":"BTCO","ibit":"IBIT","bitb":"BITB","gbtc":"GBTC","fbtc":"FBTC",
        "hodl":"HODL","brrr":"BRRR","ezbc":"EZBC","arkb":"ARKB","btcw":"BTCW","btc":"BTC",
        "bosera":"BOSERA&HASHKEY","bosera&hashkey":"BOSERA&HASHKEY",
        "harvest":"HARVEST","chinaamc":"CHINAAMC",
    }
    etf = slug_map.get(slug)
    return etf if etf in ETF_LIST else None

def _parse_date_like(series: pd.Series) -> pd.Series:
    """Parse dates in various formats, including YYYYMMDD."""
    if series.empty:
        return pd.Series(dtype="datetime64[ns]")
        
    s = series.astype(str).str.strip()
    out = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    
    # Format YYYYMMDD (8 digits) - main format from your CSVs
    mask8 = s.str.match(r"^\d{8}$")
    if mask8.any():
        try:
            out.loc[mask8] = pd.to_datetime(s.loc[mask8], format="%Y%m%d", errors="coerce")
        except Exception as e:
            print(f"[DATE-PARSE] Error parsing YYYYMMDD format: {e}")
    
    # Other standard formats
    remaining = ~mask8 & s.notna() & (s != 'nan') & (s != 'NaT')
    if remaining.any():
        try:
            out.loc[remaining] = pd.to_datetime(s.loc[remaining], errors="coerce")
        except Exception as e:
            print(f"[DATE-PARSE] Error parsing other formats: {e}")
    
    return out.dt.normalize()

def _to_num(s):
    """Convert a series to numeric, cleaning non-numeric characters."""
    if s is None or len(s) == 0:
        return pd.Series(dtype=float)
    
    clean_s = (pd.Series(s, dtype="object")
               .astype(str)
               .str.replace("\u2014", "", regex=False)     # em dash
               .str.replace("\u2013", "", regex=False)     # en dash  
               .str.replace(",", "", regex=False)          # commas
               .str.strip()                                # spaces
               .replace({"": np.nan, "nan": np.nan, "NaN": np.nan, "None": np.nan}))
    
    try:
        return pd.to_numeric(clean_s, errors="coerce")
    except Exception as e:
        print(f"[NUM-PARSE] Error converting to numeric: {e}")
        return pd.Series([np.nan] * len(s), dtype=float)

def _read_etfdirect_one(path: str) -> pd.DataFrame:
    """Read a single ETF-DIRECT CSV file."""
    try:
        df = None
        for encoding in ['utf-8', 'utf-8-sig', 'latin1', 'cp1252']:
            try:
                df = pd.read_csv(path, encoding=encoding, low_memory=False)
                break
            except UnicodeDecodeError:
                continue
        
        if df is None:
            print(f"[ETF-DIRECT] Could not read {path} with any encoding")
            return pd.DataFrame(columns=["date","nav","close","shares"])
            
        if df.empty: 
            print(f"[ETF-DIRECT] Empty file: {path}")
            return pd.DataFrame(columns=["date","nav","close","shares"])

        df.columns = [col.replace('\ufeff', '').strip() for col in df.columns]
        
        fname = os.path.basename(path)
        print(f"[ETF-DIRECT] Processing {fname} - {len(df)} rows")
        
        result = pd.DataFrame()
        result["date"] = _parse_date_like(df.iloc[:, 0])
        result["nav"] = _to_num(df.iloc[:, 1])
        result["close"] = _to_num(df.iloc[:, 2])
        if len(df.columns) >= 4:
            result["shares"] = _to_num(df.iloc[:, 3])
        else:
            result["shares"] = np.nan

        result = result.dropna(subset=["date"])
        result = result.loc[(result["nav"].notna()) & (result["nav"] > 0)]
        
        if result.empty:
            print(f"[ETF-DIRECT] WARNING: {fname} has no valid data after filtering")
            return pd.DataFrame(columns=["date","nav","close","shares"])
        
        result = result.sort_values("date").drop_duplicates(subset=["date"], keep="last")
        print(f"[ETF-DIRECT] {fname} final: {len(result)} rows, from {result['date'].min()} to {result['date'].max()}")
        
        return result[["date","nav","close","shares"]]
        
    except Exception as e:
        print(f"[ETF-DIRECT] ERROR processing {path}: {str(e)}")
        return pd.DataFrame(columns=["date","nav","close","shares"])

def load_etfdirect_map(base_dir: str) -> dict:
    """Load all ETF-DIRECT CSV files from a directory into a map."""
    found = {}
    if not base_dir or not os.path.isdir(base_dir):
        return found
    for fname in os.listdir(base_dir):
        etf = _detect_etf_from_etfdirect_filename(fname)
        if not etf: continue
        path = os.path.join(base_dir, fname)
        edf = _read_etfdirect_one(path)
        if not edf.empty:
            found[etf] = edf
            print(f"[ETF-DIRECT] {etf}: {len(edf)} rows from {path}")
    return found

def override_from_etfdirect(df: pd.DataFrame, direct_map: dict) -> pd.DataFrame:
    """Overwrite NAV/SHARES/PRICE in the main DataFrame with data from direct sources."""
    if not direct_map or df.empty: 
        print("[ETF-DIRECT] No direct data to process")
        return df

    result = df.copy()
    result["date"] = pd.to_datetime(result["date"], errors="coerce").dt.normalize()
    today = pd.Timestamp(datetime.now().date())

    for etf_name, etf_data in direct_map.items():
        nav_col = f"{etf_name}-NAVSHARE"
        close_col = f"CLOSE-{etf_name}"
        shares_col = f"{etf_name}-SHARES"
        
        for col in [nav_col, close_col, shares_col]:
            if col not in result.columns:
                result[col] = np.nan

        etf_work = etf_data.copy()
        etf_work["date"] = pd.to_datetime(etf_work["date"], errors="coerce").dt.normalize()

        if not OVERRIDE_TODAY_NAV:
            etf_work = etf_work.loc[etf_work["date"] != today]

        common_dates_set = set(result["date"].dropna()).intersection(set(etf_work["date"].dropna()))
        if not common_dates_set:
            continue

        common_mask = result["date"].isin(common_dates_set)

        # Update NAV
        try:
            nav_data = result.loc[common_mask, ["date"]].merge(
                etf_work[["date", "nav"]].dropna(subset=["nav"]), 
                on="date", how="left"
            )
            nav_mask = nav_data["nav"].notna()
            if nav_mask.any():
                original_indices = result.loc[common_mask].index[nav_mask]
                result.loc[original_indices, nav_col] = nav_data.loc[nav_mask, "nav"].values
        except: pass

        # Update Close
        try:
            close_data = result.loc[common_mask, ["date"]].merge(
                etf_work[["date", "close"]].dropna(subset=["close"]),
                on="date", how="left"
            )
            valid = close_data["close"].notna()
            if valid.any():
                idx = result.loc[common_mask].index[valid]
                result.loc[idx, close_col] = close_data.loc[valid, "close"].values
        except: pass

        # Update Shares
        try:
            has_shares = etf_work["shares"].notna().any()
            if has_shares:
                shares_need_mask = common_mask & result[shares_col].isna()
                if shares_need_mask.any():
                    shares_data = result.loc[shares_need_mask, ["date"]].merge(
                        etf_work[["date", "shares"]].dropna(subset=["shares"]), 
                        on="date", how="left"
                    )
                    shares_valid_mask = shares_data["shares"].notna()
                    if shares_valid_mask.any():
                        original_indices = result.loc[shares_need_mask].index[shares_valid_mask]
                        result.loc[original_indices, shares_col] = shares_data.loc[shares_valid_mask, "shares"].values
        except: pass

    return result

# ======================== MARKET DATA (YF) ====================
def fetch_history_one(ticker: str, start: date, end: date) -> pd.DataFrame:
    """Fetch historical close and volume data from Yahoo Finance."""
    try:
        tk = yf.Ticker(ticker)
        df = tk.history(start=(start - timedelta(days=3)),
                        end=(end + timedelta(days=2)),
                        auto_adjust=False, actions=False)
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.copy()
        df.index = pd.to_datetime(df.index).tz_localize(None).normalize()
        return df[["Close","Volume"]].rename(columns={"Close":"close","Volume":"volume"})
    except Exception as e:
        print(f"[YF] Error fetching {ticker}: {e}")
        return pd.DataFrame()

def add_btc_close_coinbase(df: pd.DataFrame) -> pd.DataFrame:
    """Add Bitcoin Price from Coinbase (via Yahoo Finance) to the DataFrame."""
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    dmin, dmax = out["date"].min().date(), out["date"].max().date()
    print(f"[BTC] Downloading BTC-USD (Coinbase) {dmin}..{dmax}")
    hist = fetch_history_one(TICKER_MAP["BTC-PRICE"], dmin, dmax)
    out["CLOSE-BTC-CB"] = np.nan
    common = out["date"].isin(hist.index)
    out.loc[common, "CLOSE-BTC-CB"] = hist.loc[out.loc[common, "date"], "close"].values
    return out

def add_etf_yf_close_volume(df: pd.DataFrame) -> pd.DataFrame:
    """Add ETF Close and Volume data from Yahoo Finance to the DataFrame."""
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    dmin, dmax = out["date"].min().date(), out["date"].max().date()
    for etf in ETF_LIST:
        tkr = TICKER_MAP.get(etf)
        if not tkr: continue
        close_col, vol_col = f"CLOSE-{etf}", f"{etf}-VOLUMEN"
        if close_col not in out.columns: out[close_col] = np.nan
        if vol_col   not in out.columns: out[vol_col]   = np.nan
        print(f"[YF] {etf} ({tkr}) {dmin}..{dmax} -> [DONE]")
        hist = fetch_history_one(tkr, dmin, dmax)
        if hist.empty: continue
        mask = out["date"].isin(hist.index)
        # Only fill if missing (to avoid overwriting direct source data)
        fill_close = mask & out[close_col].isna()
        out.loc[fill_close, close_col] = hist.loc[out.loc[fill_close,"date"], "close"].values
        out.loc[mask, vol_col] = hist.loc[out.loc[mask,"date"], "volume"].values
    return out

# ======================== CALCULATIONS WITH PROPAGATION ================
def ensure_all_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure the DataFrame has all required columns for the output."""
    cols = ["date", "Time (UTC)", "Total"] + ETF_LIST
    navs = [f"{e}-NAVSHARE" for e in ETF_LIST]
    closes = ["CLOSE-BTC-CB"] + [f"CLOSE-{e}" for e in ETF_LIST]
    vols = [f"{e}-VOLUMEN" for e in ETF_LIST]
    holds = [f"{e}-HOLDINGS" for e in ETF_LIST]
    shares= [f"{e}-SHARES" for e in ETF_LIST]
    needed = cols + navs + closes + vols + holds + shares
    out = df.copy()
    for c in needed:
        if c not in out.columns: out[c] = np.nan
    return out.reindex(columns=needed)

def first_active_date(df: pd.DataFrame, etf: str):
    """Detect the first day with any data for a given ETF."""
    mask = (
        df[etf].notna() |
        df[f"{etf}-NAVSHARE"].notna()  |
        df[f"CLOSE-{etf}"].notna()|
        df[f"{etf}-SHARES"].notna()
    )
    if not mask.any(): return None
    return pd.to_datetime(df.loc[mask, "date"]).min().normalize()

def calculate_holdings_cumsum_with_seeds(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates holdings as cumsum of flows plus initial seeds."""
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    out = out.sort_values("date")

    etf_ranges = get_etf_active_range(out)

    for etf in ETF_LIST:
        flow_col = etf
        hold_col = f"{etf}-HOLDINGS"
        if flow_col not in out.columns:
            out[hold_col] = np.nan
            continue

        start = etf_ranges.get(etf)
        if start is None:
            out[hold_col] = np.nan
            continue

        seed = float(INITIAL_HOLDINGS_BTC.get(etf, 0.0))
        flows = pd.to_numeric(out[flow_col], errors="coerce").fillna(0.0)

        running = None
        vals = []
        for dt, f in zip(out["date"], flows):
            if dt.date() < start:
                vals.append(np.nan)
                continue
            if running is None:
                running = seed + f
            else:
                running += f
            if running < 0: running = 0.0
            vals.append(running)

        out[hold_col] = vals
        print(f"[HOLDINGS] {etf}: Calculated from {start} with seed {seed} BTC")

    return out

def _estimate_nav_with_strategies_btc(prev_data, next_data, current_holdings_btc, current_close, current_btc_price):
    """Estimate NAV using multiple weighted strategies."""
    estimated_nav = None
    if pd.isna(current_holdings_btc) or pd.isna(current_btc_price) or current_btc_price <= 0:
        return None, None
    
    # 1) Use previous shares (assuming shares constant)
    if (prev_data and prev_data.get('shares', 0) > 0):
        estimated_nav = (current_holdings_btc * current_btc_price) / prev_data['shares']
    
    # 2) Performance relative to BTC
    if (prev_data and current_close and current_close > 0 and
        prev_data.get('nav', 0) > 0 and prev_data.get('close', 0) > 0):
        etf_perf = current_close / prev_data['close']
        btc_perf = current_btc_price / prev_data['btc']
        if btc_perf > 0:
            nav_perf_adj = prev_data['nav'] * etf_perf
            if estimated_nav and estimated_nav > 0:
                rel = etf_perf / btc_perf
                w_hold = min(0.8, 1 / (1 + abs(rel - 1) * 2))
                estimated_nav = (estimated_nav * w_hold + nav_perf_adj * (1 - w_hold))
            else:
                estimated_nav = nav_perf_adj
    
    # 3) Ratio NAV/Close from previous day
    if (not estimated_nav or estimated_nav <= 0) and prev_data and current_close:
        if prev_data.get('nav',0) > 0 and prev_data.get('close',0) > 0:
            estimated_nav = current_close * (prev_data['nav'] / prev_data['close'])
    
    # 4) Fallback to Close Price
    if (not estimated_nav or estimated_nav <= 0) and current_close:
        estimated_nav = current_close
    
    if estimated_nav is None or estimated_nav <= 0:
        return None, None
    
    # Estimated Shares
    estimated_shares = None
    if estimated_nav > 0 and pd.notna(current_holdings_btc) and current_btc_price > 0:
        estimated_shares = (current_holdings_btc * current_btc_price) / estimated_nav
    
    return estimated_nav, estimated_shares

def estimate_nav_and_shares_trading_days(df: pd.DataFrame, etf: str, etf_start_date: date) -> pd.DataFrame:
    """Estimate missing NAV and Shares for active trading days."""
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    
    close_col, nav_col, hold_col, shr_col = f"CLOSE-{etf}", f"{etf}-NAVSHARE", f"{etf}-HOLDINGS", f"{etf}-SHARES"
    market = market_of_etf(etf)
    if etf_start_date is None: return out
    
    trading_mask = out["date"].apply(lambda d: is_trading_day_market(d, market))
    etf_active_mask = out["date"].dt.date >= etf_start_date
    trading_days = out[trading_mask & etf_active_mask]
    
    print(f"[NAV-EST] Estimating missing trading days for {etf} since {etf_start_date}")
    
    count = 0
    for idx in trading_days.index:
        if pd.notna(out.at[idx, nav_col]) and out.at[idx, nav_col] > 0:
            continue
        
        target_date = out.at[idx, "date"]
        prev_idx = trading_days[trading_days["date"] < target_date]["date"].idxmax() if any(trading_days["date"] < target_date) else None
        next_idx = trading_days[trading_days["date"] > target_date]["date"].idxmin() if any(trading_days["date"] > target_date) else None
        
        def get_data(i):
            if i is None: return None
            return {
                'nav': out.at[i, nav_col], 'close': out.at[i, close_col],
                'holdings': out.at[i, hold_col], 'shares': out.at[i, shr_col],
                'btc': out.at[i, "CLOSE-BTC-CB"]
            }
        
        est_nav, est_shr = _estimate_nav_with_strategies_btc(
            get_data(prev_idx), get_data(next_idx),
            out.at[idx, hold_col], out.at[idx, close_col], out.at[idx, "CLOSE-BTC-CB"]
        )
        if est_nav:
            out.at[idx, nav_col] = est_nav
            if est_shr: out.at[idx, shr_col] = est_shr
            count += 1
            
    if count > 0: print(f"[NAV-EST] {etf}: Estimated {count} trading days -> [DONE]")
    else: print(f"[NAV-EST] {etf}: No estimation needed -> [DONE]")
    return out

def estimate_missing_shares(df: pd.DataFrame) -> pd.DataFrame:
    """Fallback estimation of shares using the basic formula."""
    out = df.copy()
    for etf in ETF_LIST:
        if (etf in HK_ETFS) and not ESTIMATE_SHARES_FOR_HK: continue
        nav_col, shr_col, hold_col = f"{etf}-NAVSHARE", f"{etf}-SHARES", f"{etf}-HOLDINGS"
        mask = out[shr_col].isna() & out[nav_col].notna() & out[hold_col].notna() & out["CLOSE-BTC-CB"].notna()
        if not mask.any(): continue
        nav, hold, btc = out.loc[mask, nav_col], out.loc[mask, hold_col], out.loc[mask, "CLOSE-BTC-CB"]
        out.loc[mask, shr_col] = (hold * btc) / nav
    return out

def propagate_weekend_holidays_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Propagates business day data to weekends and holidays per market (US/HK).
    Matches the logic from build_etf_data.py for consistency.
    """
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    out = out.sort_values("date")
    
    print(f"\n[PROPAGATE] Starting propagation for weekends and holidays")
    
    for etf in ETF_LIST:
        market = market_of_etf(etf)
        etf_start_date = first_active_date(out, etf)
        
        if etf_start_date is None:
            print(f"[PROPAGATE] {etf}: No start date found, skipping")
            continue
            
        nav_col = f"{etf}-NAVSHARE"
        close_col = f"CLOSE-{etf}"
        vol_col = f"{etf}-VOLUMEN"
        hold_col = f"{etf}-HOLDINGS"
        shr_col = f"{etf}-SHARES"
        
        print(f"[PROPAGATE] {etf} ({market} market): Propagating from {etf_start_date}")
        
        active_mask = out["date"] >= etf_start_date
        is_trading = out["date"].apply(lambda d: is_trading_day_market(d, market)) & active_mask
        is_non_trading = (~is_trading) & active_mask
        
        for col in [nav_col, close_col, vol_col, hold_col, shr_col]:
            if col not in out.columns:
                continue
            trading_values = out[col].where(is_trading)
            propagated_values = trading_values.ffill()
            fill_mask = is_non_trading & out[col].isna() & propagated_values.notna()
            
            if fill_mask.any():
                out.loc[fill_mask, col] = propagated_values[fill_mask]
                print(f"  {col}: {fill_mask.sum()} values propagated to non-trading days")
        
        print(f"  {etf}: {is_trading.sum()} trading days, {is_non_trading.sum()} non-trading days")
    
    return out

# ======================== STRUCTURED JSON =======================
def create_structured_json(df: pd.DataFrame, output_path: str):
    """Generates a structured JSON file with calculation notes."""
    dff = df.copy()
    dff["date"] = pd.to_datetime(dff["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    meta = {
        "metadata": {
            "total_records": int(len(dff)),
            "date_range": {
                "start_date": str(dff["date"].min()),
                "end_date": str(dff["date"].max())
            },
            "etfs_included": ETF_LIST,
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "calculation_notes": {
                "holdings_unit": "BTC",
                "nav_unit": "USD",
                "shares_calculation": "SHARES = (Holdings_BTC * BTC_Price_USD) / NAV_USD",
                "initial_holdings_seeds_btc": INITIAL_HOLDINGS_BTC,
                "weekend_holiday_propagation": "Values propagated from last trading day back to previous trading day per market (US/HK)"
            }
        },
        "daily_data": []
    }

    for _, r in dff.sort_values("date", ascending=False).iterrows():
        rec = {
            "date": r["date"],
            "time_utc": (None if pd.isna(r.get("Time (UTC)")) else r.get("Time (UTC)")),
            "bitcoin_price": (None if pd.isna(r.get("CLOSE-BTC-CB")) else float(r.get("CLOSE-BTC-CB"))),
            "total_flows": (None if pd.isna(r.get("Total")) else float(r.get("Total"))),
            "etfs": {}
        }
        for e in ETF_LIST:
            f = r.get(e)
            n = r.get(f"{e}-NAVSHARE")
            c = r.get(f"CLOSE-{e}")
            v = r.get(f"{e}-VOLUMEN")
            h = r.get(f"{e}-HOLDINGS")
            s = r.get(f"{e}-SHARES")
            obj = {
                "flows": float(f) if pd.notna(f) else None,
                "nav_share": float(n) if pd.notna(n) else None,
                "close_price": float(c) if pd.notna(c) else None,
                "volume": float(v) if pd.notna(v) else None,
                "holdings_btc": float(h) if pd.notna(h) else None,
                "shares": float(s) if pd.notna(s) else None
            }
            if any(val is not None for val in obj.values()): rec["etfs"][e] = obj
        meta["daily_data"].append(rec)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    print(f"[JSON] Saved: {output_path}")

# ======================== FLOW DATA LOADING =================
def load_flows_from_db() -> pd.DataFrame:
    """
    Load all flows from the database in wide format.

    This ensures correct holdings calculation even when CMC scraper
    runs in incremental mode (which only saves new days to CSV).

    Returns:
        DataFrame with flows or empty DataFrame if DB unavailable
    """
    try:
        from core.db_adapter import is_db_enabled, init_database
        from core.db import get_all_flows_wide_format

        if not is_db_enabled():
            init_database()

        if is_db_enabled():
            df = get_all_flows_wide_format()
            if not df.empty:
                print(f"[BUILD] ✅ Loaded {len(df)} days of flows from DATABASE")
                return df
            else:
                print("[BUILD] Database enabled but no flows found")
        else:
            print("[BUILD] Database not available")
    except ImportError as e:
        print(f"[BUILD] Database modules not available: {e}")
    except Exception as e:
        print(f"[BUILD] Error loading from database: {e}")

    return pd.DataFrame()


def load_flows_from_csv() -> pd.DataFrame:
    """
    Load flows from CSV file (fallback when DB not available).

    Returns:
        DataFrame with flows or empty DataFrame if file not found
    """
    if not os.path.exists(OUTPUT_CSV):
        print(f"[BUILD] CSV file not found: {OUTPUT_CSV}")
        return pd.DataFrame()

    df = pd.read_csv(OUTPUT_CSV)
    if not df.empty:
        print(f"[BUILD] Loaded {len(df)} days of flows from CSV")
    return df


# ======================== MAIN RUNNER =======================
def run():
    """Main pipeline execution for data building and aggregation."""
    os.makedirs(os.path.dirname(COMPLETE_FILE), exist_ok=True)

    # CRITICAL: Load flows from DB first (has complete historical data)
    # The CSV may only contain recent days when CMC scraper runs in incremental mode
    new_df = load_flows_from_db()

    # Fallback to CSV if DB not available or empty
    if new_df.empty:
        print("[BUILD] Falling back to CSV...")
        new_df = load_flows_from_csv()

    if new_df.empty:
        print(f"[ERROR] No flow data available from DB or CSV")
        return

    print(f"[BUILD] Processing {len(new_df)} days of flow data")
    
    # Robust date column detection
    if "date" not in new_df.columns and not new_df.empty:
        # Fallback to the first column if 'date' is missing (e.g., it might be 'Time')
        print(f"[BUILD] Warning: 'date' column not found, using first column '{new_df.columns[0]}'")
        new_df.rename(columns={new_df.columns[0]: "date"}, inplace=True)
        
    new_df["date"] = pd.to_datetime(new_df["date"], errors="coerce").dt.normalize()
    new_df = new_df.dropna(subset=["date"]).sort_values("date")

    df = ensure_all_columns(new_df)
    print("\n[STEP 1] Adding calendar days...")
    df = add_missing_calendar_days(df)
    print("\n[STEP 2] Calculating holdings...")
    df = calculate_holdings_cumsum_with_seeds(df)
    print("\n[STEP 3] Fetching BTC price history...")
    df = add_btc_close_coinbase(df)
    print("\n[STEP 4] Fetching ETF market data...")
    df = add_etf_yf_close_volume(df)
    print("\n[STEP 5] Applying direct source overrides...")
    try:
        m = load_etfdirect_map(ETF_DIRECT_DIR)
        df = override_from_etfdirect(df, m)
    except Exception as e: print(f"[DIRECT-ERR] {e}")
    print("\n[STEP 6] Estimating NAV/SHARES for trading days...")
    ranges = get_etf_active_range(df)
    for etf in ETF_LIST:
        start = ranges.get(etf)
        if start: df = estimate_nav_and_shares_trading_days(df, etf, start)
    print("\n[STEP 7] Calculating remaining SHARES...")
    df = estimate_missing_shares(df)
    print("\n[STEP 8] Propagating data to weekends/holidays...")
    df = propagate_weekend_holidays_data(df)

    # Export
    print("\n[STEP 9] Exporting final results...")
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    df = df.sort_values("date", ascending=False).reset_index(drop=True)
    df.to_csv(COMPLETE_FILE, index=False)
    print(f"[CSV] Saved: {COMPLETE_FILE} ({len(df)} rows)")
    create_structured_json(df, STRUCT_JSON)
    
    # Save enriched data to database
    print("\n[STEP 10] Saving enriched data to database...")
    try:
        from core.db_adapter import is_db_enabled, init_database
        from core.db import save_completed_etf_data, bulk_upsert_btc_prices
        
        if not is_db_enabled():
            init_database()
        
        if is_db_enabled():
            count = save_completed_etf_data(df)
            print(f"[DB] ✅ Saved {count} enriched records to database")
            
            # Save BTC prices to btc_prices table for AUM calculation
            print("\n[STEP 11] Saving BTC prices to database...")
            btc_prices = []
            for _, row in df.iterrows():
                try:
                    date_val = pd.to_datetime(row.get('date')).date()
                    btc_price = row.get('CLOSE-BTC-CB')
                    if pd.notna(btc_price) and float(btc_price) > 0:
                        btc_prices.append((date_val, float(btc_price)))
                except Exception:
                    continue
            
            if btc_prices:
                btc_count = bulk_upsert_btc_prices(btc_prices)
                print(f"[DB] ✅ Saved {btc_count} BTC prices to database")
            else:
                print("[DB] No BTC prices to save")
        else:
            print("[DB] Database not enabled, skipping DB save")
    except ImportError as e:
        print(f"[DB] Database modules not available: {e}")
    except Exception as e:
        print(f"[DB] Error saving to database: {e}")
    
    print(f"\n" + "-"*50)
    print(f"PIPELINE SUCCESS: Aggregated data ready.")
    print(f"Files generated:\n - {COMPLETE_FILE}\n - {STRUCT_JSON}")
    print("-"*50)

if __name__ == "__main__":
    run()