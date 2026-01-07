# ============================================================
# db_adapter.py - Adapter to save scraper data to database
# ============================================================
#
# Este módulo conecta los scrapers existentes con la base de datos.
# Intercepta los datos antes de guardar en CSV/JSON y los guarda en DB.
#
# ============================================================

import os
import logging
from datetime import datetime
from typing import Optional, Dict, List

import pandas as pd

from core.db import (
    init_pool, close_pool, test_connection,
    df_to_daily_data, bulk_upsert_flows, bulk_upsert_btc_prices,
    start_scrape_log, finish_scrape_log, get_stats
)

logger = logging.getLogger(__name__)

# ============================================================
# Ticker Mapping (filename -> database ticker)
# ============================================================

FILENAME_TO_TICKER = {
    'gbtc_dailynav': 'GBTC',
    'btc_dailynav': 'BTC',
    'ibit_dailynav': 'IBIT',
    'btco_dailynav': 'BTCO',
    'ezbc_dailynav': 'EZBC',
    'fbtc_dailynav': 'FBTC',
    'hodl_dailynav': 'HODL',
    'arkb_dailynav': 'ARKB',
    'brrr_dailynav': 'BRRR',
    'bosera_dailynav': 'BTCL',
    'harvest_dailynav': 'BTCETF',
    'chinaamc_dailynav': '9042',
    'bitb_dailynav': 'BITB',
    'btcw_dailynav': 'BTCW',
}

# CMC column name -> ticker
CMC_COLUMN_TO_TICKER = {
    'GBTC': 'GBTC',
    'BTC': 'BTC',
    'IBIT': 'IBIT',
    'BTCO': 'BTCO',
    'EZBC': 'EZBC',
    'FBTC': 'FBTC',
    'HODL': 'HODL',
    'ARKB': 'ARKB',
    'BRRR': 'BRRR',
    'BOSERA&HASHKEY': 'BTCL',
    'HARVEST': 'BTCETF',
    'CHINAAMC': '9042',
    'BITB': 'BITB',
    'BTCW': 'BTCW',
}


# ============================================================
# Global State
# ============================================================

_db_enabled = False
_current_log_id = None


def is_db_enabled() -> bool:
    """Check if database is enabled and connected."""
    return _db_enabled


def get_last_cmc_flow_date():
    """
    Get the last CMC flow date from DB for incremental fetching.
    Returns None if DB not enabled or no data exists.
    """
    if not _db_enabled:
        return None
    try:
        from core.db import get_last_flow_date
        return get_last_flow_date()
    except Exception:
        return None

def init_database() -> bool:
    """Initialize database connection."""
    global _db_enabled
    
    # Check if DATABASE_URL is set
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        logger.info("[DB] DATABASE_URL not set, database disabled")
        _db_enabled = False
        return False
    
    # Try to connect
    if init_pool():
        if test_connection():
            _db_enabled = True
            logger.info("[DB] ✅ Database enabled and connected")
            return True
    
    logger.warning("[DB] ❌ Database connection failed, falling back to CSV/JSON")
    _db_enabled = False
    return False


def close_database():
    """Close database connection."""
    global _db_enabled
    close_pool()
    _db_enabled = False


# ============================================================
# Scrape Session Management
# ============================================================

def start_session() -> Optional[int]:
    """Start a scraping session (for logging)."""
    global _current_log_id
    
    if not _db_enabled:
        return None
    
    try:
        _current_log_id = start_scrape_log()
        logger.info(f"[DB] Started scrape session #{_current_log_id}")
        return _current_log_id
    except Exception as e:
        logger.error(f"[DB] Failed to start session: {e}")
        return None


def end_session(success: bool, processed: int, failed: int, error: str = None):
    """End a scraping session."""
    global _current_log_id
    
    if not _db_enabled or not _current_log_id:
        return
    
    try:
        status = 'success' if success else 'failed'
        finish_scrape_log(_current_log_id, status, processed, failed, error)
        logger.info(f"[DB] Ended session #{_current_log_id}: {status}")
    except Exception as e:
        logger.error(f"[DB] Failed to end session: {e}")
    finally:
        _current_log_id = None


# ============================================================
# Data Saving Functions
# ============================================================

def save_etf_dataframe(df: pd.DataFrame, base_name: str) -> int:
    """
    Save ETF DataFrame to database.
    
    Args:
        df: DataFrame with columns: date, nav, market_price, etc.
        base_name: Base filename (e.g., 'gbtc_dailynav')
    
    Returns:
        Number of rows saved
    """
    if not _db_enabled:
        return 0
    
    if df.empty:
        return 0
    
    # Get ticker from filename
    ticker = FILENAME_TO_TICKER.get(base_name)
    if not ticker:
        logger.warning(f"[DB] Unknown ETF filename: {base_name}")
        return 0
    
    try:
        count = df_to_daily_data(df, ticker)
        logger.info(f"[DB] Saved {count} rows for {ticker}")
        return count
    except Exception as e:
        logger.error(f"[DB] Error saving {ticker}: {e}")
        return 0


def save_cmc_flows(df: pd.DataFrame) -> int:
    """
    Save CMC flows DataFrame to database.
    
    Args:
        df: DataFrame with columns: date, and ETF tickers as columns
    
    Returns:
        Number of rows saved
    """
    if not _db_enabled:
        return 0
    
    if df.empty:
        return 0
    
    try:
        # Convertir de formato ancho a largo
        # El df tiene columnas: date, GBTC, IBIT, BTCO, etc.
        records = []
        
        for _, row in df.iterrows():
            try:
                date = pd.to_datetime(row.get('date')).date()
            except Exception:
                continue
            
            for col, ticker in CMC_COLUMN_TO_TICKER.items():
                if col in row and pd.notna(row[col]):
                    records.append({
                        'ticker': ticker,
                        'date': date,
                        'flow_btc': float(row[col]) if row[col] else None
                    })
        
        if records:
            count = bulk_upsert_flows(records)
            logger.info(f"[DB] Saved {count} flow records")
            return count
        
        return 0
        
    except Exception as e:
        logger.error(f"[DB] Error saving flows: {e}")
        return 0


def save_btc_prices(prices: List[tuple]) -> int:
    """
    Save BTC prices to database.
    
    Args:
        prices: List of (date, price_usd) tuples
    
    Returns:
        Number of rows saved
    """
    if not _db_enabled:
        return 0
    
    if not prices:
        return 0
    
    try:
        count = bulk_upsert_btc_prices(prices)
        logger.info(f"[DB] Saved {count} BTC prices")
        return count
    except Exception as e:
        logger.error(f"[DB] Error saving BTC prices: {e}")
        return 0


# ============================================================
# Enhanced save_dataframe (replaces helpers.save_dataframe)
# ============================================================

def save_dataframe_with_db(df: pd.DataFrame, base_name: str, sheet_name: str = "Historical"):
    """
    Enhanced save_dataframe that saves to both CSV/JSON and database.
    
    This function can replace helpers.save_dataframe when DB is enabled.
    """
    from core.utils.helpers import save_dataframe as save_to_file
    
    # Always save to file (CSV/JSON) as backup
    file_path = save_to_file(df, base_name, sheet_name)
    
    # Also save to database if enabled
    if _db_enabled:
        save_etf_dataframe(df, base_name)
    
    return file_path


# ============================================================
# CLI / Testing
# ============================================================

def print_db_status():
    """Print database status and stats."""
    print("\n" + "=" * 50)
    print("DATABASE STATUS")
    print("=" * 50)
    
    db_url = os.environ.get('DATABASE_URL', 'Not set')
    # Ocultar contraseña
    if '@' in db_url:
        masked = db_url.split('@')[0].rsplit(':', 1)[0] + ':***@' + db_url.split('@')[1]
    else:
        masked = db_url
    print(f"DATABASE_URL: {masked}")
    
    if init_database():
        print("Status: ✅ Connected")
        
        stats = get_stats()
        print("\nStatistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
    else:
        print("Status: ❌ Not connected")
    
    print("=" * 50)


if __name__ == "__main__":
    print_db_status()
