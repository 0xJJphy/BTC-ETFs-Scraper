# ============================================================
# db.py - Database Module for BTC ETF Scraper
# ============================================================
# 
# Conexión y operaciones con PostgreSQL/Supabase
#
# Variables de entorno requeridas:
#   DATABASE_URL=postgresql://user:pass@host:port/dbname
#   
# O variables individuales:
#   DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
#
# ============================================================

import os
import logging
from datetime import datetime, date
from typing import Optional, List, Dict, Any
from contextlib import contextmanager

import pandas as pd

# Intentar importar psycopg2 (PostgreSQL driver)
try:
    import psycopg2
    from psycopg2.extras import execute_values, RealDictCursor
    from psycopg2.pool import ThreadedConnectionPool
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False
    print("[DB] Warning: psycopg2 not installed. Run: pip install psycopg2-binary")

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================
# Configuration
# ============================================================

def get_database_url() -> Optional[str]:
    """Get database URL from environment variables."""
    # Opción 1: URL completa
    url = os.environ.get("DATABASE_URL")
    if url:
        return url
    
    # Opción 2: Variables individuales
    host = os.environ.get("DB_HOST")
    port = os.environ.get("DB_PORT", "5432")
    name = os.environ.get("DB_NAME")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")
    
    if all([host, name, user, password]):
        return f"postgresql://{user}:{password}@{host}:{port}/{name}"
    
    return None


# ============================================================
# Connection Pool
# ============================================================

_pool: Optional['ThreadedConnectionPool'] = None


def init_pool(min_conn: int = 1, max_conn: int = 5) -> bool:
    """Initialize the connection pool."""
    global _pool
    
    if not HAS_PSYCOPG2:
        logger.error("psycopg2 not installed")
        return False
    
    url = get_database_url()
    if not url:
        logger.error("DATABASE_URL not configured")
        return False
    
    try:
        _pool = ThreadedConnectionPool(min_conn, max_conn, url)
        logger.info(f"[DB] Connection pool initialized (min={min_conn}, max={max_conn})")
        return True
    except Exception as e:
        logger.error(f"[DB] Failed to initialize pool: {e}")
        return False


def close_pool():
    """Close all connections in the pool."""
    global _pool
    if _pool:
        _pool.closeall()
        _pool = None
        logger.info("[DB] Connection pool closed")


@contextmanager
def get_connection():
    """Get a connection from the pool (context manager)."""
    global _pool
    
    if not _pool:
        if not init_pool():
            raise RuntimeError("Database pool not initialized")
    
    conn = _pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        _pool.putconn(conn)


# ============================================================
# Helper Functions
# ============================================================

def execute_query(query: str, params: tuple = None, fetch: bool = False) -> Optional[List[Dict]]:
    """Execute a query and optionally fetch results."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            if fetch:
                return [dict(row) for row in cur.fetchall()]
    return None


def execute_many(query: str, data: List[tuple]) -> int:
    """Execute a query with multiple parameter sets."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(query, data)
            return cur.rowcount


# ============================================================
# ETF Operations
# ============================================================

def get_etf_id(ticker: str) -> Optional[int]:
    """Get ETF ID by ticker."""
    result = execute_query(
        "SELECT id FROM etfs WHERE ticker = %s",
        (ticker,),
        fetch=True
    )
    return result[0]['id'] if result else None


def get_all_etfs() -> List[Dict]:
    """Get all ETFs."""
    return execute_query(
        "SELECT e.*, p.name as provider_name FROM etfs e JOIN providers p ON e.provider_id = p.id",
        fetch=True
    ) or []


# ============================================================
# Daily Data Operations
# ============================================================

def upsert_daily_data(
    ticker: str,
    date: date,
    nav: Optional[float] = None,
    market_price: Optional[float] = None,
    shares_outstanding: Optional[int] = None,
    holdings_btc: Optional[float] = None,
    volume: Optional[int] = None
) -> bool:
    """Insert or update daily data for an ETF."""
    try:
        execute_query(
            """
            SELECT upsert_daily_data(%s, %s, %s, %s, %s, %s, %s)
            """,
            (ticker, date, nav, market_price, shares_outstanding, holdings_btc, volume)
        )
        return True
    except Exception as e:
        logger.error(f"[DB] Error upserting data for {ticker}: {e}")
        return False


def bulk_upsert_daily_data(data: List[Dict]) -> int:
    """
    Bulk upsert daily data.
    
    Expected dict keys: ticker, date, nav, market_price, shares_outstanding, holdings_btc, volume
    """
    if not data:
        return 0
    
    # Primero, obtener mapping de ticker -> etf_id
    etfs = get_all_etfs()
    ticker_to_id = {e['ticker']: e['id'] for e in etfs}
    
    # Preparar datos
    rows = []
    for row in data:
        etf_id = ticker_to_id.get(row.get('ticker'))
        if not etf_id:
            continue
        rows.append((
            etf_id,
            row.get('date'),
            row.get('nav'),
            row.get('market_price'),
            row.get('shares_outstanding'),
            row.get('holdings_btc'),
            row.get('volume')
        ))
    
    if not rows:
        return 0
    
    query = """
        INSERT INTO etf_daily_data (etf_id, date, nav, market_price, shares_outstanding, holdings_btc, volume)
        VALUES %s
        ON CONFLICT (etf_id, date) DO UPDATE SET
            nav = COALESCE(EXCLUDED.nav, etf_daily_data.nav),
            market_price = COALESCE(EXCLUDED.market_price, etf_daily_data.market_price),
            shares_outstanding = COALESCE(EXCLUDED.shares_outstanding, etf_daily_data.shares_outstanding),
            holdings_btc = COALESCE(EXCLUDED.holdings_btc, etf_daily_data.holdings_btc),
            volume = COALESCE(EXCLUDED.volume, etf_daily_data.volume),
            updated_at = NOW()
    """
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, query, rows)
            return cur.rowcount


def get_daily_data(
    ticker: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = 1000
) -> pd.DataFrame:
    """Get daily data as a DataFrame."""
    conditions = []
    params = []
    
    if ticker:
        conditions.append("e.ticker = %s")
        params.append(ticker)
    if start_date:
        conditions.append("d.date >= %s")
        params.append(start_date)
    if end_date:
        conditions.append("d.date <= %s")
        params.append(end_date)
    
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    query = f"""
        SELECT 
            d.date,
            e.ticker,
            d.nav,
            d.market_price,
            d.shares_outstanding,
            d.holdings_btc,
            d.volume
        FROM etf_daily_data d
        JOIN etfs e ON d.etf_id = e.id
        WHERE {where_clause}
        ORDER BY d.date DESC, e.ticker
        LIMIT %s
    """
    params.append(limit)
    
    result = execute_query(query, tuple(params), fetch=True)
    return pd.DataFrame(result) if result else pd.DataFrame()


def get_latest_data() -> pd.DataFrame:
    """Get the most recent data for all ETFs."""
    result = execute_query(
        "SELECT * FROM v_etf_latest",
        fetch=True
    )
    return pd.DataFrame(result) if result else pd.DataFrame()


# ============================================================
# Flow Operations
# ============================================================

def upsert_flow(
    ticker: str,
    date: date,
    flow_btc: Optional[float] = None,
    flow_usd: Optional[float] = None
) -> bool:
    """Insert or update flow data."""
    etf_id = get_etf_id(ticker)
    if not etf_id:
        logger.warning(f"[DB] ETF not found: {ticker}")
        return False
    
    try:
        execute_query(
            """
            INSERT INTO etf_flows (etf_id, date, flow_btc, flow_usd)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (etf_id, date) DO UPDATE SET
                flow_btc = COALESCE(EXCLUDED.flow_btc, etf_flows.flow_btc),
                flow_usd = COALESCE(EXCLUDED.flow_usd, etf_flows.flow_usd)
            """,
            (etf_id, date, flow_btc, flow_usd)
        )
        return True
    except Exception as e:
        logger.error(f"[DB] Error upserting flow for {ticker}: {e}")
        return False


def bulk_upsert_flows(data: List[Dict]) -> int:
    """Bulk upsert flow data."""
    if not data:
        return 0
    
    etfs = get_all_etfs()
    ticker_to_id = {e['ticker']: e['id'] for e in etfs}
    
    rows = []
    for row in data:
        etf_id = ticker_to_id.get(row.get('ticker'))
        if not etf_id:
            continue
        rows.append((
            etf_id,
            row.get('date'),
            row.get('flow_btc'),
            row.get('flow_usd')
        ))
    
    if not rows:
        return 0
    
    query = """
        INSERT INTO etf_flows (etf_id, date, flow_btc, flow_usd)
        VALUES %s
        ON CONFLICT (etf_id, date) DO UPDATE SET
            flow_btc = COALESCE(EXCLUDED.flow_btc, etf_flows.flow_btc),
            flow_usd = COALESCE(EXCLUDED.flow_usd, etf_flows.flow_usd)
    """
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, query, rows)
            return cur.rowcount


# ============================================================
# BTC Price Operations
# ============================================================

def upsert_btc_price(date: date, price_usd: float) -> bool:
    """Insert or update BTC price."""
    try:
        execute_query(
            """
            INSERT INTO btc_prices (date, price_usd)
            VALUES (%s, %s)
            ON CONFLICT (date) DO UPDATE SET
                price_usd = EXCLUDED.price_usd
            """,
            (date, price_usd)
        )
        return True
    except Exception as e:
        logger.error(f"[DB] Error upserting BTC price: {e}")
        return False


def bulk_upsert_btc_prices(data: List[tuple]) -> int:
    """Bulk upsert BTC prices. data = [(date, price), ...]"""
    if not data:
        return 0
    
    query = """
        INSERT INTO btc_prices (date, price_usd)
        VALUES %s
        ON CONFLICT (date) DO UPDATE SET price_usd = EXCLUDED.price_usd
    """
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, query, data)
            return cur.rowcount


# ============================================================
# Scrape Log Operations
# ============================================================

def start_scrape_log() -> int:
    """Start a new scrape log entry, returns log ID."""
    result = execute_query(
        "INSERT INTO scrape_logs DEFAULT VALUES RETURNING id",
        fetch=True
    )
    return result[0]['id'] if result else 0


def finish_scrape_log(
    log_id: int,
    status: str,
    etfs_processed: int,
    etfs_failed: int,
    error_message: Optional[str] = None
):
    """Update scrape log with final status."""
    execute_query(
        """
        UPDATE scrape_logs SET
            finished_at = NOW(),
            status = %s,
            etfs_processed = %s,
            etfs_failed = %s,
            error_message = %s,
            execution_time_seconds = EXTRACT(EPOCH FROM (NOW() - started_at))::INTEGER
        WHERE id = %s
        """,
        (status, etfs_processed, etfs_failed, error_message, log_id)
    )


# ============================================================
# DataFrame Export/Import
# ============================================================

def df_to_daily_data(df: pd.DataFrame, ticker: str) -> int:
    """
    Import a DataFrame to daily data.
    Expected columns: date, nav, market_price (or 'market price'), 
                     shares_outstanding (optional), holdings_btc (optional)
    """
    if df.empty:
        return 0
    
    etf_id = get_etf_id(ticker)
    if not etf_id:
        logger.warning(f"[DB] ETF not found: {ticker}")
        return 0
    
    # Normalizar nombres de columnas
    df = df.copy()
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]
    
    # Renombrar si es necesario
    if 'market_price' not in df.columns and 'market' in ' '.join(df.columns):
        for col in df.columns:
            if 'market' in col and 'price' in col:
                df = df.rename(columns={col: 'market_price'})
                break
    
    rows = []
    for _, row in df.iterrows():
        try:
            d = pd.to_datetime(row.get('date')).date()
            rows.append((
                etf_id,
                d,
                row.get('nav'),
                row.get('market_price'),
                row.get('shares_outstanding'),
                row.get('holdings_btc'),
                row.get('volume')
            ))
        except Exception:
            continue
    
    if not rows:
        return 0
    
    query = """
        INSERT INTO etf_daily_data (etf_id, date, nav, market_price, shares_outstanding, holdings_btc, volume)
        VALUES %s
        ON CONFLICT (etf_id, date) DO UPDATE SET
            nav = COALESCE(EXCLUDED.nav, etf_daily_data.nav),
            market_price = COALESCE(EXCLUDED.market_price, etf_daily_data.market_price),
            shares_outstanding = COALESCE(EXCLUDED.shares_outstanding, etf_daily_data.shares_outstanding),
            holdings_btc = COALESCE(EXCLUDED.holdings_btc, etf_daily_data.holdings_btc),
            volume = COALESCE(EXCLUDED.volume, etf_daily_data.volume),
            updated_at = NOW()
    """
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, query, rows)
            count = cur.rowcount
            logger.info(f"[DB] Imported {count} rows for {ticker}")
            return count


# ============================================================
# Testing / Health Check
# ============================================================

def test_connection() -> bool:
    """Test database connection."""
    try:
        result = execute_query("SELECT 1 as test", fetch=True)
        return result is not None and result[0]['test'] == 1
    except Exception as e:
        logger.error(f"[DB] Connection test failed: {e}")
        return False


def get_stats() -> Dict[str, Any]:
    """Get database statistics."""
    stats = {}
    
    # Count records
    queries = {
        'total_etfs': "SELECT COUNT(*) FROM etfs",
        'total_daily_records': "SELECT COUNT(*) FROM etf_daily_data",
        'total_flows': "SELECT COUNT(*) FROM etf_flows",
        'total_btc_prices': "SELECT COUNT(*) FROM btc_prices",
        'date_range': """
            SELECT MIN(date) as min_date, MAX(date) as max_date 
            FROM etf_daily_data
        """
    }
    
    for key, query in queries.items():
        try:
            result = execute_query(query, fetch=True)
            if result:
                if key == 'date_range':
                    stats['min_date'] = result[0]['min_date']
                    stats['max_date'] = result[0]['max_date']
                else:
                    stats[key] = result[0]['count']
        except Exception:
            stats[key] = None
    
    return stats


# ============================================================
# Main (for testing)
# ============================================================

if __name__ == "__main__":
    print("Testing database connection...")
    
    if test_connection():
        print("✅ Connection successful!")
        
        stats = get_stats()
        print("\nDatabase stats:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        print("\nETFs in database:")
        for etf in get_all_etfs():
            print(f"  - {etf['ticker']}: {etf['name']}")
    else:
        print("❌ Connection failed!")
        print("\nMake sure to set DATABASE_URL environment variable:")
        print("  export DATABASE_URL=postgresql://user:pass@host:port/dbname")
