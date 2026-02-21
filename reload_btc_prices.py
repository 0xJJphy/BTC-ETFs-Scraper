import os
import sys
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from core.db_adapter import init_database
from core.db import bulk_upsert_btc_prices

def reload_btc_prices():
    print("="*50)
    print("RELOADING BTC PRICES (Yahoo Finance -> DB)")
    print("="*50)

    # 1. Initialize Database
    if not init_database():
        print("[ERROR] Could not connect to database. Check .env configuration.")
        return

    # 2. Define Time Range (Full History for safety)
    start_date = "2024-01-01"
    end_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    
    print(f"[FETCH] Downloading BTC-USD from {start_date} to {end_date}...")

    try:
        # 3. Fetch Data from Yahoo Finance
        ticker = "BTC-USD"
        df = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=False)
        
        if df.empty:
            print("[ERROR] No data downloaded from Yahoo Finance.")
            return

        # Handle MultiIndex columns if present (common in recent yfinance versions)
        if isinstance(df.columns, pd.MultiIndex):
            df = df.xs(ticker, axis=1, level=1, drop_level=True)

        # Reset index to get Date as a column
        df = df.reset_index()
        
        # Standardize column names
        df.columns = [c.lower() for c in df.columns]
        
        if 'date' not in df.columns or 'close' not in df.columns:
            print(f"[ERROR] Unexpected columns from YF: {df.columns}")
            return

        # 4. Prepare Data for DB Upsert
        # Format: List of tuples (date, price_usd)
        prices_data = []
        for _, row in df.iterrows():
            d = row['date'].date()
            p = float(row['close'])
            if p > 0:
                prices_data.append((d, p))

        print(f"[PROCESS] Prepared {len(prices_data)} records for upsert.")

        # 5. Bulk Upsert
        # The function uses ON CONFLICT DO UPDATE, so duplicates are handled safely.
        count = bulk_upsert_btc_prices(prices_data)
        
        print(f"[SUCCESS] ✅ Saved/Updated {count} BTC price records in the database.")
        
    except Exception as e:
        print(f"[ERROR] Failed to reload prices: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    reload_btc_prices()
