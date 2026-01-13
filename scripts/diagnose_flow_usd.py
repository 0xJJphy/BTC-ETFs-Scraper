#!/usr/bin/env python3
"""
Diagnose Flow USD Issue
=======================
Checks why flow_usd is not being calculated.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.db import init_pool, close_pool, execute_query, test_connection

def diagnose():
    print("\n" + "=" * 70)
    print("FLOW USD DIAGNOSTIC")
    print("=" * 70)

    if not init_pool():
        print("\n❌ Cannot connect to database!")
        return

    if not test_connection():
        print("\n❌ Database connection test failed!")
        return

    print("\n✅ Connected to database\n")

    # 1. Check btc_prices table
    print("-" * 70)
    print("1. BTC PRICES TABLE")
    print("-" * 70)

    result = execute_query("SELECT COUNT(*) as count FROM btc_prices", fetch=True)
    btc_count = result[0]['count'] if result else 0
    print(f"   Total records: {btc_count}")

    if btc_count > 0:
        result = execute_query(
            "SELECT MIN(date) as min_date, MAX(date) as max_date FROM btc_prices",
            fetch=True
        )
        if result and result[0]:
            print(f"   Date range: {result[0]['min_date']} to {result[0]['max_date']}")

        result = execute_query(
            "SELECT date, price_usd FROM btc_prices ORDER BY date DESC LIMIT 5",
            fetch=True
        )
        if result:
            print("   Latest prices:")
            for r in result:
                print(f"      {r['date']}: ${r['price_usd']:,.2f}")
    else:
        print("   ⚠️  NO BTC PRICES IN DATABASE!")
        print("   This is why flow_usd cannot be calculated.")

    # 2. Check etf_flows table
    print("\n" + "-" * 70)
    print("2. ETF FLOWS TABLE")
    print("-" * 70)

    result = execute_query("SELECT COUNT(*) as count FROM etf_flows", fetch=True)
    flows_count = result[0]['count'] if result else 0
    print(f"   Total records: {flows_count}")

    if flows_count > 0:
        result = execute_query(
            "SELECT MIN(date) as min_date, MAX(date) as max_date FROM etf_flows",
            fetch=True
        )
        if result and result[0]:
            print(f"   Date range: {result[0]['min_date']} to {result[0]['max_date']}")

        result = execute_query(
            "SELECT COUNT(*) as count FROM etf_flows WHERE flow_btc IS NOT NULL",
            fetch=True
        )
        print(f"   Records with flow_btc: {result[0]['count'] if result else 0}")

        result = execute_query(
            "SELECT COUNT(*) as count FROM etf_flows WHERE flow_usd IS NOT NULL",
            fetch=True
        )
        print(f"   Records with flow_usd: {result[0]['count'] if result else 0}")

    # 3. Check date overlap
    print("\n" + "-" * 70)
    print("3. DATE OVERLAP ANALYSIS")
    print("-" * 70)

    if btc_count > 0 and flows_count > 0:
        result = execute_query("""
            SELECT COUNT(DISTINCT f.date) as count
            FROM etf_flows f
            JOIN btc_prices bp ON f.date = bp.date
        """, fetch=True)
        overlap = result[0]['count'] if result else 0
        print(f"   Dates with both flows AND btc_price: {overlap}")

        if overlap == 0:
            print("\n   ⚠️  NO DATE OVERLAP!")
            print("   Checking date formats...")

            # Sample dates from each table
            result = execute_query(
                "SELECT date FROM etf_flows ORDER BY date DESC LIMIT 3",
                fetch=True
            )
            if result:
                print(f"   Flow dates sample: {[str(r['date']) for r in result]}")

            result = execute_query(
                "SELECT date FROM btc_prices ORDER BY date DESC LIMIT 3",
                fetch=True
            )
            if result:
                print(f"   BTC price dates sample: {[str(r['date']) for r in result]}")
    else:
        print("   Cannot check overlap - missing data in one or both tables")

    # 4. Recommendation
    print("\n" + "-" * 70)
    print("4. DIAGNOSIS")
    print("-" * 70)

    if btc_count == 0:
        print("""
   PROBLEM: btc_prices table is empty!

   The BTC prices are saved in STEP 11 of data_builder.py.
   This happens AFTER the CMC scraper saves flows.

   SOLUTION: Run the full pipeline to populate btc_prices:
      python main.py --all

   Or just the build step (if flows already exist):
      python main.py --build
        """)
    elif flows_count == 0:
        print("""
   PROBLEM: etf_flows table is empty!

   SOLUTION: Run the CMC scraper:
      python main.py --cmc
        """)
    elif overlap == 0:
        print("""
   PROBLEM: Date mismatch between etf_flows and btc_prices!

   The dates in both tables don't match.
   This could be a timezone or date format issue.
        """)
    else:
        print(f"""
   Tables look OK ({overlap} matching dates).

   Try running the calculation manually:
      python scripts/calculate_flow_usd.py
        """)

    print("=" * 70)
    close_pool()


if __name__ == "__main__":
    diagnose()
