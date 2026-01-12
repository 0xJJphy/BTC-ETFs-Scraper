#!/usr/bin/env python3
"""
Calculate Flow USD Script
=========================
Calculates and updates flow_usd values using flow_btc * btc_price.

Usage:
    python scripts/calculate_flow_usd.py

This script:
1. Finds all etf_flows records where flow_usd is NULL but flow_btc exists
2. Looks up the BTC price for each date from btc_prices table
3. Calculates flow_usd = flow_btc * btc_price
4. Updates the records in the database
"""

import os
import sys

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.db import init_pool, close_pool, execute_query, test_connection

def calculate_flow_usd() -> int:
    """
    Calculate and update flow_usd for all flows that have flow_btc but no flow_usd.

    Returns:
        Number of records updated
    """
    print("\n" + "=" * 60)
    print("CALCULATE FLOW USD")
    print("=" * 60)

    # Check connection
    if not test_connection():
        print("\n❌ Cannot connect to database!")
        return 0

    print("\n✅ Connected to database")

    # Check how many records need updating
    check_query = """
        SELECT COUNT(*) as count
        FROM etf_flows f
        WHERE f.flow_btc IS NOT NULL
          AND f.flow_usd IS NULL
    """
    result = execute_query(check_query, fetch=True)
    pending_count = result[0]['count'] if result else 0

    print(f"\nRecords with flow_btc but no flow_usd: {pending_count}")

    if pending_count == 0:
        print("\n✅ All flow_usd values are already calculated!")
        return 0

    # Check how many have matching BTC prices
    match_query = """
        SELECT COUNT(*) as count
        FROM etf_flows f
        JOIN btc_prices bp ON f.date = bp.date
        WHERE f.flow_btc IS NOT NULL
          AND f.flow_usd IS NULL
    """
    result = execute_query(match_query, fetch=True)
    match_count = result[0]['count'] if result else 0

    print(f"Records with matching BTC price: {match_count}")

    if match_count == 0:
        print("\n⚠️  No BTC prices available for the flow dates!")
        print("Run the scraper first to get BTC prices.")
        return 0

    # Update flow_usd = flow_btc * btc_price
    print(f"\nCalculating flow_usd for {match_count} records...")

    update_query = """
        UPDATE etf_flows f
        SET flow_usd = f.flow_btc * bp.price_usd,
            updated_at = NOW()
        FROM btc_prices bp
        WHERE f.date = bp.date
          AND f.flow_btc IS NOT NULL
          AND f.flow_usd IS NULL
    """

    try:
        execute_query(update_query)

        # Verify the update
        verify_query = """
            SELECT COUNT(*) as count
            FROM etf_flows
            WHERE flow_usd IS NOT NULL
        """
        result = execute_query(verify_query, fetch=True)
        total_with_usd = result[0]['count'] if result else 0

        # Check remaining nulls
        remaining_query = """
            SELECT COUNT(*) as count
            FROM etf_flows
            WHERE flow_btc IS NOT NULL AND flow_usd IS NULL
        """
        result = execute_query(remaining_query, fetch=True)
        remaining = result[0]['count'] if result else 0

        print(f"\n✅ Updated {match_count} records")
        print(f"   Total records with flow_usd: {total_with_usd}")

        if remaining > 0:
            print(f"   ⚠️  {remaining} records still missing flow_usd (no BTC price for those dates)")

        return match_count

    except Exception as e:
        print(f"\n❌ Error updating records: {e}")
        return 0


def show_sample():
    """Show a sample of the updated data."""
    sample_query = """
        SELECT
            f.date,
            e.ticker,
            f.flow_btc,
            bp.price_usd as btc_price,
            f.flow_usd
        FROM etf_flows f
        JOIN etfs e ON f.etf_id = e.id
        JOIN btc_prices bp ON f.date = bp.date
        WHERE f.flow_usd IS NOT NULL
        ORDER BY f.date DESC, e.ticker
        LIMIT 10
    """

    result = execute_query(sample_query, fetch=True)

    if result:
        print("\n" + "-" * 80)
        print("Sample of updated records:")
        print("-" * 80)
        print(f"{'Date':<12} {'Ticker':<10} {'Flow BTC':>15} {'BTC Price':>12} {'Flow USD':>15}")
        print("-" * 80)
        for row in result:
            print(f"{str(row['date']):<12} {row['ticker']:<10} {row['flow_btc']:>15.2f} {row['btc_price']:>12.2f} {row['flow_usd']:>15.2f}")


def main():
    try:
        if not init_pool():
            print("❌ Failed to initialize database connection")
            sys.exit(1)

        count = calculate_flow_usd()

        if count > 0:
            show_sample()

        print("\n" + "=" * 60)
        print("DONE")
        print("=" * 60)

        sys.exit(0)

    except KeyboardInterrupt:
        print("\n\n❌ Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
    finally:
        close_pool()


if __name__ == "__main__":
    main()
