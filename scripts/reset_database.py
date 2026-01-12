#!/usr/bin/env python3
"""
Reset Database Script
=====================
Clears all data tables to allow a fresh full execution.

Usage:
    python scripts/reset_database.py [--confirm]

Options:
    --confirm    Skip confirmation prompt (for automated use)

Tables cleared:
    - etf_daily_data (holdings, NAV, shares, etc.)
    - etf_flows (CMC flow data)
    - btc_prices (Bitcoin prices)
    - scrape_logs (execution logs)

Note: ETF and Provider master data is NOT deleted.
"""

import os
import sys

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.db import init_pool, close_pool, execute_query, test_connection, get_stats


TABLES_TO_CLEAR = [
    "etf_daily_data",
    "etf_flows",
    "btc_prices",
    "scrape_logs",
]


def get_table_counts() -> dict:
    """Get row counts for each table."""
    counts = {}
    for table in TABLES_TO_CLEAR:
        try:
            result = execute_query(f"SELECT COUNT(*) as count FROM {table}", fetch=True)
            counts[table] = result[0]['count'] if result else 0
        except Exception as e:
            counts[table] = f"Error: {e}"
    return counts


def clear_tables(confirm: bool = False) -> bool:
    """Clear all data tables."""

    print("\n" + "=" * 60)
    print("DATABASE RESET SCRIPT")
    print("=" * 60)

    # Test connection
    if not test_connection():
        print("\n❌ Cannot connect to database!")
        print("Make sure DATABASE_URL is set correctly.")
        return False

    print("\n✅ Connected to database")

    # Show current counts
    print("\nCurrent table counts:")
    counts = get_table_counts()
    total_rows = 0
    for table, count in counts.items():
        print(f"  {table}: {count} rows")
        if isinstance(count, int):
            total_rows += count

    if total_rows == 0:
        print("\n⚠️  Tables are already empty. Nothing to do.")
        return True

    # Confirmation
    if not confirm:
        print(f"\n⚠️  WARNING: This will delete {total_rows} rows from the database!")
        print("This action cannot be undone.\n")
        response = input("Type 'YES' to confirm: ")
        if response != "YES":
            print("\n❌ Aborted. No changes made.")
            return False

    # Clear tables
    print("\nClearing tables...")

    for table in TABLES_TO_CLEAR:
        try:
            # Use TRUNCATE for speed, with CASCADE for foreign keys
            execute_query(f"TRUNCATE TABLE {table} CASCADE")
            print(f"  ✅ {table} cleared")
        except Exception as e:
            # Fallback to DELETE if TRUNCATE fails
            try:
                execute_query(f"DELETE FROM {table}")
                print(f"  ✅ {table} cleared (using DELETE)")
            except Exception as e2:
                print(f"  ❌ {table} failed: {e2}")

    # Verify
    print("\nVerifying...")
    counts_after = get_table_counts()
    all_clear = True
    for table, count in counts_after.items():
        status = "✅" if count == 0 else "❌"
        print(f"  {status} {table}: {count} rows")
        if count != 0:
            all_clear = False

    if all_clear:
        print("\n" + "=" * 60)
        print("✅ DATABASE RESET COMPLETE")
        print("=" * 60)
        print("\nYou can now run a full scrape:")
        print("  python main.py --all")
    else:
        print("\n⚠️  Some tables still have data")

    return all_clear


def main():
    confirm = "--confirm" in sys.argv or "-y" in sys.argv

    try:
        if not init_pool():
            print("❌ Failed to initialize database connection")
            sys.exit(1)

        success = clear_tables(confirm=confirm)
        sys.exit(0 if success else 1)

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
