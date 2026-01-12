-- ============================================================
-- Reset Database Script
-- ============================================================
-- Clears all data tables to allow a fresh full execution.
--
-- Usage:
--   psql $DATABASE_URL -f scripts/reset_database.sql
--
-- Note: ETF and Provider master data is NOT deleted.
-- ============================================================

-- Show current counts before clearing
SELECT 'BEFORE RESET' as status;
SELECT 'etf_daily_data' as table_name, COUNT(*) as rows FROM etf_daily_data
UNION ALL
SELECT 'etf_flows', COUNT(*) FROM etf_flows
UNION ALL
SELECT 'btc_prices', COUNT(*) FROM btc_prices
UNION ALL
SELECT 'scrape_logs', COUNT(*) FROM scrape_logs;

-- Clear tables (TRUNCATE is faster than DELETE)
TRUNCATE TABLE etf_daily_data CASCADE;
TRUNCATE TABLE etf_flows CASCADE;
TRUNCATE TABLE btc_prices CASCADE;
TRUNCATE TABLE scrape_logs CASCADE;

-- Verify tables are empty
SELECT 'AFTER RESET' as status;
SELECT 'etf_daily_data' as table_name, COUNT(*) as rows FROM etf_daily_data
UNION ALL
SELECT 'etf_flows', COUNT(*) FROM etf_flows
UNION ALL
SELECT 'btc_prices', COUNT(*) FROM btc_prices
UNION ALL
SELECT 'scrape_logs', COUNT(*) FROM scrape_logs;

SELECT '✅ Database reset complete!' as message;
