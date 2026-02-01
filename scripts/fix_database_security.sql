-- ============================================================
-- Database Security Remediation Script
-- ============================================================
-- Addresses security issues:
-- 1. Sets v_latest_metrics to SECURITY INVOKER
-- 2. Sets v_daily_summary to SECURITY INVOKER
-- 3. Enables RLS on futures_daily_metrics_staging
--
-- Usage:
--   psql $DATABASE_URL -f scripts/fix_database_security.sql
-- ============================================================

DO $$
BEGIN
    RAISE NOTICE 'Starting database security remediation...';

    -- 1. Fix v_latest_metrics
    -- SECURITY INVOKER ensures the view respects the permissions of the querying user.
    IF EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'v_latest_metrics') THEN
        BEGIN
            ALTER VIEW public.v_latest_metrics SET (security_invoker = on);
            RAISE NOTICE '✅ View v_latest_metrics set to SECURITY INVOKER';
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING '⚠️ Could not set v_latest_metrics to SECURITY INVOKER. Error: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'ℹ️ View v_latest_metrics not found, skipping.';
    END IF;

    -- 2. Fix v_daily_summary
    IF EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'v_daily_summary') THEN
        BEGIN
            ALTER VIEW public.v_daily_summary SET (security_invoker = on);
            RAISE NOTICE '✅ View v_daily_summary set to SECURITY INVOKER';
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING '⚠️ Could not set v_daily_summary to SECURITY INVOKER. Error: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'ℹ️ View v_daily_summary not found, skipping.';
    END IF;

    -- 3. Enable RLS on futures_daily_metrics_staging
    -- Prevents unauthorized access when exposed via PostgREST.
    IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'futures_daily_metrics_staging') THEN
        BEGIN
            ALTER TABLE public.futures_daily_metrics_staging ENABLE ROW LEVEL SECURITY;
            RAISE NOTICE '✅ RLS enabled on table futures_daily_metrics_staging';
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING '⚠️ Could not enable RLS on futures_daily_metrics_staging. Error: %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'ℹ️ Table futures_daily_metrics_staging not found, skipping.';
    END IF;

    RAISE NOTICE 'Database security remediation complete.';
END $$;
