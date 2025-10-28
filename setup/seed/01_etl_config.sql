/*
==========================
setup/seed/seed_01_etl_config.sql
==========================

Purpose:
---------
- Create an idempotent key-value configuration table `public.etl_config` and seed it with base file paths used by Bronze seeders.

Parameters:
-----------
- None.

Design & idempotency:
---------------------
- Table created with `CREATE TABLE IF NOT EXISTS`.
- Inserts use `ON CONFLICT DO NOTHING` so existing values are preserved; safe for re-runs.

Usage:
------
- Execute against your target warehouse database (e.g., `sql_retail_analytics_warehouse`):
  psql -d <db> -f setup/seed/seed_01_etl_config.sql

Notes:
------
- The script sets `search_path = public, bronze` so unqualified objects land in `public`.
- Ensure the provided paths end with a trailing slash; they must be reachable by the DB server if used for server-side COPY.
*/

-- Ensure target schema exists (harmless if already present)
CREATE SCHEMA IF NOT EXISTS public;

-- Resolve unqualified names into PUBLIC first (then BRONZE)
SET search_path = public, bronze;

/*
Fail-fast / informative notice: if the config table already exists, inform the operator.
This helps avoid accidental surprises when re-running the script and documents intent.
*/
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'etl_config'
  ) THEN
    RAISE NOTICE 'Notice: public.etl_config already exists - the script will only seed missing keys.';
  END IF;
END$$;

-- Create the config table (idempotent)
CREATE TABLE IF NOT EXISTS public.etl_config (
    config_key   VARCHAR(100) PRIMARY KEY,
    config_value VARCHAR(200) NOT NULL
);

-- Seed defaults (idempotent: DO NOTHING on key conflict)
INSERT INTO public.etl_config (config_key, config_value)
VALUES
  ('base_path_crm', 'C:/Users/Laurent/Studies/sql-ultimate-course/Udemy-SQL-Data-Warehouse-Project/datasets/crm/'),
  ('base_path_erp', 'C:/Users/Laurent/Studies/sql-ultimate-course/Udemy-SQL-Data-Warehouse-Project/datasets/erp/')
ON CONFLICT (config_key) DO NOTHING;

-- Optional: quick confirmation output
SELECT
    config_key,
    config_value
FROM public.etl_config
WHERE config_key
   IN ('base_path_crm','base_path_erp')
ORDER BY config_key;

/*
=================
Testing Queries:
=================
1) Verify table existence and structure
SELECT
  table_schema,
  table_name
FROM information_schema.tables
WHERE table_schema='public'
  AND table_name='etl_config';
-- Expect: one row returned (the config table exists)

2) Check seeded keys and non-NULL values
SELECT
  config_key,
  config_value,
  (config_value IS NULL OR trim(config_value)='') AS is_empty
FROM public.etl_config
ORDER BY config_key;
-- Expect: each required key (e.g. base_path_crm, base_path_erp) present and is_empty = false

3) Validate both required keys exist, regardless of any others
SELECT
  COUNT(*) FILTER (WHERE config_key = 'base_path_crm') > 0 AS has_base_path_crm,
  COUNT(*) FILTER (WHERE config_key = 'base_path_erp') > 0 AS has_base_path_erp
FROM public.etl_config;
-- Expect: return two booleans — both should be true if those rows exist

4) Idempotency test (re-run should not duplicate or alter rows)
WITH before_ct AS (
  SELECT
    COUNT(*) AS n
  FROM public.etl_config
)
SELECT
  (SELECT n FROM before_ct) AS count_before,
  (SELECT COUNT(*) FROM public.etl_config) AS count_after,
  (SELECT COUNT(*) FROM public.etl_config) - (SELECT n FROM before_ct) AS delta;
-- Expect: delta = 0 after a second run

5) Downstream compatibility to check that seed_load_jobs_from_config.sql can read base paths correctly
WITH cfg AS (
  SELECT
    MAX(CASE WHEN config_key='base_path_crm' THEN config_value END) AS base_path_crm,
    MAX(CASE WHEN config_key='base_path_erp' THEN config_value END) AS base_path_erp
  FROM public.etl_config
)
SELECT
  base_path_crm,
  base_path_erp
FROM cfg;
-- Expect: both non-NULL and valid filesystem paths
*/