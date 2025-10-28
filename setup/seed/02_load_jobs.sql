/*
===========================
setup/seed/02_load_jobs.sql
===========================

Purpose:
---------
- Seed `bronze.load_jobs` by reading base paths from `public.etl_config`.
- Idempotent inserts using ON CONFLICT DO NOTHING.

Parameters:
-----------
- None. Requires `public.etl_config` and `bronze.load_jobs` to exist.

Usage:
------
- Run after `seed_01_etl_config.sql`.
  psql -d <db> -f setup/seed/02_load_jobs.sql

Notes:
------
- This script raises a clear error if prerequisites are missing (fail-fast).
- Adjust file paths in `public.etl_config` as needed for your environment and OS.
*/

-- Ensure unqualified names resolve to BRONZE first (PUBLIC second for extensions like pgcrypto)
SET search_path = bronze, public;

-- Fail-fast: ensure public.etl_config exists and bronze.load_jobs DDL exists
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'etl_config'
  ) THEN
    RAISE EXCEPTION 'Required table public.etl_config not found. Run setup/seed/seed_01_etl_config.sql first.';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'bronze' AND table_name = 'load_jobs'
  ) THEN
    RAISE EXCEPTION 'Required table bronze.load_jobs not found. Ensure DDL has been applied (ddl_bronze_tables.sql or load_bronze.sql).';
  END IF;
END$$;

-- Build absolute paths from public.etl_config (keys: base_path_crm, base_path_erp)
WITH cfg AS (
SELECT
    MAX(
        CASE
            WHEN config_key = 'base_path_crm' THEN config_value
        END
    ) AS base_path_crm,

    MAX(
        CASE
            WHEN config_key = 'base_path_erp' THEN config_value
        END
    ) AS base_path_erp
FROM public.etl_config
)
-- Seed the jobs registry from those base paths; idempotent via ON CONFLICT
INSERT INTO bronze.load_jobs (table_name, file_path, is_enabled, load_order)
SELECT * FROM (
    -------------
    -- CRM files
    -------------
    SELECT
        'bronze.crm_customer_info' AS table_name,
        cfg.base_path_crm || 'customer_info.csv' AS file_path,
        TRUE, 10
    FROM cfg
    UNION ALL
    SELECT
        'bronze.crm_product_info' AS table_name,
        cfg.base_path_crm || 'product_info.csv' AS file_path,
        TRUE, 20
    FROM cfg
    UNION ALL
    SELECT
        'bronze.crm_sales_details' AS table_name,
        cfg.base_path_crm || 'sales_details.csv' AS file_path,
        TRUE, 30
    FROM cfg
    -------------
    -- ERP files
    -------------
    UNION ALL
    SELECT
        'bronze.erp_customer_profiles' AS table_name,
        cfg.base_path_erp || 'customer_profiles.csv' AS file_path,
        TRUE, 40
    FROM cfg
    UNION ALL
    SELECT
        'bronze.erp_location_hierarchy' AS table_name,
        cfg.base_path_erp || 'location_hierarchy.csv' AS file_path,
        TRUE, 50
    FROM cfg
    UNION ALL
    SELECT
        'bronze.erp_product_categories' AS table_name,
        cfg.base_path_erp || 'product_categories.csv' AS file_path,
        TRUE, 60
    FROM cfg
) t
ON CONFLICT (table_name) DO NOTHING;

-- Quick verification output
SELECT
    table_name,
    file_path,
    is_enabled,
    load_order
FROM bronze.load_jobs
ORDER BY load_order;

/*
=================
Testing Queries:
=================

1) Config preflight (must exist and be non‑NULL)
SELECT
  SUM((config_key='base_path_crm' AND config_value IS NOT NULL)::int) AS has_base_path_crm,
  SUM((config_key='base_path_erp' AND config_value IS NOT NULL)::int) AS has_base_path_erp
FROM public.etl_config
WHERE config_key
   IN ('base_path_crm','base_path_erp');
-- Expect: has_base_path_crm = true AND has_base_path_erp = true (1).

2) Seeding idempotency (running this script twice should not add rows)
WITH before_ct AS (
    SELECT
        COUNT(*) AS n
    FROM bronze.load_jobs)
SELECT
  (SELECT n FROM before_ct)                AS count_before,
  (SELECT COUNT(*) FROM bronze.load_jobs)  AS count_after,
  (SELECT COUNT(*) FROM bronze.load_jobs) - (SELECT n FROM before_ct) AS delta
-- Expect delta = 0 on the second run.

3) Expected jobs exist with non‑NULL paths and enabled flag
SELECT
    table_name,
    file_path,
    is_enabled,
    load_order
FROM bronze.load_jobs
WHERE is_enabled
  AND (file_path IS NULL OR file_path='')
ORDER BY load_order;
-- Expect: zero rows (all enabled jobs must have non‑empty file_path).

4) No duplicate table entries (enforced by unique index)
SELECT
    table_name,
    COUNT(*) AS c
FROM bronze.load_jobs
GROUP BY table_name
HAVING COUNT(*) > 1;
-- Expect: zero rows.

5) Any log rows yet?
SELECT COUNT(*) AS log_rows FROM bronze.load_log;
-- Expect: zero rows before any loads.

6) Inspect the latest run:
WITH last_run AS (
  SELECT run_id
  FROM bronze.load_log
  WHERE phase = 'START'
  ORDER BY started_at DESC
  LIMIT 1
)
SELECT
    phase,
    status,
    COUNT(*) AS events
FROM bronze.load_log
WHERE run_id = (SELECT run_id FROM last_run)
GROUP BY phase, status
ORDER BY phase;
-- Expect: At least phases START, TRUNCATE, COPY, FINISH with status 'OK' for successful tables.

7) Identify which tables failed and why
WITH last_run AS (
  SELECT run_id
  FROM bronze.load_log
  WHERE phase = 'START'
  ORDER BY started_at DESC
  LIMIT 1
)
SELECT
  table_name,
  message,       -- error detail
  rows_loaded,
  started_at,
  finished_at
FROM bronze.load_log
WHERE run_id = (SELECT run_id FROM last_run)
  AND phase = 'COPY'
  AND status = 'ERROR'
ORDER BY finished_at DESC;
-- Expect: Any failed rows will have an error message.

7) See the successful tables and their row counts
WITH last_run AS (
  SELECT run_id
  FROM bronze.load_log
  WHERE phase = 'START'
  ORDER BY started_at DESC
  LIMIT 1
),
last_copy_ok AS (
  SELECT table_name, MAX(finished_at) AS last_copy_at
  FROM bronze.load_log
  WHERE run_id = (SELECT run_id FROM last_run)
    AND phase = 'COPY'
    AND status = 'OK'
  GROUP BY table_name
)
SELECT l.table_name, l.rows_loaded, l.finished_at
FROM bronze.load_log l
JOIN last_copy_ok c
  ON l.table_name = c.table_name AND l.finished_at = c.last_copy_at
ORDER BY l.table_name;
-- Expect: One row per successfully loaded table with rows_loaded and timestamp.
*/