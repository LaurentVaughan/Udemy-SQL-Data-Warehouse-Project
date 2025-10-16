/*
===========================
Seed Load Jobs from Config
===========================
Purpose:
---------
Provide a robust, data-driven loader for Bronze tables that:
  - TRUNCATEs targets for clean, idempotent reloads.
  - COPY loads CSVs from server-visible file paths.
  - Logs every step (timings, rows, messages) into bronze.load_log for auditability.
  - Performs a preflight VALIDATION of enabled jobs; exits gracefully if none.
  - Continues on per-table errors (logs them) so one bad file doesn’t abort the batch.

Parameters:
------------
- None (the procedure reads table→file mappings from bronze.load_jobs).

Usage:
--------
Pre-requisites:
- Run bronze/ddl_bronze_log.sql (creates bronze.load_log + indexes).
- Ensure Bronze data tables already exist (created elsewhere).
- Ensure pgcrypto is available (for gen_random_uuid()).

Install (this script):
- psql -f bronze/load_bronze.sql  -- creates bronze.load_jobs (if missing) and the loader procedure

Seed jobs (one-time):
- Prefer: bronze/seed_load_jobs_from_config.sql (reads base paths from public.etl_config).
- Or insert rows directly into bronze.load_jobs (table_name, file_path, load_order, is_enabled).

Security & Ops Notes:
----------------------
- COPY reads files from the *database server* host. Paths must be readable by the Postgres service account.
- For FK/identity-heavy targets you may use:
  - TRUNCATE TABLE %I.%I RESTART IDENTITY CASCADE  (edit the TRUNCATE EXECUTE line accordingly).
- This script sets: SET search_path = bronze, public; and fully qualifies objects as bronze.*.
- Per-table errors are logged with SQLERRM; the procedure CONTINUEs to the next job.

Optional (included at end of file, commented):
----------------------------------------------
- Seeding examples: template INSERTs into bronze.load_jobs.
- Ad-hoc operational queries:
  - Recent runs summary (duration, rows, error flag).
  - Latest run per-table results (rows_loaded, duration, message).

Verification (post‑run quick checks):
----------------------------------
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

5) Inspect the latest run:
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

6) Identify which tables failed and why
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

Dependencies:
--------------
- Schema: bronze
- Tables: bronze.load_jobs (registry), bronze.load_log (logging)
- Extension: pgcrypto (for gen_random_uuid())
*/

-- Ensure unqualified names resolve to BRONZE first (PUBLIC second for extensions like pgcrypto)
SET search_path = bronze, public;

-- Create the BRONZE schema if it doesn’t exist yet (no-op if present)
CREATE SCHEMA IF NOT EXISTS bronze;

-- Jobs registry table (idempotent)
CREATE TABLE IF NOT EXISTS bronze.load_jobs (
    id          BIGSERIAL   PRIMARY KEY,
    table_name  TEXT        NOT NULL,
    file_path   TEXT        NOT NULL,
    is_enabled  BOOLEAN     NOT NULL DEFAULT TRUE,
    load_order  INTEGER     NOT NULL DEFAULT 100
);

-- Prevent duplicates
CREATE UNIQUE INDEX IF NOT EXISTS ux_load_jobs_table_name
    ON bronze.load_jobs (table_name);

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