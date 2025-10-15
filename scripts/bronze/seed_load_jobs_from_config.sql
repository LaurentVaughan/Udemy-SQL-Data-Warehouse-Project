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

Verification (post-run quick checks):
-------------------------------------
- SHOW search_path;                                   -- expect "bronze, public"
- SELECT COUNT(*) FROM bronze.load_jobs;              -- jobs present
- CALL bronze.load_bronze();                          -- then inspect bronze.load_log (START → TRUNCATE/COPY → FINISH)
- Compare a target table COUNT(*) to its rows_loaded in the latest COPY log.

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
        'bronze.erp_cust_az12' AS table_name,
        cfg.base_path_erp || 'CUST_AZ12.csv' AS file_path,
        TRUE, 40
    FROM cfg
    UNION ALL
    SELECT
        'bronze.erp_loc_a101' AS table_name,
        cfg.base_path_erp || 'LOC_A101.csv' AS file_path,
        TRUE, 50
    FROM cfg
    UNION ALL
    SELECT
        'bronze.erp_px_cat_g1v2' AS table_name,
        cfg.base_path_erp || 'PX_CAT_G1V2.csv' AS file_path,
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


