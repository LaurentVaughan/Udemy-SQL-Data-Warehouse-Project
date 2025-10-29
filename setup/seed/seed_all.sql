/*
=====================================
setup/seed/seed_all.sql
=====================================
Overview:
---------
- Defines lightweight, idempotent stored procedures to create required
  schemas and DDL for the Bronze layer, seed configuration values, and
  populate the `bronze.load_jobs` registry. The stored procedures are
  intended for CI/deployment automation and for operators who prefer to
  CALL procedures rather than run multiple SQL scripts.

Relationship to Standalone Scripts:
------------------------------------
- This file contains ONLY the orchestration procedure (setup.seed_all)
- All actual DDL, schema creation, and seeding logic lives in standalone files

Standalone Files (Single Source of Truth):
  * setup/create_schemas.sql              - Creates bronze/silver/gold schemas
  * setup/seed/01_etl_config.sql          - Creates & seeds public.etl_config
  * scripts/bronze/ddl_bronze_log.sql     - Creates bronze.load_log table
  * scripts/bronze/ddl_bronze_tables.sql  - Creates bronze data tables

This File (Orchestration Only):
  * setup.ddl_bronze_tables()  - Creates bronze.load_jobs (lightweight registry)
  * setup.seed_load_jobs()     - Populates bronze.load_jobs from discovered tables
  * setup.seed_all()           - Orchestrator that validates prerequisites

Design Philosophy:
------------------
- DDL and configuration data should live in standalone, version-controlled SQL files
- Procedures are for orchestration, validation, and dynamic data operations only
- This prevents duplication and keeps the source of truth clear

Usage:
------
1. Create schemas:
   \i setup/create_schemas.sql

2. Seed configuration:
   \i setup/seed/01_etl_config.sql

3. Apply DDL:
   \i scripts/bronze/ddl_bronze_log.sql
   \i scripts/bronze/ddl_bronze_tables.sql

4. Create orchestration procedures:
   \i setup/seed/seed_all.sql

5. Run orchestrator (creates load_jobs and populates registry):
   CALL setup.seed_all();

Path Convention (REQUIRED):
---------------------------
⚠️  ALL file path values in public.etl_config MUST be provided WITHOUT trailing slashes.
    ✅ Correct:   'C:/path/to/folder'
    ❌ Incorrect: 'C:/path/to/folder/'
- Seeders and loaders append '/' when constructing file paths
- This convention prevents double-slash bugs

Notes:
------
- This file intentionally avoids destructive DROP TABLE statements. DDL
  that modifies data tables lives in `scripts/bronze/ddl_bronze_tables.sql`.
*/

-- 0) Housekeeping
CREATE SCHEMA IF NOT EXISTS setup;

-- 1) Wrap bronze.load_jobs DDL (lightweight metadata table)
CREATE OR REPLACE PROCEDURE setup.ddl_bronze_tables()
LANGUAGE plpgsql
AS $$
BEGIN
  -- Ensure the bronze schema exists (harmless if already present)
  CREATE SCHEMA IF NOT EXISTS bronze;

  -- jobs registry used by the loader
  CREATE TABLE IF NOT EXISTS bronze.load_jobs (
    table_name TEXT PRIMARY KEY,
    file_path  TEXT,
    is_enabled BOOLEAN DEFAULT TRUE,
    load_order INTEGER
  );

  CREATE INDEX IF NOT EXISTS idx_load_jobs_order ON bronze.load_jobs (load_order);
END;
$$;

-- 2) SEED: load_jobs (uses config; lives in bronze)
-- Registers bronze table → CSV file mappings in bronze.load_jobs metadata registry.
-- This does NOT load data; it only creates the job registry.
CREATE OR REPLACE PROCEDURE setup.seed_load_jobs()
LANGUAGE plpgsql
AS $$
DECLARE
  v_base_crm TEXT;
  v_base_erp TEXT;
BEGIN
  -- Pull config values for CRM and ERP
  SELECT
    MAX(CASE WHEN config_key='base_path_crm' THEN config_value END),
    MAX(CASE WHEN config_key='base_path_erp' THEN config_value END)
  INTO v_base_crm, v_base_erp
  FROM public.etl_config;

  IF v_base_crm IS NULL OR v_base_erp IS NULL THEN
    RAISE EXCEPTION 'Missing base_path_crm or base_path_erp in etl_config.';
  END IF;

  -- Build a list of relevant bronze tables (excluding system tables)
  WITH bronze_tables AS (
    SELECT
      n.nspname AS schema_name,
      c.relname AS relname,
      format('%I.%I', n.nspname, c.relname) AS table_name
    FROM pg_class c
    JOIN pg_namespace n
      ON n.oid = c.relnamespace
    WHERE n.nspname = 'bronze'
      AND c.relkind = 'r'         -- ordinary tables
      AND c.relpersistence = 'p'  -- permanent
      AND c.relname NOT IN ('load_jobs', 'load_log')  -- exclude helpers
  ),
  parsed AS (
    SELECT
      table_name,
      split_part(relname, '_', 1) AS source,
      substring(relname from '^[^_]+_(.*)$') AS dataset
    FROM bronze_tables
    WHERE relname LIKE '%\_%' ESCAPE '\'
  ),
  resolved AS (
    SELECT
      p.table_name,
      CASE
        WHEN p.source = 'crm' THEN v_base_crm || '/' || p.dataset || '.csv'
        WHEN p.source = 'erp' THEN v_base_erp || '/' || p.dataset || '.csv'
        ELSE NULL
      END AS file_path,
      p.source,
      p.dataset
    FROM parsed p
  ),
  ordered AS (
    SELECT
      r.table_name,
      r.file_path,
      TRUE AS is_enabled,
      row_number() OVER (
        PARTITION BY r.source
        ORDER BY r.dataset
      ) + CASE WHEN r.source='erp'
               THEN 1000
               ELSE 0
               END AS load_order
    FROM resolved r
    WHERE r.file_path IS NOT NULL
  )
  INSERT INTO bronze.load_jobs (
    table_name,
    file_path,
    is_enabled,
    load_order
)
  SELECT
    table_name,
    file_path,
    is_enabled,
    load_order
  FROM ordered
  ON CONFLICT (table_name) DO UPDATE
    SET file_path  = EXCLUDED.file_path,
        is_enabled = EXCLUDED.is_enabled,
        load_order = EXCLUDED.load_order;
END;
$$;

-- 3) Orchestrator
-- Validates prerequisites and orchestrates the final setup steps
-- IMPORTANT: This assumes all standalone DDL and seed scripts have been run first
CREATE OR REPLACE PROCEDURE setup.seed_all()
LANGUAGE plpgsql
AS $$
BEGIN
  -- Validate prerequisite: bronze schema exists
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.schemata 
    WHERE schema_name = 'bronze'
  ) THEN
    RAISE EXCEPTION 'bronze schema does not exist. Please run setup/create_schemas.sql first.';
  END IF;
  
  -- Validate prerequisite: etl_config exists
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables 
    WHERE table_schema = 'public' AND table_name = 'etl_config'
  ) THEN
    RAISE EXCEPTION 'public.etl_config table does not exist. Please run setup/seed/01_etl_config.sql first.';
  END IF;
  
  -- Validate prerequisite: bronze.load_log exists
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables 
    WHERE table_schema = 'bronze' AND table_name = 'load_log'
  ) THEN
    RAISE EXCEPTION 'bronze.load_log table does not exist. Please run scripts/bronze/ddl_bronze_log.sql first.';
  END IF;
  
  -- Create supporting bronze tables used by the loader (jobs registry)
  CALL setup.ddl_bronze_tables();
  
  -- Seed the jobs registry from discovered bronze tables (metadata only, no data loading)
  CALL setup.seed_load_jobs();
  
  RAISE NOTICE 'Setup complete! All prerequisites validated, bronze.load_jobs created and populated.';
END;
$$;

-- 4) Execute all steps (uncomment to run at apply-time)
-- NOTE: Ensure all prerequisite scripts have been run first (see below)
-- CALL setup.seed_all();

/*
=================
Setup Sequence (Complete Bronze Layer Initialization)
=================

Step-by-step execution order:

1. Create schemas (run ONCE):
   \i setup/create_schemas.sql

2. Seed configuration (run ONCE):
   \i setup/seed/01_etl_config.sql

3. Create DDL for load_log (run ONCE):
   \i scripts/bronze/ddl_bronze_log.sql

4. Create bronze data tables (run ONCE):
   \i scripts/bronze/ddl_bronze_tables.sql

5. Create orchestration procedures (this file):
   \i setup/seed/seed_all.sql

6. Run orchestrator (creates load_jobs registry and populates it):
   CALL setup.seed_all();

The orchestrator validates that steps 1-4 were completed before proceeding.

=================
Testing / Verification
=================

-- 1) Confirm required objects exist
SELECT to_regclass('setup.seed_all') AS seed_proc,
       to_regclass('setup.seed_etl_config') AS seed_etl_proc,
       to_regclass('bronze.load_jobs') AS load_jobs_table,
       to_regclass('bronze.load_log') AS load_log_table;

-- 2) Inspect seeded config values (no trailing slash)
SELECT config_key, config_value FROM public.etl_config WHERE config_key IN ('base_path_crm','base_path_erp');

-- 3) Inspect seeded jobs
SELECT table_name, file_path, is_enabled, load_order FROM bronze.load_jobs ORDER BY load_order;

-- 4) Sanity: loader procedure available
SELECT to_regclass('bronze.load_bronze') AS loader_proc;

*/
