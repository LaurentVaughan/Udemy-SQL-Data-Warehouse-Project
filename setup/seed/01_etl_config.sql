/*
============================
setup/seed/01_etl_config.sql
============================

Overview
--------
• Creates the public.etl_config key-value configuration table
• Seeds initial base file path configuration for Bronze data loaders
• Provides centralized configuration management for the data warehouse

Single Source of Truth
-----------------------
• This is the ONLY file that defines the etl_config table structure
• This is the ONLY file that defines initial configuration values
• The seed_all.sql orchestrator validates this exists but does NOT duplicate it

Prerequisites
-------------
• Target database exists (e.g., sql_retail_analytics_warehouse)
• Connection established with sufficient privileges to create tables in public schema

Execution Context
-----------------
• Run BEFORE: setup/seed/seed_all.sql
• Run AFTER: Database creation (setup/create_db.sql)
• Idempotent: Safe to re-run; existing values preserved via ON CONFLICT DO NOTHING

Path Convention (CRITICAL)
---------------------------
⚠️  ALL file path values MUST be provided WITHOUT trailing slashes
    ✅ Correct:   'C:/path/to/folder'
    ❌ Incorrect: 'C:/path/to/folder/'

• Data loaders append '/' when constructing full file paths
• This prevents double-slash bugs (path//file.csv)
• No automatic normalization occurs; paths stored as-provided

Usage
-----
Via psql:
  psql -d sql_retail_analytics_warehouse -f setup/seed/01_etl_config.sql

Via VS Code PostgreSQL extension:
  1. Connect to target database
  2. Execute this file

Testing
-------
Comprehensive test coverage available in:
  tests/test_01_etl_config.ipynb
*/

-- Ensure target schema exists (harmless if already present)
CREATE SCHEMA IF NOT EXISTS public;

-- Resolve unqualified names into PUBLIC first (then BRONZE)
SET search_path = public, bronze;

-- Informative notice if table already exists
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
-- IMPORTANT: Paths must NOT have trailing slashes (see Path Convention above)
INSERT INTO public.etl_config (config_key, config_value)
VALUES
  ('base_path_crm', 'C:/Users/Laurent/Studies/sql-ultimate-course/Udemy-SQL-Data-Warehouse-Project/datasets/source_crm'),
  ('base_path_erp', 'C:/Users/Laurent/Studies/sql-ultimate-course/Udemy-SQL-Data-Warehouse-Project/datasets/source_erp')
ON CONFLICT (config_key) DO NOTHING;

-- Confirmation output
SELECT
    config_key,
    config_value
FROM public.etl_config
WHERE config_key IN ('base_path_crm','base_path_erp')
ORDER BY config_key;
