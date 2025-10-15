/*
================
Seed ETL Config
================
Purpose:
---------
 - Create a small, idempotent key–value configuration table for file paths and seed it with sensible defaults.
 - This script is non-destructive: Uses `ON CONFLICT DO NOTHING`, so it will NEVER overwrite existing rows.

Design choices:
----------------
- Schema: public
- Table:  public.etl_config
          (
            config_key   VARCHAR(100) PRIMARY KEY,
            config_value VARCHAR(200) NOT NULL
          )
- Idempotency: inserts skip when the key already exists (safe to re-run).

Usage:
-------
VS Code (PostgreSQL extension)
  1) Connect to target DB (e.g., `sql_retail_analytics_warehouse`)
  2) Open and execute this file

psql (terminal)
  psql -d sql_retail_analytics_warehouse -f setup/utils/seed_etl_config.sql

Notes:
-------
- This file sets `search_path = public, bronze` for the current session so any
  unqualified objects created here land in **public** by default.
- Paths should end with a trailing slash (e.g., .../datasets/crm/).
- Downstream scripts (e.g., bronze seeders) will read these values to build absolute file paths.

Verification (quick checks):
-----------------------------
- Table exists?
  SELECT 
    table_schema,
    table_name
  FROM information_schema.tables
  WHERE table_schema='public'
    AND table_name='etl_config';

- View values:
  SELECT
    config_key,
    config_value
  FROM public.etl_config
  ORDER BY config_key;

- Insert new key (manual):
  INSERT INTO public.etl_config (
    config_key,
    config_value
  ) VALUES ('some_other_path', 'D:/data/somewhere/');

- Update existing key (manual/admin):
  UPDATE public.etl_config
  SET config_value = 'D:/data/crm/'
  WHERE config_key = 'base_path_crm';
*/

-- Ensure target schema exists (harmless if already present)
CREATE SCHEMA IF NOT EXISTS public;

-- Resolve unqualified names into PUBLIC first (then BRONZE)
SET search_path = public, bronze;

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
