/*
====================
seed_etl_config.sql
====================
Purpose:
    Create a small, idempotent key–value configuration table for file paths
    (and similar settings) and seed it with defaults. The script uses
    `ON CONFLICT DO NOTHING`, so it will NEVER overwrite existing rows.

Design choices:
    - Schema:   public
    - Table:    public.etl_config(config_key VARCHAR(100) PRIMARY KEY,
                                  config_value VARCHAR(200) NOT NULL
                                )
    - Idempotency: inserts skip when the key already exists.

How to run:
    VS Code (Microsoft PostgreSQL extension):
        1) Connect to your target database (e.g., `sql_retail_analytics_warehouse`)
        2) Open and execute this file.

    psql (terminal):
        psql -d sql_retail_analytics_warehouse -f setup/utils/seed_etl_config.sql

========================
Usage patterns & checks
========================
-- Verify the table exists:
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'etl_config';

-- View all config values:
    SELECT config_key, config_value
    FROM public.etl_config
    ORDER BY config_key;

-- Insert a new key (non-destructive; errors if duplicate PK):
    INSERT INTO public.etl_config (config_key, config_value)
    VALUES ('some_other_path', 'D:/data/somewhere/');

-- Update an existing value (manual/admin action, not done by this script):
    UPDATE public.etl_config
    SET config_value = 'D:/data/crm/'
    WHERE config_key = 'base_path_crm';
*/

-- Ensure target schema exists (harmless if already present)
CREATE SCHEMA IF NOT EXISTS public;

-- Ensure we run with expected search_path so any future objects are created in bronze when intended
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
SELECT config_key, config_value
FROM public.etl_config
WHERE config_key IN ('base_path_crm','base_path_erp')
ORDER BY config_key;
