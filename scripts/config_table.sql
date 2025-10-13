/*
==========================================
Create Configuration Table for File Paths (Postgres)
==========================================
- Uses public.etl_config
- Uses standard Postgres types and IF NOT EXISTS idioms
- Inserts use INSERT ... SELECT ... WHERE NOT EXISTS for idempotency
*/
CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.etl_config (
    config_key   VARCHAR(100) PRIMARY KEY,
    config_value VARCHAR(200)
);

-- Insert CRM base path if not already present
INSERT INTO public.etl_config (config_key, config_value)
SELECT 'base_path_crm', 'C:/Users/Laurent/Studies/sql-ultimate-course/Udemy-SQL-Data-Warehouse-Project/datasets/crm/'
WHERE NOT EXISTS (
    SELECT 1 FROM public.etl_config WHERE config_key = 'base_path_crm'
);

-- Insert ERP base path if not already present
INSERT INTO public.etl_config (config_key, config_value)
SELECT 'base_path_erp', 'C:/Users/Laurent/Studies/sql-ultimate-course/Udemy-SQL-Data-Warehouse-Project/datasets/erp/'
WHERE NOT EXISTS (
    SELECT 1 FROM public.etl_config WHERE config_key = 'base_path_erp'
);

-- To show Table Contents:
-- SELECT * FROM public.etl_config ORDER BY config_key;
-- To drop table, highlight and run the following:
-- DROP TABLE IF EXISTS public.etl_config;