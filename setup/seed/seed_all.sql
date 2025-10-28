-- Orchestrator: setup/seed_all.sql
-- Runs all seed scripts in order: config -> bronze job registry
-- Usage: psql -d <db> -f setup/seed_all.sql
\i setup/seed/seed_01_etl_config.sql
\i setup/seed/seed_02_load_jobs.sql

-- Optional: show the seeded keys and seeded jobs
SELECT * FROM public.etl_config WHERE config_key IN ('base_path_crm','base_path_erp');
SELECT table_name, file_path, is_enabled, load_order FROM bronze.load_jobs ORDER BY load_order;
