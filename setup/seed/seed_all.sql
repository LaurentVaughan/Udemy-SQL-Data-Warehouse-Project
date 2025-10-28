-- 0) Housekeeping
CREATE SCHEMA IF NOT EXISTS setup;

-- 1) SEED: etl_config
CREATE OR REPLACE PROCEDURE setup.seed_etl_config()
LANGUAGE plpgsql
AS $$
BEGIN
  -- Config table lives in public (adjust if you prefer setup.)
  CREATE TABLE IF NOT EXISTS public.etl_config (
    config_key   TEXT PRIMARY KEY,
    config_value TEXT NOT NULL
  );

  -- Seed defaults (idempotent and ensures every inserted path ends with /)
    INSERT INTO public.etl_config (config_key, config_value)
    SELECT config_key, 
        CASE 
            WHEN right(config_value, 1) = '/' THEN config_value
            ELSE config_value || '/'
        END
    FROM (VALUES
    ('base_path_crm', 'C:/Users/Laurent/Studies/sql-ultimate-course/Udemy-SQL-Data-Warehouse-Project/datasets/crm'),
    ('base_path_erp', 'C:/Users/Laurent/Studies/sql-ultimate-course/Udemy-SQL-Data-Warehouse-Project/datasets/erp')
    ) AS v(config_key, config_value)
    ON CONFLICT (config_key) DO NOTHING;
END;
$$;

-- 2) Create working schemas
CREATE OR REPLACE PROCEDURE setup.create_schemas()
LANGUAGE plpgsql
AS $$
BEGIN
  CREATE SCHEMA IF NOT EXISTS bronze;
  -- CREATE SCHEMA IF NOT EXISTS silver;
  -- CREATE SCHEMA IF NOT EXISTS gold;
END;
$$;

-- 3) Wrap bronze.load_log DDL
CREATE OR REPLACE PROCEDURE setup.ddl_bronze_log()
LANGUAGE plpgsql
AS $$
BEGIN
    CREATE TABLE IF NOT EXISTS bronze.load_log (
        id           BIGSERIAL   PRIMARY KEY,
        run_id       UUID        NOT NULL,
        phase        TEXT        NOT NULL,  -- START | TRUNCATE | COPY | SEPARATOR | FINISH | ERROR
        table_name   TEXT,
        file_path    TEXT,
        status       TEXT        NOT NULL,  -- OK | ERROR
        rows_loaded  BIGINT,
        started_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        finished_at  TIMESTAMPTZ,
        error_msg    TEXT,
        duration_sec INTEGER GENERATED ALWAYS AS (
        CASE
            WHEN finished_at IS NULL THEN NULL
            ELSE floor(extract(epoch from (finished_at - started_at)))::int
        END
        ) STORED
    );

    CREATE INDEX IF NOT EXISTS idx_load_log_run_id      ON bronze.load_log (run_id);          -- batch-grouped reads
    CREATE INDEX IF NOT EXISTS idx_load_log_phase       ON bronze.load_log (phase);           -- filter by step type
    CREATE INDEX IF NOT EXISTS idx_load_log_table_name  ON bronze.load_log (table_name);      -- per-table drilldowns
    CREATE INDEX IF NOT EXISTS idx_load_log_file_path   ON bronze.load_log (file_path);       -- source provenance
    CREATE INDEX IF NOT EXISTS idx_load_log_status      ON bronze.load_log (status);          -- quick error scans
    CREATE INDEX IF NOT EXISTS idx_load_log_started_at  ON bronze.load_log (started_at);      -- time-range queries
    CREATE INDEX IF NOT EXISTS idx_load_log_finished_at ON bronze.load_log (finished_at);     -- time-range queries
    CREATE INDEX IF NOT EXISTS idx_load_log_duration    ON bronze.load_log (duration_sec);    -- slow-step profiling
END;
$$;

-- 4) SEED: load_jobs (uses config; lives in bronze)
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

-- 4) Orchestrator
CREATE OR REPLACE PROCEDURE setup.seed_all()
LANGUAGE plpgsql
AS $$
BEGIN
  CALL setup.create_schemas();
  CALL setup.ddl_bronze_log();
  CALL setup.seed_etl_config();
  CALL setup.seed_load_jobs();
END;
$$;

-- 5) Execute all steps
CALL setup.seed_all();
