/*
==========================
bronze/ddl_bronze_log.sql
==========================
Purpose:
---------
- Create the schema and the append-only log table that records each Bronze batch run.
- Provide constraints and indexes for fast observability and reliable downstream reporting.

- Create the Bronze-layer load logging objects (schema + append-only log table) and
- Add constraints/indexes for fast observability and reliable downstream reporting.
- Every loader execution writes structured rows:
    - batch START/FINISH plus per-table TRUNCATE/COPY steps, errors.

Parameters:
------------
- None.

Design choices:
----------------
- Schema: bronze
- Table:  bronze.load_log (TEXT over VARCHAR by design; length is unbounded and storage is identical)
- Phases: START | VALIDATION | TRUNCATE | COPY | SEPARATOR | FINISH | ERROR
- Idempotent: all statements are IF NOT EXISTS / NOT VALID where appropriate
- Indexes: support common queries for dashboards and investigations
- Constraints: lightweight CHECK constraints on status and phase (NOT VALID so creation never blocks)

Usage:
-------
VS Code (PostgreSQL extension)
  1) Connect to target DB
  2) Run this file

psql (terminal)
  psql -d sql_retail_analytics_warehouse -f bronze/ddl_bronze_log.sql

Notes:
-------------------
- CHECK constraints are created NOT VALID to avoid blocking on historical rows; VALIDATE later if desired:
--   ALTER TABLE bronze.load_log VALIDATE CONSTRAINT load_log_status_chk;
--   ALTER TABLE bronze.load_log VALIDATE CONSTRAINT load_log_phase_chk;
- Add/adjust indexes based on actual query patterns (e.g., by started_at range, run_id lookups).

Verification (quick checks):
-----------------------------
- Table exists
  SELECT
    table_schema,
    table_name
  FROM information_schema.tables
  WHERE table_schema='bronze'
    AND table_name='load_log';

- Recent runs (one per run_id)
  SELECT
    run_id,
    MIN(started_at) AS started_at,
    MAX(finished_at) AS finished_at,
    SUM(rows_loaded) FILTER (WHERE phase='COPY') AS total_rows_loaded,
    BOOL_OR(status='ERROR') AS had_errors
  FROM bronze.load_log
  GROUP BY run_id
  ORDER BY started_at DESC
  LIMIT 5;
*/

-- Ensure schema exists
CREATE SCHEMA IF NOT EXISTS bronze;

-- Centralized structured logging for loads (TEXT by design)
CREATE TABLE IF NOT EXISTS bronze.load_log (
    id              BIGSERIAL   PRIMARY KEY,                        -- surrogate key for convenience
    run_id          UUID        NOT NULL,                           -- groups all steps of a batch
    phase           TEXT        NOT NULL,                           -- START | TRUNCATE | COPY | SEPARATOR | FINISH | ERROR
    table_name      TEXT,                                           -- schema-qualified, e.g. bronze.erp_cust_az12
    file_path       TEXT,                                           -- source file path (as seen by the DB server)
    status          TEXT        NOT NULL,                           -- OK | ERROR
    rows_loaded     BIGINT,                                         -- populated on COPY success
    started_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), -- step start
    finished_at     TIMESTAMPTZ,                                    -- step end
    duration_sec    INTEGER,                                        -- finished_at - started_at (seconds)
    message         TEXT                                            -- free-form info / SQLERRM (error) on failure
);

-- Quality constraints (validated lazily so creation never blocks)
ALTER TABLE bronze.load_log
    ADD CONSTRAINT load_log_status_chk
        CHECK (status IN ('OK', 'ERROR'))
    NOT VALID;

ALTER TABLE bronze.load_log
    ADD CONSTRAINT load_log_phase_chk
        CHECK (phase IN ('START','TRUNCATE','COPY','SEPARATOR','FINISH','ERROR'))
    NOT VALID;

-- Helpful indexes for dashboards and investigations
CREATE INDEX IF NOT EXISTS idx_load_log_run_id      ON bronze.load_log (run_id);          -- batch-grouped reads
CREATE INDEX IF NOT EXISTS idx_load_log_phase       ON bronze.load_log (phase);           -- filter by step type
CREATE INDEX IF NOT EXISTS idx_load_log_table_name  ON bronze.load_log (table_name);      -- per-table drilldowns
CREATE INDEX IF NOT EXISTS idx_load_log_file_path   ON bronze.load_log (file_path);       -- source provenance
CREATE INDEX IF NOT EXISTS idx_load_log_status      ON bronze.load_log (status);          -- quick error scans
CREATE INDEX IF NOT EXISTS idx_load_log_rows_loaded ON bronze.load_log (rows_loaded);     -- heavy vs light loads
CREATE INDEX IF NOT EXISTS idx_load_log_started_at  ON bronze.load_log (started_at);      -- time-range queries
CREATE INDEX IF NOT EXISTS idx_load_log_finished_at ON bronze.load_log (finished_at);     -- time-range queries
CREATE INDEX IF NOT EXISTS idx_load_log_duration    ON bronze.load_log (duration_sec);    -- slow-step profiling
