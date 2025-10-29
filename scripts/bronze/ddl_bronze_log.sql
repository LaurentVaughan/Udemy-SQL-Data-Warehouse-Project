/*
==================================
scripts/bronze/ddl_bronze_log.sql
==================================

Overview:
---------
Creates the bronze-layer audit logging infrastructure for ETL observability.

Purpose:
--------
Creates bronze.load_log table with supporting indexes and constraints to track
all ETL operations. Every load execution writes structured rows documenting
batch events, table operations, and error details.

What This Creates:
------------------
- bronze schema (if not exists)
- pgcrypto extension (UUID generation)
- bronze.load_log table (append-only audit log)
- 9 indexes (run_id, phase, table_name, file_path, status, rows_loaded, started_at, finished_at, duration_sec)
- 2 CHECK constraints (status, phase)

What Logs Are Captured:
-----------------------
- Batch START/FINISH events
- Per-table TRUNCATE/COPY operations
- Error details with SQLERRM messages
- Duration and row counts

Design Choices:
---------------
- TEXT over VARCHAR: unbounded length, identical storage
- Phases: START | VALIDATION | TRUNCATE | COPY | SEPARATOR | FINISH | ERROR
- Idempotent: all statements use IF NOT EXISTS / NOT VALID
- CHECK constraints: NOT VALID on creation (never blocks, validate later if needed)

Single Source of Truth:
-----------------------
This file is the authoritative definition for bronze.load_log structure.
Do NOT modify the table schema elsewhere.

Prerequisites:
--------------
- Database: sql_retail_analytics_warehouse must exist
- Schema: bronze schema is created by this script

Testing:
--------
→ tests/test_ddl_bronze_log.ipynb
  • 27 tests across 7 suites
  • Table/schema existence, column definitions, all 9 indexes, CHECK constraints
  • Default values, sequences, integration tests with sample data

*/

-- Ensure schema exists
CREATE SCHEMA IF NOT EXISTS bronze;

-- Provide a UUID generator used by load scripts. This uses pgcrypto's gen_random_uuid().
-- Creating this extension requires appropriate privileges; it's safe to run IF NOT EXISTS.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

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