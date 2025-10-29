/*
===============================
scripts/bronze/load_bronze.sql
===============================

Overview:
---------
Defines the bronze.load_bronze() procedure that executes the full ETL pipeline
for bronze layer ingestion: TRUNCATE → COPY with comprehensive audit logging.

Purpose:
--------
Load CSV files into bronze tables using metadata-driven configuration from
bronze.load_jobs. All operations are logged to bronze.load_log for observability.

What This Does:
---------------
- Reads job metadata from bronze.load_jobs (table → file mappings)
- For each enabled job (ordered by load_order):
  1. TRUNCATE target bronze table
  2. COPY CSV data from file_path
  3. Log each step (phase, duration, row counts, errors)
- Continues processing remaining jobs even if individual tables fail
- Records overall run status (OK if all succeeded, ERROR if any failed)

What This Doesn't Do:
---------------------
- Does NOT validate CSV structure before loading
- Does NOT perform data transformations (raw ingestion only)
- Does NOT create tables (see ddl_bronze_tables.sql)
- Does NOT retry failed operations automatically

Execution Model:
----------------
- DESTRUCTIVE: TRUNCATE replaces all existing data
- IDEMPOTENT: Same inputs → same end state (each run is logged separately)
- FAULT-TOLERANT: Individual table failures don't stop the entire load
- AUDITABLE: Every operation logged with timestamps, durations, row counts

Error Handling:
---------------
- Table-level errors: Logged as ERROR phase, processing continues
- Procedure-level errors: Logged and re-raised (fatal)
- All errors captured with SQLERRM messages

Single Source of Truth:
-----------------------
This file defines bronze.load_bronze() procedure and two helper procedures for
error logging. Do NOT modify procedure logic elsewhere.

Naming Conventions:
-------------------
Variable Prefixes:
  • p_* = Procedure parameters (input arguments)
  • v_* = Local variables (procedure scope)

Specific Variables Used:
  • p_step_id          - Load log entry ID to update
  • p_started_at       - Step start timestamp
  • p_rows_loaded      - Number of rows loaded (optional)
  • p_run_id           - Unique identifier for entire load batch
  • p_phase            - ETL phase (TRUNCATE, COPY, ERROR, etc.)
  • p_table_name       - Target bronze table
  • p_file_path        - Source CSV file path
  • p_error_message    - Error message from SQLERRM
  • v_finished_at      - Step completion timestamp
  • v_duration_sec     - Step duration in seconds
  • v_run_id           - Generated UUID for current load batch
  • v_job              - Record from bronze.load_jobs query
  • v_step_id          - Current step's load_log entry ID
  • v_step_started_at  - Current step's start timestamp
  • v_rows_loaded      - Row count from COPY operation
  • v_had_errors       - Boolean flag tracking failures

Prerequisites:
--------------
- Database: sql_retail_analytics_warehouse
- Schema: bronze (created by create_schemas.sql)
- Tables: bronze.load_jobs, bronze.load_log (created by ddl_bronze_log.sql)
- Extension: pgcrypto (created by ddl_bronze_log.sql)
- Configuration: bronze.load_jobs populated (by 02_register_bronze_jobs.sql)

Testing:
--------
→ tests/tests_bronze/test_load_bronze.ipynb
  • Comprehensive validation of load procedure
  • Log structure verification
  • Error handling scenarios
  • Performance metrics

*/

/*=======================================
  HELPER PROCEDURE: Log Step Completion
=======================================*/

CREATE OR REPLACE PROCEDURE bronze.log_step_success(
    p_step_id BIGINT,
    p_started_at TIMESTAMPTZ,
    p_rows_loaded BIGINT DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_finished_at TIMESTAMPTZ := clock_timestamp();
BEGIN
    UPDATE bronze.load_log
    SET 
        finished_at = v_finished_at,
        duration_sec = EXTRACT(EPOCH FROM (v_finished_at - p_started_at))::INTEGER,
        rows_loaded = p_rows_loaded
    WHERE id = p_step_id;
END;
$$;

/*====================================
  HELPER PROCEDURE: Log Step Failure
====================================*/
CREATE OR REPLACE PROCEDURE bronze.log_step_error(
    p_step_id BIGINT,
    p_run_id UUID,
    p_phase TEXT,
    p_table_name TEXT,
    p_file_path TEXT,
    p_started_at TIMESTAMPTZ,
    p_error_message TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_finished_at TIMESTAMPTZ := clock_timestamp();
    v_duration_sec INTEGER := EXTRACT(EPOCH FROM (v_finished_at - p_started_at))::INTEGER;
BEGIN
    UPDATE bronze.load_log
    SET 
        status = 'ERROR',
        finished_at = v_finished_at,
        duration_sec = v_duration_sec,
        message = p_error_message
    WHERE id = p_step_id;

    INSERT INTO bronze.load_log (
        run_id, phase, table_name, file_path, status,
        started_at, finished_at, duration_sec, message
    )
    VALUES (
        p_run_id, 'ERROR', p_table_name, p_file_path, 'ERROR',
        p_started_at, v_finished_at, v_duration_sec, p_error_message
    );
END;
$$;

/*==================================
  MAIN PROCEDURE: Bronze Layer ETL
==================================*/
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_run_id UUID := gen_random_uuid();
    v_job RECORD;
    v_step_id BIGINT;
    v_step_started_at TIMESTAMPTZ;
    v_rows_loaded BIGINT;
    v_had_errors BOOLEAN := FALSE;
BEGIN
    INSERT INTO bronze.load_log (run_id, phase, status, started_at)
    VALUES (v_run_id, 'START', 'OK', clock_timestamp());

    FOR v_job IN
        SELECT table_name, file_path
        FROM bronze.load_jobs
        WHERE COALESCE(is_enabled, FALSE)
        ORDER BY load_order
    LOOP
        v_step_started_at := clock_timestamp();
        
        INSERT INTO bronze.load_log (run_id, phase, table_name, file_path, status, started_at)
        VALUES (v_run_id, 'TRUNCATE', v_job.table_name, v_job.file_path, 'OK', v_step_started_at)
        RETURNING id INTO v_step_id;

        BEGIN
            EXECUTE format('TRUNCATE TABLE %s', v_job.table_name);
            CALL bronze.log_step_success(v_step_id, v_step_started_at);
        EXCEPTION WHEN OTHERS THEN
            v_had_errors := TRUE;
            CALL bronze.log_step_error(
                v_step_id, v_run_id, 'TRUNCATE',
                v_job.table_name, v_job.file_path, v_step_started_at, SQLERRM
            );
            CONTINUE;
        END;

        v_step_started_at := clock_timestamp();
        
        INSERT INTO bronze.load_log (run_id, phase, table_name, file_path, status, started_at)
        VALUES (v_run_id, 'COPY', v_job.table_name, v_job.file_path, 'OK', v_step_started_at)
        RETURNING id INTO v_step_id;

        BEGIN
            EXECUTE format(
                'COPY %s FROM %L WITH (FORMAT csv, HEADER true, DELIMITER '','', QUOTE ''"'', ESCAPE ''"'')',
                v_job.table_name, v_job.file_path
            );
            GET DIAGNOSTICS v_rows_loaded = ROW_COUNT;
            CALL bronze.log_step_success(v_step_id, v_step_started_at, v_rows_loaded);
        EXCEPTION WHEN OTHERS THEN
            v_had_errors := TRUE;
            CALL bronze.log_step_error(
                v_step_id, v_run_id, 'COPY',
                v_job.table_name, v_job.file_path, v_step_started_at, SQLERRM
            );
            CONTINUE;
        END;
    END LOOP;

    INSERT INTO bronze.load_log (run_id, phase, status, started_at, finished_at, message)
    VALUES (
        v_run_id, 'FINISH',
        CASE WHEN v_had_errors THEN 'ERROR' ELSE 'OK' END,
        NULL, clock_timestamp(),
        CASE WHEN v_had_errors THEN 'Completed with errors' ELSE NULL END
    );

EXCEPTION WHEN OTHERS THEN
    INSERT INTO bronze.load_log (run_id, phase, status, message, started_at, finished_at)
    VALUES (v_run_id, 'ERROR', 'ERROR', SQLERRM, NULL, clock_timestamp());
    RAISE;
END;
$$;

COMMENT ON PROCEDURE bronze.load_bronze() IS 
    'Executes bronze layer ETL: reads job metadata from bronze.load_jobs, ' ||
    'truncates and loads CSV data into bronze tables, logs all operations to bronze.load_log';

COMMENT ON PROCEDURE bronze.log_step_success(BIGINT, TIMESTAMPTZ, BIGINT) IS
    'Helper procedure: updates load_log entry with success status, duration, and optional row count';

COMMENT ON PROCEDURE bronze.log_step_error(BIGINT, UUID, TEXT, TEXT, TEXT, TIMESTAMPTZ, TEXT) IS
    'Helper procedure: updates load_log entry with error status and creates ERROR phase entry';