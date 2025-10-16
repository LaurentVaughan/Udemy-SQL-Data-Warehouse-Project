/*
========================================
Bronze Loader Procedure & Jobs Registry
========================================

Purpose:
---------
- Provide a robust, data-driven loader for Bronze tables that:
  - TRUNCATEs targets for clean, idempotent reloads.
  - COPY loads CSVs from server-visible file paths.
  - Logs every step (timings, rows, messages) into bronze.load_log for auditability.

Parameters:
------------
- None (the procedure reads from the registry table bronze.load_jobs).

Design choices & idempotency:
----------------------------
- Objects are created IF NOT EXISTS where appropriate (extensions, tables, indexes).
- The loader records per-step status in `bronze.load_log` and continues on per-table errors so a single failing file doesn't abort the batch.
- COPY is executed server-side; file paths must be accessible to the PostgreSQL server process.

Usage:
-------
1) Prerequisites:
   - Run `bronze/ddl_bronze_log.sql` to create the logging table and indexes.
   - Ensure Bronze tables exist (e.g., via `bronze/ddl_bronze_tables.sql`).
2) Install and register the loader:
   - psql -d <db> -f bronze/load_bronze.sql
3) Run the loader:
   - CALL bronze.load_bronze();

Security & operational notes:
-----------------------------
- COPY reads files from the database server host. Use server-accessible paths or use psql's `\copy` from a client when necessary.
- If targets have FK/identity constraints, consider `TRUNCATE ... RESTART IDENTITY CASCADE` (adjust procedure accordingly).

Verification (quick checks):
---------------------------
- Confirm procedure exists:
SELECT
  proname
FROM pg_proc
WHERE proname = 'load_bronze';

- Summarize recent runs - Last N:(duration, total rows, any errors)
  SELECT run_id,
  MIN(started_at) AS started_at,
  MAX(finished_at) AS finished_at,
  MAX(duration_sec) AS duration_sec,
  SUM(rows_loaded) FILTER (WHERE phase = 'COPY') AS total_rows_loaded,
  BOOL_OR(status = 'ERROR') AS had_errors
  FROM bronze.load_log
  GROUP BY run_id
  ORDER BY started_at DESC
  LIMIT 5;

- Inspect per-table results for the latest run (rows loaded, durations, messages)
  WITH last_run AS (
  SELECT
    run_id
  FROM bronze.load_log
  WHERE phase IN ('START','FINISH')
  ORDER BY started_at DESC
  LIMIT 1
  )
  SELECT
    l.table_name,
    l.file_path,
    l.status,
    l.rows_loaded,
    l.duration_sec,
    l.message
  FROM bronze.load_log l
  JOIN last_run r
  USING (run_id)
  WHERE l.phase = 'COPY'
  ORDER BY l.table_name;
*/

SET search_path = bronze, public;

-- Fail-fast when the session search_path does not start with `bronze`.
DO $$
BEGIN
  IF TRIM(split_part(current_setting('search_path'), ',', 1)) <> 'bronze' THEN
    RAISE EXCEPTION 'search_path must start with "bronze"; current: %', current_setting('search_path');
  END IF;
END$$;

-- 0) UUID generator (for run_id)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1) Jobs registry: map target tables to server-visible CSV paths
CREATE TABLE IF NOT EXISTS bronze.load_jobs (
    id          BIGSERIAL  PRIMARY KEY,
    table_name  TEXT       NOT NULL,   -- schema-qualified (e.g. bronze.erp_cust_az12)
    file_path   TEXT       NOT NULL,   -- absolute path visible to the PostgreSQL server
    is_enabled  BOOLEAN    NOT NULL DEFAULT TRUE,
    load_order  INTEGER    NOT NULL DEFAULT 100
);
CREATE INDEX IF NOT EXISTS idx_load_jobs_enabled_order
    ON bronze.load_jobs (is_enabled, load_order, id);

-- Ensure each target table is only registered once
CREATE UNIQUE INDEX IF NOT EXISTS ux_load_jobs_table_name
  ON bronze.load_jobs (table_name);

-- 2) Main loader procedure
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_run_id        UUID         := gen_random_uuid();
    v_batch_start   TIMESTAMPTZ  := clock_timestamp();
    v_batch_end     TIMESTAMPTZ;
    v_start         TIMESTAMPTZ;
    v_end           TIMESTAMPTZ;
    v_dur           INTEGER;
    v_rows          BIGINT;
    v_table         TEXT;
    v_schema        TEXT;
    v_name          TEXT;
    v_file          TEXT;
    v_enabled_jobs  INTEGER      := 0;
BEGIN
    -- Batch START
    INSERT INTO bronze.load_log (
        run_id,
        phase,
        status,
        message
    ) VALUES (
        v_run_id, 'START', 'OK',
        'Loading Bronze Layer started'
    );

  -- Preflight validation: ensure there are enabled jobs to run
  SELECT
    COUNT(*)
    INTO v_enabled_jobs
    FROM bronze.load_jobs
    WHERE is_enabled;
    IF v_enabled_jobs = 0 THEN
    INSERT INTO bronze.load_log (
      run_id,
      phase,
      status,
      started_at,
      finished_at,
      duration_sec,
      message
    ) VALUES (
        v_run_id, 'VALIDATION', 'OK',
        v_batch_start, clock_timestamp(),
        0, 'No enabled jobs found in bronze.load_jobs - nothing to do'
    );

    -- Batch FINISH (graceful)
    INSERT INTO bronze.load_log (
      run_id,
      phase,
      status,
      started_at,
      finished_at,
      duration_sec,
      message
    ) VALUES (
        v_run_id, 'FINISH', 'OK',
        v_batch_start, clock_timestamp(),
        floor(EXTRACT(EPOCH FROM (clock_timestamp() - v_batch_start)))::INTEGER,
        'Finished: 0 jobs executed'
    );
    RETURN;
  END IF;

    -- Iterate jobs (enabled, ordered)
    FOR v_table, v_file IN
        SELECT
          table_name,
          file_path
        FROM bronze.load_jobs
        WHERE is_enabled
        ORDER BY load_order, id
    LOOP
        -- Parse schema-qualified name
        v_schema := split_part(v_table, '.', 1);
        v_name   := split_part(v_table, '.', 2);

        -- TRUNCATE target (use RESTART IDENTITY/CASCADE if needed in your schema)
        v_start := clock_timestamp();
        BEGIN
            EXECUTE format('TRUNCATE TABLE %I.%I', v_schema, v_name);

            v_end := clock_timestamp();
            v_dur := floor(EXTRACT(EPOCH FROM (v_end - v_start)))::INTEGER;

            INSERT INTO bronze.load_log (
              run_id, phase,
              table_name,
              status,
              started_at,
              finished_at,
              duration_sec,
              message
            ) VALUES (
                v_run_id, 'TRUNCATE', v_table, 'OK',
                v_start, v_end, v_dur, 'Truncated target table'
            );
    EXCEPTION WHEN OTHERS THEN
            v_end := clock_timestamp();
            v_dur := floor(EXTRACT(EPOCH FROM (v_end - v_start)))::INTEGER;

            INSERT INTO bronze.load_log (
              run_id,
              phase,
              table_name,
              status,
              started_at,
              finished_at,
              duration_sec,
              message
            ) VALUES (
                v_run_id, 'TRUNCATE', v_table, 'ERROR',
                v_start, v_end, v_dur, SQLERRM
            );

      -- Don't abort batch; continue with next job so one bad table doesn't stop others
      CONTINUE;
        END;

        -- COPY FROM CSV (server-side)
        v_start := clock_timestamp();
        BEGIN
            EXECUTE format('COPY %I.%I FROM %L WITH (FORMAT CSV, HEADER TRUE)',
                           v_schema, v_name, v_file);

            -- Count loaded rows (safe post-TRUNCATE)
            EXECUTE format('SELECT COUNT(*)::BIGINT FROM %I.%I', v_schema, v_name)
                INTO v_rows;

            v_end := clock_timestamp();
            v_dur := floor(EXTRACT(EPOCH FROM (v_end - v_start)))::INTEGER;

            INSERT INTO bronze.load_log (
              run_id,
              phase,
              table_name,
              file_path,
              status,
              rows_loaded,
              started_at,
              finished_at,
              duration_sec,
              message
            ) VALUES (
                v_run_id, 'COPY', v_table, v_file, 'OK',
                v_rows, v_start, v_end, v_dur,
                format('Loaded CSV into %s', v_table)
            );
        EXCEPTION WHEN OTHERS THEN
            v_end := clock_timestamp();
            v_dur := floor(EXTRACT(EPOCH FROM (v_end - v_start)))::INTEGER;

            INSERT INTO bronze.load_log (
              run_id,
              phase,
              table_name,
              file_path,
              status,
              started_at,
              finished_at,
              duration_sec,
              message
            ) VALUES (
                v_run_id, 'COPY', v_table, v_file, 'ERROR',
                v_start, v_end, v_dur, SQLERRM
            );

      -- Log and continue to next job instead of aborting the entire batch
      CONTINUE;
        END;

        -- Optional separator for readability in dashboards
        INSERT INTO bronze.load_log (
          run_id,
          phase,
          table_name,
          status,
          message
        ) VALUES (
            v_run_id, 'SEPARATOR', v_table, 'OK',
            '-------------'
        );
    END LOOP;

    -- Batch FINISH
    v_batch_end := clock_timestamp();

    INSERT INTO bronze.load_log (
      run_id,
      phase,
      status,
      started_at,
      finished_at,
      duration_sec,
      message
    ) VALUES (
        v_run_id, 'FINISH', 'OK',
        v_batch_start, v_batch_end,
        floor(EXTRACT(EPOCH FROM (v_batch_end - v_batch_start)))::INTEGER,
        format('Loading Bronze Layer completed in %s seconds',
        floor(EXTRACT(EPOCH FROM (v_batch_end - v_batch_start)))::INTEGER)
    );

EXCEPTION WHEN OTHERS THEN
    -- Batch ERROR
    v_batch_end := clock_timestamp();

    INSERT INTO bronze.load_log (
      run_id,
      phase,
      status,
      started_at,
      finished_at,
      duration_sec,
      message
    )
    VALUES (
        v_run_id, 'ERROR', 'ERROR',
        v_batch_start, v_batch_end,
        floor(EXTRACT(EPOCH FROM (v_batch_end - v_batch_start)))::INTEGER,
        format('Error during Bronze load: %s', SQLERRM)
    );

    RAISE;
END;
$$;
