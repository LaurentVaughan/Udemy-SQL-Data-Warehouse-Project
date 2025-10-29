/*
==============================
setup/orchestrate_bronze.sql
==============================

Overview:
---------
Bronze layer orchestration procedures for CI/CD automation. Validates prerequisites,
creates bronze.load_jobs registry, and populates job metadata from discovered tables.

Purpose:
--------
Provides three stored procedures for automated Bronze layer setup:
- setup.ddl_bronze_tables() - Creates bronze.load_jobs metadata table
- setup.seed_load_jobs() - Populates job registry from discovered tables
- setup.orchestrate_bronze() - Main orchestrator with prerequisite validation

What This Creates:
------------------
- setup schema (if not exists)
- bronze.load_jobs table (metadata registry: table → CSV file mappings)
- setup.ddl_bronze_tables() procedure
- setup.seed_load_jobs() procedure  
- setup.orchestrate_bronze() procedure

What This Does NOT Do:
----------------------
- Does NOT create schemas (see setup/create_schemas.sql)
- Does NOT create bronze data tables (see scripts/bronze/ddl_bronze_tables.sql)
- Does NOT create bronze.load_log table (see scripts/bronze/ddl_bronze_log.sql)
- Does NOT seed etl_config (see setup/seed/01_etl_config.sql)
- Does NOT load CSV data (see scripts/bronze/load_bronze.sql)

Design Philosophy:
------------------
**Single Source of Truth:**
- DDL and configuration data live in standalone, version-controlled SQL files
- Procedures are for orchestration, validation, and dynamic data operations only
- This prevents duplication and keeps the source of truth clear

**Standalone Files (Authoritative):**
  • setup/create_schemas.sql - Creates bronze/silver/gold schemas
  • setup/seed/01_etl_config.sql - Creates & seeds public.etl_config
  • scripts/bronze/ddl_bronze_log.sql - Creates bronze.load_log table
  • scripts/bronze/ddl_bronze_tables.sql - Creates bronze data tables

**This File (Orchestration):**
  • Creates bronze.load_jobs table (lightweight metadata only)
  • Populates bronze.load_jobs from discovered tables and etl_config
  • Validates all prerequisites before execution

Naming Conventions:
-------------------
Variable Prefixes:
  • v_* = Local variables (procedure scope)

Specific Variables Used:
  • v_base_crm - Base path for CRM CSV files from etl_config
  • v_base_erp - Base path for ERP CSV files from etl_config

Path Convention (REQUIRED):
---------------------------
⚠️ ALL file path values in public.etl_config MUST NOT have trailing slashes
   ✅ Correct:   'C:/path/to/folder'
   ❌ Incorrect: 'C:/path/to/folder/'

Procedures automatically append '/' when constructing file paths.
This convention prevents double-slash bugs.

Prerequisites:
--------------
Run these scripts BEFORE executing setup.orchestrate_bronze():
1. setup/create_schemas.sql (creates bronze/silver/gold schemas)
2. setup/seed/01_etl_config.sql (creates & seeds etl_config table)
3. scripts/bronze/ddl_bronze_log.sql (creates bronze.load_log table)
4. scripts/bronze/ddl_bronze_tables.sql (creates 6 bronze data tables)

Testing:
--------
→ tests/test_orchestrate_bronze.ipynb (comprehensive validation)
→ tests/test_02_register_bronze_jobs.ipynb (job registration tests)

*/

CREATE SCHEMA IF NOT EXISTS setup;

/*=====================================================
  PROCEDURE 1: Create bronze.load_jobs Metadata Table
=====================================================*/
CREATE OR REPLACE PROCEDURE setup.ddl_bronze_tables()
LANGUAGE plpgsql
AS $$
BEGIN
    CREATE SCHEMA IF NOT EXISTS bronze;

    CREATE TABLE IF NOT EXISTS bronze.load_jobs (
        table_name TEXT PRIMARY KEY,
        file_path TEXT,
        is_enabled BOOLEAN DEFAULT TRUE,
        load_order INTEGER
    );

    CREATE INDEX IF NOT EXISTS idx_load_jobs_order 
        ON bronze.load_jobs (load_order);
END;
$$;

COMMENT ON PROCEDURE setup.ddl_bronze_tables() IS
    'Creates bronze.load_jobs metadata registry table with index on load_order';

/*=================================================
  PROCEDURE 2: Populate bronze.load_jobs Registry
=================================================*/
CREATE OR REPLACE PROCEDURE setup.seed_load_jobs()
LANGUAGE plpgsql
AS $$
DECLARE
    v_base_crm TEXT;
    v_base_erp TEXT;
BEGIN
    SELECT
        MAX(CASE WHEN config_key = 'base_path_crm' THEN config_value END),
        MAX(CASE WHEN config_key = 'base_path_erp' THEN config_value END)
    INTO v_base_crm, v_base_erp
    FROM public.etl_config;

    IF v_base_crm IS NULL OR v_base_erp IS NULL THEN
        RAISE EXCEPTION 'Missing base_path_crm or base_path_erp in public.etl_config';
    END IF;

    WITH bronze_tables AS (
        SELECT
            n.nspname AS schema_name,
            c.relname AS relname,
            format('%I.%I', n.nspname, c.relname) AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'bronze'
            AND c.relkind = 'r'
            AND c.relpersistence = 'p'
            AND c.relname NOT IN ('load_jobs', 'load_log')
    ),
    parsed AS (
        SELECT
            table_name,
            split_part(relname, '_', 1) AS source,
            substring(relname FROM '^[^_]+_(.*)$') AS dataset
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
            ) + CASE WHEN r.source = 'erp' THEN 1000 ELSE 0 END AS load_order
        FROM resolved r
        WHERE r.file_path IS NOT NULL
    )
    INSERT INTO bronze.load_jobs (table_name, file_path, is_enabled, load_order)
    SELECT table_name, file_path, is_enabled, load_order
    FROM ordered
    ON CONFLICT (table_name) DO UPDATE
        SET file_path = EXCLUDED.file_path,
            is_enabled = EXCLUDED.is_enabled,
            load_order = EXCLUDED.load_order;
END;
$$;

COMMENT ON PROCEDURE setup.seed_load_jobs() IS
    'Populates bronze.load_jobs by discovering bronze tables and mapping them to CSV files using etl_config base paths';

/*======================================================================
  PROCEDURE 3: Orchestrator - Validates Prerequisites & Executes Setup
======================================================================*/
CREATE OR REPLACE PROCEDURE setup.orchestrate_bronze()
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.schemata 
        WHERE schema_name = 'bronze'
    ) THEN
        RAISE EXCEPTION 'bronze schema does not exist. Run setup/create_schemas.sql first.';
    END IF;
    
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = 'etl_config'
    ) THEN
        RAISE EXCEPTION 'public.etl_config does not exist. Run setup/seed/01_etl_config.sql first.';
    END IF;
    
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'bronze' AND table_name = 'load_log'
    ) THEN
        RAISE EXCEPTION 'bronze.load_log does not exist. Run scripts/bronze/ddl_bronze_log.sql first.';
    END IF;
    
    CALL setup.ddl_bronze_tables();
    CALL setup.seed_load_jobs();
    
    RAISE NOTICE 'Bronze layer setup complete! bronze.load_jobs created and populated with % entries.',
        (SELECT COUNT(*) FROM bronze.load_jobs);
END;
$$;

COMMENT ON PROCEDURE setup.orchestrate_bronze() IS
    'Orchestrates bronze layer setup: validates prerequisites, creates load_jobs table, populates job registry';