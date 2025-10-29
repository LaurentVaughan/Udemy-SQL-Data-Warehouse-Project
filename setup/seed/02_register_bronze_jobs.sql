/*
=============================
02_register_bronze_jobs.sql
=============================

⚠️  CRITICAL: This script registers METADATA only - it does NOT load CSV data!

Overview:
---------
Populates the bronze.load_jobs registry table with mappings between bronze tables
and their source CSV files. The actual data loader (load_bronze.sql) reads this
registry to know which files to load into which tables.

What This Does:
---------------
- ✅ Discovers all bronze tables dynamically from pg_catalog
- ✅ Maps tables to CSV files using naming convention: bronze.{source}_{dataset} → {dataset}.csv
- ✅ Resolves full file paths using public.etl_config base paths
- ✅ Assigns load execution order (CRM tables first, then ERP tables)
- ✅ Sets is_enabled flag for all discovered tables

What This Does NOT Do:
-----------------------
- ❌ Does NOT load actual CSV data into tables
- ❌ Does NOT modify or truncate existing data
- ❌ Does NOT execute COPY commands

Single Source of Truth:
------------------------
This file is a thin wrapper that calls setup.seed_load_jobs() procedure.
- Implementation: setup/seed/seed_all.sql (setup.seed_load_jobs procedure)
- Wrapper: This file (for standalone execution in automation)

Table Naming Convention:
-------------------------
Auto-discovery pattern:

Table format:     bronze.{source}_{dataset}
CSV file format:  {base_path}/{dataset}.csv

Examples:
- bronze.crm_cust_info  → {base_path_crm}/cust_info.csv
- bronze.crm_prd_info   → {base_path_crm}/prd_info.csv
- bronze.erp_CUST_AZ12  → {base_path_erp}/CUST_AZ12.csv
- bronze.erp_LOC_A101   → {base_path_erp}/LOC_A101.csv

Delimiter logic:
- Part before underscore = source system (crm|erp)
- Part after underscore = dataset name (becomes CSV filename)

Load Order Assignment:
----------------------
Sequential execution priority:
- CRM tables: load_order assigned 0-999 (alphabetically by dataset name)
- ERP tables: load_order assigned 1000+ (alphabetically by dataset name)

Rationale:
- Ensures CRM data loads before ERP data
- Within each source system, tables load alphabetically
- Predictable, reproducible execution sequence

Prerequisites:
--------------
Required objects must exist:
1. bronze schema
2. public.etl_config table with base_path_crm and base_path_erp keys
3. bronze.load_jobs table
4. setup.seed_load_jobs() procedure
5. Bronze data tables following naming convention: bronze.{source}_{dataset}

Path Convention:
- etl_config base paths must NOT have trailing slashes

Testing:
--------
Comprehensive test suite: tests/test_02_register_bronze_jobs.ipynb

*/

-- Invoke the canonical seeder and show a quick verification table
CALL setup.seed_load_jobs();

-- Quick verification output: Show registered job metadata
SELECT
  table_name,
  file_path,
  is_enabled,
  load_order
FROM bronze.load_jobs
ORDER BY load_order;