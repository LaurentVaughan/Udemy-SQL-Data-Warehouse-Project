/*
==========================
setup/create_schemas.sql
==========================

Overview:
---------
Creates the three-tier medallion architecture schemas for the data warehouse:
- bronze: Raw data ingestion layer (source system replicas)
- silver: Cleansed and conformed data layer (business rules applied)
- gold: Analytics and presentation layer (aggregated, denormalized)

Single Source of Truth:
------------------------
This is the ONLY place where warehouse schemas are created.

Key principle:
- This file = schema DEFINITION (CREATE SCHEMA statements)
- seed_all.sql = schema VALIDATION (checks existence, does NOT create)
- NO duplication of CREATE SCHEMA logic anywhere else

Relationship to Other Files:
-----------------------------
Execution order:
1. setup/create_db.sql              - Creates sql_retail_analytics_warehouse database
2. setup/create_schemas.sql (THIS)  - Creates bronze/silver/gold schemas
3. setup/seed/seed_all.sql          - Validates schemas exist, orchestrates bronze layer setup

Dependencies:
- Requires: sql_retail_analytics_warehouse database must exist
- Required by: All subsequent setup scripts expect these schemas

Design & Idempotency:
---------------------
Implementation:
- Uses CREATE SCHEMA IF NOT EXISTS for safe re-execution
- AUTHORIZATION CURRENT_USER sets executing user as owner
- No CASCADE drops - schemas preserved on re-run

Safe operations:
- Re-running this script is harmless (IF NOT EXISTS clause)
- Existing schemas and their objects remain untouched
- Ownership transferred to current user if schema exists

Execution Context:
------------------
Connection requirements:
- Database: sql_retail_analytics_warehouse
- User: Must have CREATEDB or schema creation privileges
- Authentication: Standard PostgreSQL connection

Schema Architecture:
--------------------
bronze schema:
- Purpose: Raw data from source systems (CRM, ERP)
- Tables: Direct 1:1 mapping to CSV files
- Transformations: None (preserves source data exactly)
- Naming: Prefixed by source system (crm_*, erp_*)

silver schema:
- Purpose: Cleansed, validated, conformed data
- Tables: Business entities after data quality rules
- Transformations: Type conversions, deduplication, validation
- Naming: Business entity names (customers, products, sales)

gold schema:
- Purpose: Analytics-ready aggregations and metrics
- Tables: Dimensional models, fact tables, aggregates
- Transformations: Denormalization, pre-aggregation, KPIs
- Naming: Business-friendly names optimized for BI tools

Usage:
------
Method 1 - Direct execution:
    psql -d sql_retail_analytics_warehouse -f setup/create_schemas.sql

Method 2 - From psql prompt:
    \c sql_retail_analytics_warehouse
    \i setup/create_schemas.sql

Method 3 - SQL client (DBeaver, pgAdmin):
    1. Connect to sql_retail_analytics_warehouse
    2. Open this file
    3. Execute entire script

Verification:
    Run query: SELECT schema_name FROM information_schema.schemata
               WHERE schema_name IN ('bronze', 'silver', 'gold')
               ORDER BY schema_name;
    Expected: 3 rows (bronze, gold, silver)

Prerequisites:
--------------
Required:
- sql_retail_analytics_warehouse database exists (run setup/create_db.sql first)
- Current user has CREATE privilege on database
- Connection to sql_retail_analytics_warehouse established

Post-Creation Steps:
--------------------
Next steps after running this script:
1. Verify schemas created: Check information_schema.schemata
2. Run bronze layer DDL: Execute scripts/bronze/ddl_bronze_tables.sql
3. Configure ETL metadata: Execute setup/seed/01_etl_config.sql
4. Register load jobs: Execute setup/seed/02_register_bronze_jobs.sql
5. Orchestrate setup: Call setup.seed_all() procedure

Testing:
--------
For comprehensive testing, use: tests/tests_setup/test_create_schemas.ipynb

Test coverage includes:
- Schema existence validation
- Ownership verification
- Privilege checks
- Architecture compliance (3-tier structure)
- Idempotency validation

*/

-- Create schemas if they do not exist
CREATE SCHEMA IF NOT EXISTS bronze AUTHORIZATION CURRENT_USER;
CREATE SCHEMA IF NOT EXISTS silver AUTHORIZATION CURRENT_USER;
CREATE SCHEMA IF NOT EXISTS gold AUTHORIZATION CURRENT_USER;