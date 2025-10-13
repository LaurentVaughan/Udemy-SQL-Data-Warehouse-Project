/*
===================
create_schemas.sql
===================
Purpose:
  Create standard data-warehouse schemas in `sql_retail_analytics_warehouse`:
  - bronze  (raw ingestion)
  - silver  (cleansed & conformed)
  - gold    (analytics & presentation)

How to run:
  - Connect to `sql_retail_analytics_warehouse` in VS Code.
  - Execute this script.

Idempotent:
  - Uses CREATE SCHEMA IF NOT EXISTS and will not fail if rerun.
*/

-- Step 1: Create schemas if they do not exist
CREATE SCHEMA IF NOT EXISTS bronze AUTHORIZATION CURRENT_USER;
CREATE SCHEMA IF NOT EXISTS silver AUTHORIZATION CURRENT_USER;
CREATE SCHEMA IF NOT EXISTS gold AUTHORIZATION CURRENT_USER;

-- Step 2: Verify schema creation
SELECT
  current_database()                     AS current_database_name,
  n.nspname                              AS schema_name,
  pg_catalog.pg_get_userbyid(n.nspowner) AS schema_owner
FROM pg_catalog.pg_namespace AS n
WHERE n.nspname IN ('bronze', 'silver', 'gold')
ORDER BY schema_name;
