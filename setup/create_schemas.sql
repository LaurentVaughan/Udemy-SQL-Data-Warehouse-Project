/*
===================
create_schemas.sql
===================

Purpose:
---------
- Create the standard data-warehouse schemas used by the project:
  - `bronze`  (raw ingestion)
  - `silver`  (cleansed & conformed)
  - `gold`    (analytics & presentation)

Parameters:
-----------
- None.

Design & idempotency:
---------------------
- Uses `CREATE SCHEMA IF NOT EXISTS` so the script is safe to re-run.

Usage:
------
- Connect to the target database (e.g., `sql_retail_analytics_warehouse`) and execute this script.
*/

-- Create schemas if they do not exist
CREATE SCHEMA IF NOT EXISTS bronze AUTHORIZATION CURRENT_USER;
CREATE SCHEMA IF NOT EXISTS silver AUTHORIZATION CURRENT_USER;
CREATE SCHEMA IF NOT EXISTS gold AUTHORIZATION CURRENT_USER;

/*
=================
Testing Queries:
=================
1) Verify schema creation
SELECT
  current_database()                     AS current_database_name,
  n.nspname                              AS schema_name,
  pg_catalog.pg_get_userbyid(n.nspowner) AS schema_owner
FROM pg_catalog.pg_namespace AS n
WHERE n.nspname IN ('bronze', 'silver', 'gold')
ORDER BY schema_name;
-- Expect: three rows (one per schema) with the current database name, schema name, and user as owner.
*/