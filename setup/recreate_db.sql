/*
======================
setup/recreate_db.sql
======================

Purpose:
---------
- Drop (if exists) and recreate the main data warehouse database `sql_retail_analytics_warehouse` with a clean template and predictable locale/encoding.

Parameters:
-----------
- None. This script must be executed connected to a database other than the target (typically `postgres`).

Design & safety notes:
----------------------
- Uses `DROP DATABASE IF EXISTS ... WITH (FORCE)` to remove active connections before recreation. This is destructive to existing data.
- Intended for development/test environments. Review before running in production.

Usage:
------
1) Connect to the `postgres` database (not the target database).
2) Run the script via VS Code or psql:
   psql -d postgres -f setup/recreate_db.sql

Verification:
-------------
- To confirm the new database exists, run:
  SELECT
    datname,
    datdba
  FROM pg_database
  WHERE datname = 'sql_retail_analytics_warehouse';
*/

-- Step 1: Drop the existing database (if any)
DROP DATABASE IF EXISTS sql_retail_analytics_warehouse WITH (FORCE);

-- Step 2: Create a new clean database
CREATE DATABASE sql_retail_analytics_warehouse
  WITH TEMPLATE = template0
       ENCODING = 'UTF8'
       LC_COLLATE = 'en_GB.UTF-8'
       LC_CTYPE   = 'en_GB.UTF-8';

-- Step 3: Wait briefly to ensure catalog refresh
SELECT pg_sleep(1);

-- Step 4: Confirm creation details
SELECT
  d.datname                                  AS database_name,
  pg_catalog.pg_get_userbyid(d.datdba)       AS database_owner,
  pg_catalog.pg_encoding_to_char(d.encoding) AS database_encoding,
  d.datcollate                               AS database_collation,
  d.datctype                                 AS database_ctype
FROM pg_catalog.pg_database AS d
WHERE d.datname = 'sql_retail_analytics_warehouse';
