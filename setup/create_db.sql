/*
======================
setup/create_db.sql
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

Manual Pre-Step: Drop the old database (PowerShell)
----------------------------------------------------------------
Run these commands in PowerShell before executing this SQL file.
Adjust the password and version to your local installation as needed.

1) Terminate any active connections to the target database
psql -U postgres -h localhost -d postgres -v ON_ERROR_STOP=1 -c `
"SELECT pg_terminate_backend(pid)
   FROM pg_stat_activity
  WHERE datname = 'sql_retail_analytics_warehouse'
    AND pid <> pg_backend_pid();"

2) Drop the database (requires PostgreSQL 13+ for WITH (FORCE))
psql -U postgres -h localhost -d postgres -v ON_ERROR_STOP=1 -c `
"DROP DATABASE IF EXISTS sql_retail_analytics_warehouse WITH (FORCE);"

3) Optional: Verify drop success
psql -U postgres -h localhost -d postgres -v ON_ERROR_STOP=1 -c `
"SELECT datname FROM pg_database WHERE datname = 'sql_retail_analytics_warehouse';"
# Expect: no rows returned.

4) Recreate Database:
- Run this SQL file to recreate the database within a new postgres owner server.
- Create new database connection:
  - Server: localhost
  - User: postgres
  - Password: <your_password>
  - Database Name: sql_retail_analytics_warehouse
  - Connection Name: Postgres 18
- Ensure connection to new server instance.
Test connection and save if successful.

5) Run the `setup/create_schemas.sql` script to create the standard schemas within the new server instance.
*/

-- Create a new clean database
CREATE DATABASE sql_retail_analytics_warehouse
  WITH TEMPLATE = template0
       ENCODING = 'UTF8'
       LC_COLLATE = 'en_GB.UTF-8'
       LC_CTYPE   = 'en_GB.UTF-8';

-- Wait briefly to ensure catalog refresh
SELECT pg_sleep(1);

-- Confirm creation details
SELECT
  d.datname                                  AS database_name,
  pg_catalog.pg_get_userbyid(d.datdba)       AS database_owner,
  pg_catalog.pg_encoding_to_char(d.encoding) AS database_encoding,
  d.datcollate                               AS database_collation,
  d.datctype                                 AS database_ctype
FROM pg_catalog.pg_database AS d
WHERE d.datname = 'sql_retail_analytics_warehouse';

/*
=================
Testing Queries:
=================
1) To confirm the new database exists:
SELECT
  d.datname                                  AS database_name,
  pg_catalog.pg_get_userbyid(d.datdba)       AS database_owner,
  pg_catalog.pg_encoding_to_char(d.encoding) AS database_encoding,
  d.datcollate                               AS database_collation,
  d.datctype                                 AS database_ctype
FROM pg_catalog.pg_database AS d
WHERE d.datname = 'sql_retail_analytics_warehouse';
-- Expect: one row; encoding='UTF8', collation='en_GB.UTF-8', ctype='en_GB.UTF-8'.
*/
