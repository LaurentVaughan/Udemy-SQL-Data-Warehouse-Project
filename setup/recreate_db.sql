/*
================
recreate_db.sql
================
Purpose:
  Drops and recreates the main Data Warehouse database "SQL-Retail-Analytics-Warehouse".

How to run:
  - Connect to the `postgres` database (NOT the target DB) in VS Code or psql.
  - Execute this script.

Steps:
  1. Drops any active connections (via FORCE).
  2. Creates a fresh UTF-8, en_GB-collated database.
  3. Confirms successful creation.

=======
Usage:
=======
1. Check the current database:
   SELECT current_database();

2. Ensure you have CONNECT privilege
    GRANT CONNECT ON DATABASE sql_retail_analytics_warehouse TO postgres;

3. Verify VS Code is on the same server as PGAdmin
    SELECT
    inet_server_addr()   AS server_ip,
    current_setting('port') AS server_port,
    version()            AS server_version,
    current_database()   AS current_db,
    current_user         AS current_user_name,
    current_setting('data_directory') AS data_dir;

4. Confirm the database exists:
    SELECT datname, datallowconn, datacl
    FROM pg_database
    WHERE datname = 'sql_retail_analytics_warehouse';

5. Execute from VS Code terminal or psql CLI:
    psql -d postgres -f setup/utils/recreate_db.sql
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
