/*
=======================
setup/create_db.sql
=======================

Overview
--------
• Creates the sql_retail_analytics_warehouse database with clean template
• Configures UTF-8 encoding with en_GB locale for predictable collation
• Intended for initial database setup in development/test environments

Single Source of Truth
-----------------------
• This is the ONLY file that defines the warehouse database structure
• Database name, encoding, and locale settings defined here exclusively
• No other scripts attempt to create this database

Prerequisites
-------------
• PostgreSQL 13+ installed (required for DROP DATABASE WITH FORCE)
• Connection to 'postgres' database (NOT the target database)
• Superuser or database creation privileges
• No active connections to sql_retail_analytics_warehouse (script will show termination steps)

Execution Context
-----------------
• Run BEFORE: setup/create_schemas.sql, setup/seed/01_etl_config.sql
• Run AFTER: PostgreSQL installation and initial setup
• Destructive: Drops existing database if present (development/test only)

Database Configuration
----------------------
• Name: sql_retail_analytics_warehouse
• Template: template0 (clean, no extra objects)
• Encoding: UTF8 (universal character support)
• Collation: en_GB.UTF-8 (British English sorting rules)
• CType: en_GB.UTF-8 (British English character classification)

Usage
-----
Via psql (recommended):
  psql -U postgres -h localhost -d postgres -f setup/create_db.sql

Via VS Code PostgreSQL extension:
  1. Connect to 'postgres' database
  2. Execute this file
  3. Create new connection to sql_retail_analytics_warehouse

Manual Pre-Steps (if database exists with active connections)
--------------------------------------------------------------
PowerShell commands to terminate connections and force drop:

1) Terminate active connections:
   psql -U postgres -h localhost -d postgres -v ON_ERROR_STOP=1 -c `
   "SELECT pg_terminate_backend(pid) FROM pg_stat_activity 
    WHERE datname = 'sql_retail_analytics_warehouse' AND pid <> pg_backend_pid();"

2) Force drop database (PostgreSQL 13+):
   psql -U postgres -h localhost -d postgres -v ON_ERROR_STOP=1 -c `
   "DROP DATABASE IF EXISTS sql_retail_analytics_warehouse WITH (FORCE);"

3) Verify drop (expect no rows):
   psql -U postgres -h localhost -d postgres -v ON_ERROR_STOP=1 -c `
   "SELECT datname FROM pg_database WHERE datname = 'sql_retail_analytics_warehouse';"

Post-Creation Steps
-------------------
1. Create new database connection in VS Code/pgAdmin:
   • Host: localhost
   • User: postgres
   • Database: sql_retail_analytics_warehouse
   • Connection Name: sql_retail_analytics_warehouse

2. Run schema creation:
   \i setup/create_schemas.sql

3. Proceed with setup sequence (see setup/seed/seed_all.sql for complete order)

Testing
-------
Comprehensive test coverage available in:
  tests/tests_setup/test_create_db.ipynb
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
