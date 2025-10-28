/*
======================
ddl/bronze_tables.sql
======================

Purpose:
---------
- Create canonical bronze tables for PostgreSQL
- Drops existing tables if they already exist.
- Adds any missing columns with ALTER TABLE 
- Adds columns only if they do not already exist (idempotent).

Parameters:
------------
- None.

Design choices:
----------------
- Schema: bronze
- Tables: 5 tables from CRM and ERP systems
- Idempotent: all statements are IF NOT EXISTS / NOT VALID where appropriate
- Data types: simple types (INTEGER, VARCHAR, DATE, TIMESTAMP)
- No constraints or indexes at this stage (append-only, raw data)
- No primary keys (ingestion may have duplicates; cleansing later)

Usage:
-------
VS Code (PostgreSQL extension)
  1) Connect to target DB
  2) Run this file
psql (terminal)
  psql -d sql_retail_analytics_warehouse -f bronze/ddl_bronze_tables.sql

Notes:
-------
- Adjust data types and lengths based on actual source data characteristics.
- Add constraints and indexes later based on usage patterns and data quality needs.
*/

-- CRM: Customer Info
DROP TABLE IF EXISTS bronze.crm_customer_info;

CREATE TABLE bronze.crm_customer_info (
  customer_id              INTEGER,
  customer_key             VARCHAR(50),
  customer_first_name      VARCHAR(50),
  customer_last_name       VARCHAR(50),
  customer_material_status VARCHAR(50),
  customer_gender          VARCHAR(50),
  customer_create_date     DATE
);

-- CRM: Product Info
DROP TABLE IF EXISTS bronze.crm_product_info;

CREATE TABLE bronze.crm_product_info (
  product_id         INTEGER,
  product_key        VARCHAR(50),
  product_nm         VARCHAR(50),
  product_cost       INTEGER,
  product_line       VARCHAR(50),
  product_start_date TIMESTAMP,
  product_end_date   TIMESTAMP
);

--  CRM: Sales Details
DROP TABLE IF EXISTS bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details (
  sales_order_number  VARCHAR(50),
  sales_product_key   VARCHAR(50),
  sales_customer_id   INTEGER,
  sales_order_date    TIMESTAMP,
  sales_shipping_date DATE,
  sales_due_date      DATE,
  sales_sales         INTEGER,
  sales_quantity      INTEGER,
  sales_price         INTEGER
);

-- ERP: Customer Profiles
DROP TABLE IF EXISTS bronze.erp_customer_profiles;

CREATE TABLE bronze.erp_customer_profiles (
  cid           VARCHAR(50),
  date_of_birth DATE,
  gender        VARCHAR(50)
);

-- ERP: Location Hierarchy
DROP TABLE IF EXISTS bronze.erp_location_hierarchy;

CREATE TABLE bronze.erp_location_hierarchy (
  cid     VARCHAR(50),
  country VARCHAR(50)
);

-- ERP: Product Categories
DROP TABLE IF EXISTS bronze.erp_product_categories;

CREATE TABLE bronze.erp_product_categories (
  id          VARCHAR(50),
  category    VARCHAR(50),
  subcategory VARCHAR(50),
  maintenance VARCHAR(50)
);

/*
=================
Testing Queries:
=================

1) List all bronze tables
SELECT
  table_schema,
  table_name
FROM information_schema.tables
WHERE table_schema = 'bronze'
ORDER BY table_name;
-- Expect: 7 rows (6 data + 1 load_job + 1 load_log).

2) Inspect columns (example: erp_product_categories)
SELECT
  column_name,
  data_type
FROM information_schema.columns
WHERE table_schema = 'bronze'
  AND table_name = 'erp_product_categories'
ORDER BY ordinal_position;
-- Expect: 4 columns (id, category, subcategory, maintenance).

4) Quick existence check (NULL means missing)
SELECT
  to_regclass('bronze.crm_customer_info')      AS crm_customer_info,
  to_regclass('bronze.crm_product_info')       AS crm_product_info,
  to_regclass('bronze.crm_sales_details')      AS crm_sales_details,
  to_regclass('bronze.erp_customer_profiles')  AS erp_customer_profiles,
  to_regclass('bronze.erp_location_hierarchy') AS erp_location_hierarchy,
  to_regclass('bronze.erp_product_categories') AS erp_product_categories;
-- Expect: all non-NULL.
*/