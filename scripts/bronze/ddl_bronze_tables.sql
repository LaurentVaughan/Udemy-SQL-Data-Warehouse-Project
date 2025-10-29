/*
==============================
ddl_bronze_tables.sql
==============================

Overview:
---------
Creates the six bronze layer data tables that store raw CSV data from CRM and ERP systems.
These tables are append-only, schema-on-read structures with no constraints or indexes.

Purpose:
--------
Bronze tables serve as the raw data ingestion layer:
- Direct 1:1 mapping to source CSV files
- No transformations applied (preserves source data exactly)
- No data quality rules enforced at this stage
- Simple data types (INTEGER, VARCHAR, DATE, TIMESTAMP)

What This Creates:
------------------
CRM Tables (3):
- bronze.crm_cust_info      - Customer demographic information
- bronze.crm_prd_info       - Product catalog and lifecycle data
- bronze.crm_sales_details  - Sales transaction records

ERP Tables (3):
- bronze.erp_CUST_AZ12      - Customer profiles with DOB and gender
- bronze.erp_LOC_A101       - Customer location hierarchy
- bronze.erp_PX_CAT_G1V2    - Product category and subcategory mapping

Design Choices:
---------------
- Schema: bronze
- Destructive: Uses DROP TABLE IF EXISTS (recreates tables on each run)
- No primary keys: Source data may contain duplicates (cleaned in silver layer)
- No foreign keys: Relationships established in silver/gold layers
- No indexes: Raw ingestion prioritizes write speed
- No constraints: Data quality validation happens in silver layer
- Simple types: Matches CSV source data characteristics

Single Source of Truth:
------------------------
This is the canonical DDL for bronze data tables.
- seed_all.sql does NOT create these tables
- This file must be run BEFORE setup.seed_all()

Prerequisites:
--------------
Required:
- bronze schema exists (created by setup/create_schemas.sql)
- Database: sql_retail_analytics_warehouse

Testing:
--------
Comprehensive test suite: tests/test_ddl_bronze_tables.ipynb

*/

-- CRM: Customer Info
DROP TABLE IF EXISTS bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info (
  customer_id              INTEGER,
  customer_key             VARCHAR(50),
  customer_first_name      VARCHAR(50),
  customer_last_name       VARCHAR(50),
  customer_material_status VARCHAR(50),
  customer_gender          VARCHAR(50),
  customer_create_date     DATE
);

-- CRM: Product Info
DROP TABLE IF EXISTS bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info (
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
DROP TABLE IF EXISTS bronze.erp_CUST_AZ12;

CREATE TABLE bronze.erp_CUST_AZ12 (
  cid           VARCHAR(50),
  date_of_birth DATE,
  gender        VARCHAR(50)
);

-- ERP: Location Hierarchy
DROP TABLE IF EXISTS bronze.erp_LOC_A101;

CREATE TABLE bronze.erp_LOC_A101 (
  cid     VARCHAR(50),
  country VARCHAR(50)
);

-- ERP: Product Categories
DROP TABLE IF EXISTS bronze.erp_PX_CAT_G1V2;

CREATE TABLE bronze.erp_PX_CAT_G1V2 (
  id          VARCHAR(50),
  category    VARCHAR(50),
  subcategory VARCHAR(50),
  maintenance VARCHAR(50)
);
