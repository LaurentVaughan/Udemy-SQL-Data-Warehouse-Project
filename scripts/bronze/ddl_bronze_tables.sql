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

===================
Validation Queries
===================
-- 1) Confirm DB + search path
SELECT current_database() AS db, current_user AS user;
SHOW search_path;

-- 2) List all bronze tables
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'bronze'
ORDER BY table_name;

-- 3) Inspect columns (example: erp_px_cat_g1v2)
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'bronze' AND table_name = 'erp_px_cat_g1v2'
ORDER BY ordinal_position;

-- 4) Quick existence check (NULL means missing)
SELECT
  to_regclass('bronze.crm_customer_info')   AS crm_customer_info,
  to_regclass('bronze.crm_product_info')    AS crm_product_info,
  to_regclass('bronze.crm_sales_details')   AS crm_sales_details,
  to_regclass('bronze.erp_loc_a101')        AS erp_loc_a101,
  to_regclass('bronze.erp_cust_az12')       AS erp_cust_az12,
  to_regclass('bronze.erp_px_cat_g1v2')     AS erp_px_cat_g1v2;
*/

/*===================
  CRM: customer_info
  =================*/
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

/*==================
  CRM: product_info
  ================*/
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

/*===================
  CRM: sales_details
  =================*/
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

/*==============
  ERP: loc A101
  ============*/
DROP TABLE IF EXISTS bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101 (
  cid     VARCHAR(50),
  country VARCHAR(50)
);

/*===================
  ERP: customer AZ12
  =================*/
DROP TABLE IF EXISTS bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12 (
  cid           VARCHAR(50),
  date_of_birth DATE,
  gender        VARCHAR(50)
);


/*======================
  ERP: px category G1V2
  ====================*/
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;

CREATE TABLE bronze.erp_px_cat_g1v2 (
  id          VARCHAR(50),
  category    VARCHAR(50),
  subcategory VARCHAR(50),
  maintenance VARCHAR(50)
);
