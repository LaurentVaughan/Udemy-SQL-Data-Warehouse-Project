/*
=================================
DDL Script: Create Bronze Tables (Postgres)
=================================
Converted from T-SQL -> Postgres:
 - Removed GO batches
 - Replaced NVARCHAR -> VARCHAR, INT -> INTEGER, DATETIME -> TIMESTAMP
 - Uses DROP TABLE IF EXISTS ... CASCADE and CREATE SCHEMA IF NOT EXISTS
*/
CREATE SCHEMA IF NOT EXISTS bronze;

DROP TABLE IF EXISTS bronze.crm_customer_info CASCADE;
CREATE TABLE bronze.crm_customer_info (
    customer_id                  INTEGER,
    customer_key                 VARCHAR(50),
    customer_first_name          VARCHAR(50),
    customer_last_name           VARCHAR(50),
    customer_material_status     VARCHAR(50),
    customer_gender              VARCHAR(50),
    customer_create_date         DATE
);

DROP TABLE IF EXISTS bronze.crm_product_info CASCADE;
CREATE TABLE bronze.crm_product_info (
    product_id          INTEGER,
    product_key         VARCHAR(50),
    product_nm          VARCHAR(50),
    product_cost        INTEGER,
    product_line        VARCHAR(50),
    product_start_date  TIMESTAMP,
    product_end_date    TIMESTAMP
);

DROP TABLE IF EXISTS bronze.crm_sales_details CASCADE;
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

DROP TABLE IF EXISTS bronze.erp_local_a101 CASCADE;
CREATE TABLE bronze.erp_local_a101 (
    cid     VARCHAR(50),
    country VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_customer_az12 CASCADE;
CREATE TABLE bronze.erp_customer_az12 (
    cid           VARCHAR(50),
    date_of_birth DATE,
    gender        VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2 CASCADE;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           VARCHAR(50),
    category     VARCHAR(50),
    subcategory  VARCHAR(50),
    maintenance  VARCHAR(50)
);