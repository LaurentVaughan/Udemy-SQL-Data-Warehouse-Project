/*
=================================
DDL Script: Create Bronze Tables
=================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
*/
IF OBJECT_ID('bronze.crm_customer_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_customer_info;
GO
CREATE TABLE bronze.crm_customer_info (
    customer_id                  INT,
    customer_key                 NVARCHAR(50),
    customer_first_name          NVARCHAR(50),
    customer_last_name           NVARCHAR(50),
    customer_material_status     NVARCHAR(50),
    customer_gender              NVARCHAR(50),
    customer_create_date         DATE
);
GO

IF OBJECT_ID('bronze.crm_product_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_product_info;
GO
CREATE TABLE bronze.crm_product_info (
    product_id          INT,
    product_key         NVARCHAR(50),
    product_nm          NVARCHAR(50),
    product_cost        INT,
    product_line        NVARCHAR(50),
    product_start_date  DATETIME,
    product_end_date    DATETIME
);
GO

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO
CREATE TABLE bronze.crm_sales_details (
    sales_order_number  NVARCHAR(50),
    sales_product_key   NVARCHAR(50),
    sales_customer_id   INT,       
    sales_order_date    DATETIME,
    sales_shipping_date DATE,
    sales_due_date      DATE,
    sales_sales         INT,
    sales_quantity      INT,
    sales_price         INT,
);
GO

IF OBJECT_ID('bronze.erp_local_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_local_a101;
GO
CREATE TABLE bronze.erp_local_a101 (
		cid     NVARCHAR(50),
		country NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_customer_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_customer_az12;
GO
CREATE TABLE bronze.erp_customer_az12 (
		cid           NVARCHAR(50),
		date_of_birth DATE,
		gender        NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_px_category_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_category_g1v2;
GO
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    category     NVARCHAR(50),
    subcategory  NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
GO