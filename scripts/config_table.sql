/*
==========================================
Create Configuration Table for File Paths
==========================================
Purpose:
    Initialize centralized configuration table for ETL file paths

Overview:
    - Creates dbo.etl_config table if it does not exist
    - Inserts base path for CRM source if not already present
    - Enables dynamic path resolution for BULK INSERT operations
    - Supports modular, maintainable ETL pipeline aligned with medallion architecture

Table Schema:
    dbo.etl_config (
        config_key     NVARCHAR(100) PRIMARY KEY,
        config_value   NVARCHAR(200)
    )

Usage:
    - Retrieve config values in ETL scripts using:
        SELECT config_value FROM dbo.etl_config WHERE config_key = 'base_path_crm';

    - To add new paths for other sources or layers:
        IF NOT EXISTS (
            SELECT 1 FROM dbo.etl_config WHERE config_key = 'base_path_example'
        )
        BEGIN
            INSERT INTO dbo.etl_config (config_key, config_value)
            VALUES (
                'base_path_example',
                'C:\path\to\example\source\'
            );
        END;

    - Recommended keys:
        'base_path_crm'      → CRM source folder
        'base_path_erp'      → ERP source folder
        'silver_load_mode'   → e.g. 'truncate_insert'
        'gold_model_type'    → e.g. 'star_schema'

Notes:
    - This script is idempotent and safe to re-run
    - Use MERGE for upsert logic if config values may change
    - Consider adding metadata columns (e.g. last_updated, description) for auditability
*/

-- Create config table if it doesn't exist
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables
    WHERE name = 'etl_config'
      AND schema_id = SCHEMA_ID('dbo')
)
BEGIN
    CREATE TABLE dbo.etl_config (
        config_key   NVARCHAR(100) PRIMARY KEY,
        config_value NVARCHAR(200)
    );
END;
GO

-- Insert CRM base path if not already present
IF NOT EXISTS (
    SELECT 1
    FROM dbo.etl_config
    WHERE config_key = 'base_path_crm'
)
BEGIN
    INSERT INTO dbo.etl_config (config_key, config_value)
    VALUES (
        'base_path_crm',
        'C:\Users\Laurent\Studies\sql-ultimate-course\Udemy-SQL-Data-Warehouse-Project\datasets\source_crm\'
    );
END;
GO

-- Insert ERP base path if not already present
IF NOT EXISTS (
    SELECT 1
    FROM dbo.etl_config
    WHERE config_key = 'base_path_erp'
)
BEGIN
    INSERT INTO dbo.etl_config (config_key, config_value)
    VALUES (
        'base_path_erp',
        'C:\Users\Laurent\Studies\sql-ultimate-course\Udemy-SQL-Data-Warehouse-Project\datasets\source_erp\'
    );
END;
