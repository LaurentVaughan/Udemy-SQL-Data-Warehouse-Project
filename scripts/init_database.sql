/*
================================
Create Data Warehouse & Schemas
================================
Author: Laurent
Purpose:
  1. Drops existing 'udemy_sql_datawarehouse' if it exists.
  2. Creates a fresh database.
  3. Creates schemas: bronze, silver, gold (if not already present).
  4. Uses safe checks and delays to avoid race conditions.

WARNING: This script will delete the existing database.
         Ensure backups are taken before execution.
*/

-- Step 1: Switch to master context
USE master;
GO

-- Step 2: Drop existing database if it exists
IF EXISTS (
    SELECT 1 FROM sys.databases WHERE name = 'udemy_sql_datawarehouse'
)
BEGIN
    PRINT 'Dropping existing database...';
    ALTER DATABASE udemy_sql_datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE udemy_sql_datawarehouse;
    PRINT 'Database dropped.';
END
GO

-- Step 3: Create new database
PRINT 'Creating new database...';
CREATE DATABASE udemy_sql_datawarehouse;
GO

-- Step 4: Wait to ensure database is ready
WAITFOR DELAY '00:00:02';
GO

-- Step 5: Switch to new database context
USE udemy_sql_datawarehouse;
GO

-- Step 6: Create schemas if they don't exist
PRINT 'Creating schemas...';

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO

PRINT 'Database and schemas created successfully.';