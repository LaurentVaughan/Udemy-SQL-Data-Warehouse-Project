# Orchestration Strategy for Data Warehouse ETL

## Overview
This document explains the multi-tier orchestration approach for the SQL data warehouse project, covering Bronze → Silver → Gold layers and overall pipeline execution.

---

## Architecture: Layered Orchestration

### **Layer-Specific Orchestrators** (SQL Procedures)
Each medallion layer has its own orchestration procedure:

```
setup/
  ├── orchestrate_bronze.sql    ← Bronze layer orchestrator
  ├── orchestrate_silver.sql    ← Silver layer orchestrator (future)
  └── orchestrate_gold.sql      ← Gold layer orchestrator (future)
```

**Purpose:** Each orchestrator validates prerequisites, creates metadata tables, and coordinates layer-specific setup.

**Example - Bronze Layer:**
```sql
-- Defined in setup/orchestrate_bronze.sql
CALL setup.orchestrate_bronze();

-- What it does:
-- 1. Validates bronze schema exists
-- 2. Validates etl_config table exists
-- 3. Validates bronze.load_log exists
-- 4. Creates bronze.load_jobs metadata table
-- 5. Populates bronze.load_jobs from discovered tables
```

---

## Main Orchestration Strategies

You have **three options** for coordinating the entire pipeline:

### **Option 1: SQL Master Orchestrator** (Recommended for simplicity)

**File:** `setup/orchestrate_all.sql`

```sql
/*
================================
setup/orchestrate_all.sql
================================
Master orchestrator for complete data warehouse setup
*/

CREATE OR REPLACE PROCEDURE setup.orchestrate_all()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Starting complete data warehouse orchestration...';
    
    -- Bronze Layer
    RAISE NOTICE '[1/3] Orchestrating Bronze layer...';
    CALL setup.orchestrate_bronze();
    
    -- Silver Layer
    RAISE NOTICE '[2/3] Orchestrating Silver layer...';
    CALL setup.orchestrate_silver();
    
    -- Gold Layer
    RAISE NOTICE '[3/3] Orchestrating Gold layer...';
    CALL setup.orchestrate_gold();
    
    RAISE NOTICE 'Complete data warehouse orchestration finished successfully!';
END;
$$;

-- Usage:
-- CALL setup.orchestrate_all();
```

**Pros:**
- ✅ Pure SQL - no external dependencies
- ✅ Database-native transaction support
- ✅ Simple deployment (just run one SQL file)
- ✅ Easy to debug in psql/IDE

**Cons:**
- ❌ Limited logging/monitoring capabilities
- ❌ No parallel execution (unless using advanced PL/pgSQL)
- ❌ Harder to integrate with modern CI/CD tools

---

### **Option 2: Python Orchestrator** (Recommended for production)

**File:** `orchestrate_all.py` (project root)

```python
#!/usr/bin/env python3
"""
orchestrate_all.py
==================
Master orchestrator for SQL data warehouse ETL pipeline.
Coordinates Bronze → Silver → Gold layer execution with logging and error handling.
"""

import psycopg2
import logging
from datetime import datetime
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataWarehouseOrchestrator:
    """Orchestrates complete ETL pipeline across all medallion layers."""
    
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.conn: Optional[psycopg2.extensions.connection] = None
        
    def connect(self):
        """Establish database connection."""
        logger.info("Connecting to database: %s", self.db_config['database'])
        self.conn = psycopg2.connect(**self.db_config)
        self.conn.autocommit = True
        
    def disconnect(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def execute_procedure(self, procedure_name: str, layer: str):
        """Execute a stored procedure and log results."""
        logger.info("[%s] Executing: %s", layer, procedure_name)
        start_time = datetime.now()
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"CALL {procedure_name}()")
            
            # Fetch NOTICE messages from PostgreSQL
            for notice in self.conn.notices:
                logger.info("[%s] %s", layer, notice.strip())
            self.conn.notices.clear()
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.info("[%s] Completed in %.2f seconds", layer, duration)
            cursor.close()
            
        except Exception as e:
            logger.error("[%s] FAILED: %s", layer, str(e))
            raise
    
    def orchestrate_bronze(self):
        """Orchestrate Bronze layer setup."""
        self.execute_procedure('setup.orchestrate_bronze', 'BRONZE')
    
    def orchestrate_silver(self):
        """Orchestrate Silver layer setup."""
        self.execute_procedure('setup.orchestrate_silver', 'SILVER')
    
    def orchestrate_gold(self):
        """Orchestrate Gold layer setup."""
        self.execute_procedure('setup.orchestrate_gold', 'GOLD')
    
    def run_full_pipeline(self):
        """Execute complete Bronze → Silver → Gold pipeline."""
        logger.info("=" * 70)
        logger.info("STARTING COMPLETE DATA WAREHOUSE ORCHESTRATION")
        logger.info("=" * 70)
        
        pipeline_start = datetime.now()
        
        try:
            self.connect()
            
            # Layer 1: Bronze (raw data ingestion)
            logger.info("\n[LAYER 1/3] BRONZE - Raw Data Ingestion")
            logger.info("-" * 70)
            self.orchestrate_bronze()
            
            # Layer 2: Silver (cleaned, conformed data)
            logger.info("\n[LAYER 2/3] SILVER - Data Cleansing & Conformance")
            logger.info("-" * 70)
            self.orchestrate_silver()
            
            # Layer 3: Gold (aggregated, business-ready data)
            logger.info("\n[LAYER 3/3] GOLD - Business Aggregations")
            logger.info("-" * 70)
            self.orchestrate_gold()
            
            total_duration = (datetime.now() - pipeline_start).total_seconds()
            
            logger.info("\n" + "=" * 70)
            logger.info("ORCHESTRATION COMPLETE - Total time: %.2f seconds", total_duration)
            logger.info("=" * 70)
            
        except Exception as e:
            logger.error("\n" + "!" * 70)
            logger.error("ORCHESTRATION FAILED: %s", str(e))
            logger.error("!" * 70)
            raise
        
        finally:
            self.disconnect()


def main():
    """Main entry point."""
    # Database configuration
    db_config = {
        'host': 'localhost',
        'database': 'sql_retail_analytics_warehouse',
        'user': 'postgres',
        'password': 'your_password_here'  # Use env vars in production
    }
    
    # Create orchestrator and run pipeline
    orchestrator = DataWarehouseOrchestrator(db_config)
    orchestrator.run_full_pipeline()


if __name__ == '__main__':
    main()
```

**Usage:**
```bash
# Run complete pipeline
python orchestrate_all.py

# Or with environment variables
export POSTGRES_PASSWORD=mypassword
python orchestrate_all.py
```

**Pros:**
- ✅ Rich logging and monitoring
- ✅ Easy integration with CI/CD (GitHub Actions, GitLab CI, Jenkins)
- ✅ Can add email notifications, Slack alerts, metrics
- ✅ Parallel execution possible with `concurrent.futures`
- ✅ Better error handling and retry logic
- ✅ Can integrate with data quality frameworks (Great Expectations)

**Cons:**
- ❌ Requires Python environment
- ❌ Additional dependency management (psycopg2)
- ❌ More complex than pure SQL

---

### **Option 3: Shell Script Orchestrator** (Simple alternative)

**File:** `orchestrate_all.sh` (project root)

```bash
#!/bin/bash
# orchestrate_all.sh
# Master orchestrator for data warehouse ETL pipeline

set -e  # Exit on error

# Configuration
DB_NAME="sql_retail_analytics_warehouse"
DB_USER="postgres"
DB_HOST="localhost"

echo "======================================================================"
echo "STARTING COMPLETE DATA WAREHOUSE ORCHESTRATION"
echo "======================================================================"

start_time=$(date +%s)

# Function to execute SQL procedure
execute_procedure() {
    local procedure=$1
    local layer=$2
    
    echo ""
    echo "[$layer] Executing: $procedure"
    echo "----------------------------------------------------------------------"
    
    psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" \
        -c "CALL $procedure();" \
        || { echo "[$layer] FAILED!"; exit 1; }
    
    echo "[$layer] Completed successfully"
}

# Bronze Layer
echo ""
echo "[LAYER 1/3] BRONZE - Raw Data Ingestion"
echo "----------------------------------------------------------------------"
execute_procedure "setup.orchestrate_bronze" "BRONZE"

# Silver Layer
echo ""
echo "[LAYER 2/3] SILVER - Data Cleansing & Conformance"
echo "----------------------------------------------------------------------"
execute_procedure "setup.orchestrate_silver" "SILVER"

# Gold Layer
echo ""
echo "[LAYER 3/3] GOLD - Business Aggregations"
echo "----------------------------------------------------------------------"
execute_procedure "setup.orchestrate_gold" "GOLD"

# Calculate total duration
end_time=$(date +%s)
duration=$((end_time - start_time))

echo ""
echo "======================================================================"
echo "ORCHESTRATION COMPLETE - Total time: ${duration}s"
echo "======================================================================"
```

**Usage:**
```bash
chmod +x orchestrate_all.sh
./orchestrate_all.sh
```

**Pros:**
- ✅ Simple and portable
- ✅ No additional dependencies (just psql)
- ✅ Easy to integrate with cron jobs

**Cons:**
- ❌ Limited error handling
- ❌ Basic logging only
- ❌ Platform-dependent (Bash on Linux/Mac, requires WSL on Windows)

---

## Recommended Approach for Your Project

### **For Development/Learning:** Option 1 (SQL Master Orchestrator)
- Simple, pure SQL
- Easy to understand and debug
- Good for learning the orchestration patterns

### **For Production:** Option 2 (Python Orchestrator)
- Professional-grade logging
- Easy CI/CD integration
- Flexible for adding monitoring, alerts, retries
- Industry-standard approach

---

## Complete Project Structure

```
Udemy-SQL-Data-Warehouse-Project/
├── orchestrate_all.py              ← Main orchestrator (Option 2)
├── orchestrate_all.sh              ← Shell orchestrator (Option 3)
├── requirements.txt                ← Python dependencies
│
├── setup/
│   ├── create_db.sql              ← Database creation
│   ├── create_schemas.sql         ← Schema creation
│   ├── orchestrate_bronze.sql     ← Bronze orchestrator
│   ├── orchestrate_silver.sql     ← Silver orchestrator (future)
│   ├── orchestrate_gold.sql       ← Gold orchestrator (future)
│   ├── orchestrate_all.sql        ← SQL master orchestrator (Option 1)
│   │
│   └── seed/
│       ├── 01_etl_config.sql      ← Config data seeding
│       └── 02_register_bronze_jobs.sql
│
├── scripts/
│   ├── bronze/
│   │   ├── ddl_bronze_log.sql
│   │   ├── ddl_bronze_tables.sql
│   │   └── load_bronze.sql
│   │
│   ├── silver/                    ← Future
│   └── gold/                      ← Future
│
└── tests/
    ├── test_orchestrate_bronze.ipynb
    ├── test_orchestrate_silver.ipynb  ← Future
    └── test_orchestrate_gold.ipynb    ← Future
```

---

## Execution Flow (Complete Pipeline)

```
1. Manual Prerequisites (run once):
   ├─ setup/create_db.sql           → Creates database
   ├─ setup/create_schemas.sql      → Creates bronze/silver/gold schemas
   └─ setup/seed/01_etl_config.sql  → Seeds configuration

2. Layer-Specific DDL (run once per layer):
   Bronze:
   ├─ scripts/bronze/ddl_bronze_log.sql    → Creates load_log table
   └─ scripts/bronze/ddl_bronze_tables.sql → Creates data tables
   
   Silver:
   └─ scripts/silver/ddl_silver_tables.sql → Creates silver tables (future)
   
   Gold:
   └─ scripts/gold/ddl_gold_tables.sql     → Creates gold tables (future)

3. Deploy Orchestration Procedures:
   ├─ setup/orchestrate_bronze.sql  → Deploys bronze orchestrator
   ├─ setup/orchestrate_silver.sql  → Deploys silver orchestrator
   └─ setup/orchestrate_gold.sql    → Deploys gold orchestrator

4. Run Main Orchestrator (repeatable):
   
   Option A (SQL):
   └─ CALL setup.orchestrate_all();
   
   Option B (Python):
   └─ python orchestrate_all.py
   
   Option C (Shell):
   └─ ./orchestrate_all.sh
```

---

## CI/CD Integration Example (GitHub Actions)

```yaml
# .github/workflows/etl-pipeline.yml
name: ETL Pipeline

on:
  schedule:
    - cron: '0 2 * * *'  # Daily at 2 AM
  workflow_dispatch:      # Manual trigger

jobs:
  run-etl:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          pip install psycopg2-binary
      
      - name: Run ETL Pipeline
        env:
          POSTGRES_PASSWORD: ${{ secrets.POSTGRES_PASSWORD }}
        run: |
          python orchestrate_all.py
      
      - name: Notify on failure
        if: failure()
        run: echo "ETL pipeline failed!"
```

---

## Summary

**Immediate Next Steps:**
1. ✅ File moved and renamed: `setup/orchestrate_bronze.sql`
2. ✅ Procedure renamed: `setup.orchestrate_bronze()`
3. Create similar orchestrators for silver/gold when ready
4. Choose main orchestration approach (Python recommended for production)

**Future Expansion:**
- Add `setup/orchestrate_silver.sql` when building silver layer
- Add `setup/orchestrate_gold.sql` when building gold layer  
- Create `orchestrate_all.py` (or `.sql` or `.sh`) as main orchestrator
- Integrate with CI/CD for automated daily runs
