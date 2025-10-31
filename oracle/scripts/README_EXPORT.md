# Oracle Data Export for PostgreSQL Testing

This directory contains scripts to export data from Oracle for PostgreSQL testing.

## Export Scripts

### 1. Full Data Export
**File:** `export_data_for_postgres.sql`

Exports ALL data from Oracle tables to PostgreSQL-compatible INSERT statements.

```bash
# Run in Oracle
cd /mnt/ai/cnn/ai_report/oracle/scripts
sqlplus billing7/billing@bm7 @export_data_for_postgres.sql

# Output files:
# - export_spnet_traffic.sql
# - export_steccom_expenses.sql
# - export_load_logs.sql
# - export_summary.txt
```

### 2. Sample Data Export
**File:** `export_sample_data.sql`

Exports limited sample data (100 SPNET_TRAFFIC, 50 STECCOM_EXPENSES) for quick testing.

```bash
# Run in Oracle
cd /mnt/ai/cnn/ai_report/oracle/scripts
sqlplus billing7/billing@bm7 @export_sample_data.sql

# Output files:
# - sample_spnet_traffic.sql
# - sample_steccom_expenses.sql
# - sample_summary.txt
```

## Usage in PostgreSQL

After exporting, copy the generated SQL files to your PostgreSQL environment:

```bash
# Copy files to PostgreSQL data directory
cp sample_*.sql ../../postgresql/data/

# Load in PostgreSQL (from postgresql directory)
cd ../../postgresql
psql -U postgres -d billing -f data/sample_spnet_traffic.sql
psql -U postgres -d billing -f data/sample_steccom_expenses.sql
```

## Alternative: Direct Export with CSV

You can also export to CSV format:

```sql
-- In Oracle
SET COLSEP ','
SET PAGESIZE 0
SET TRIMSPOOL ON
SET HEADSEP OFF
SET LINESIZE 32767
SPOOL spnet_traffic.csv

SELECT 
    imei, contract_id, 
    TO_CHAR(bill_month, 'YYYY-MM-DD'),
    plan_name, usage_type, 
    usage_bytes, total_amount
FROM spnet_traffic;

SPOOL OFF
```

Then load in PostgreSQL:

```sql
\COPY spnet_traffic(imei, contract_id, bill_month, plan_name, usage_type, usage_bytes, total_amount) 
FROM 'spnet_traffic.csv' 
WITH (FORMAT CSV, DELIMITER ',');
```

## Data Tables Exported

1. **SPNET_TRAFFIC** - Main traffic data
2. **STECCOM_EXPENSES** - Expense records  
3. **LOAD_LOGS** - Load history
4. **TARIFF_PLANS** - Already have tariff_plans_data.sql

## Notes

- The export scripts handle NULL values properly
- Single quotes in strings are escaped
- Dates are formatted for PostgreSQL compatibility
- Use sample export for quick testing
- Use full export for production data migration


