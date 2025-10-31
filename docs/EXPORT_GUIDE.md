# Quick Guide: Export Oracle Data for PostgreSQL Testing

## Step 1: Export Sample Data from Oracle

```bash
cd /mnt/ai/cnn/ai_report/oracle/scripts
sqlplus billing7/billing@bm7 @export_sample_data.sql
```

This will create:
- `sample_spnet_traffic.sql` (100 records)
- `sample_steccom_expenses.sql` (50 records)  
- `sample_summary.txt` (summary info)

## Step 2: Move Files to PostgreSQL Data Directory

```bash
# Move generated files to PostgreSQL data directory
mv sample_spnet_traffic.sql ../../postgresql/data/
mv sample_steccom_expenses.sql ../../postgresql/data/
```

## Step 3: Setup PostgreSQL Test Database

```bash
cd ../../postgresql

# Create database and load schema
psql -U postgres << EOF
CREATE DATABASE billing;
\c billing
EOF

# Install tables
psql -U postgres -d billing -f tables/install_all_tables.sql

# Load tariff plans
psql -U postgres -d billing -f data/tariff_plans_data.sql

# Load sample data
psql -U postgres -d billing -f data/sample_spnet_traffic.sql
psql -U postgres -d billing -f data/sample_steccom_expenses.sql

# Create function
psql -U postgres -d billing -f functions/calculate_overage.sql

# Create views
psql -U postgres -d billing -f views/install_all_views.sql
```

## Step 4: Test the Setup

```bash
psql -U postgres -d billing << EOF
-- Check data loaded
SELECT COUNT(*) FROM spnet_traffic;
SELECT COUNT(*) FROM steccom_expenses;
SELECT COUNT(*) FROM tariff_plans;

-- Test the views
SELECT * FROM v_spnet_overage_analysis LIMIT 5;
SELECT * FROM v_consolidated_overage_report LIMIT 5;

-- Test calculate_overage function
SELECT calculate_overage('SBD Tiered 1250 10K', 35000);
EOF
```

## Alternative: Full Data Export

If you need ALL data (not just samples):

```bash
cd /mnt/ai/cnn/ai_report/oracle/scripts
sqlplus billing7/billing@bm7 @export_data_for_postgres.sql
```

This generates:
- `export_spnet_traffic.sql` (all records)
- `export_steccom_expenses.sql` (all records)
- `export_load_logs.sql` (all records)

## Troubleshooting

**Problem:** sqlplus not found
```bash
# Check Oracle client installation
which sqlplus
echo $ORACLE_HOME
```

**Problem:** Can't connect to PostgreSQL
```bash
# Check PostgreSQL is running
sudo systemctl status postgresql

# Check connection
psql -U postgres -l
```

**Problem:** Permission denied
```bash
# Become postgres user
sudo -u postgres psql
```

## Quick One-Liner Setup

```bash
# From ai_report root directory
cd oracle/scripts && \
sqlplus billing7/billing@bm7 @export_sample_data.sql && \
mv sample_*.sql ../../postgresql/data/ && \
cd ../../postgresql && \
psql -U postgres -d billing -f data/sample_spnet_traffic.sql && \
psql -U postgres -d billing -f data/sample_steccom_expenses.sql && \
echo "Data loaded successfully!"
```

