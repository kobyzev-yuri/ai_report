# PostgreSQL Setup with Oracle Dump Data

Complete guide to set up PostgreSQL test database using real Oracle data.

## What You Have

- `oracle/test/V_IRIDIUM_SERVICES_INFO.txt` - **~30,000 real service records** from Oracle
- SPNET_TRAFFIC and STECCOM_EXPENSES CSV files (loaded separately)
- TARIFF_PLANS data

## Quick Setup (5 Steps)

### Step 1: Create Database and Tables

```bash
cd /mnt/ai/cnn/ai_report/postgresql

# Create database
psql -U postgres << EOF
DROP DATABASE IF EXISTS billing;
CREATE DATABASE billing;
EOF

# Create tables
psql -U postgres -d billing -f tables/01_spnet_traffic.sql
psql -U postgres -d billing -f tables/02_steccom_expenses.sql
psql -U postgres -d billing -f tables/03_tariff_plans.sql
psql -U postgres -d billing -f tables/04_load_logs.sql
psql -U postgres -d billing -f tables/05_iridium_services_info.sql
```

### Step 2: Load Tariff Plans

```bash
psql -U postgres -d billing -f data/tariff_plans_data.sql
```

### Step 3: Load Oracle Dump (Billing Integration Data) ⭐

```bash
cd /mnt/ai/cnn/ai_report/postgresql/data

# Check Oracle dump exists
ls -lh ../../oracle/test/V_IRIDIUM_SERVICES_INFO.txt

# Import Oracle data
./import_oracle_dump.sh
```

**OR manually:**

```bash
psql -U postgres -d billing << 'EOF'
-- Load directly (if file format is clean)
\copy IRIDIUM_SERVICES_INFO FROM '/mnt/ai/cnn/ai_report/oracle/test/V_IRIDIUM_SERVICES_INFO.txt' WITH (FORMAT text, DELIMITER E'\t', HEADER true, NULL 'NULL');

-- Check loaded data
SELECT COUNT(*) FROM IRIDIUM_SERVICES_INFO;
SELECT COUNT(*) FROM IRIDIUM_SERVICES_INFO WHERE STATUS = 1;
EOF
```

### Step 4: Create Functions

```bash
psql -U postgres -d billing -f functions/calculate_overage.sql
```

### Step 5: Create Views

```bash
psql -U postgres -d billing -f views/01_v_spnet_overage_analysis.sql
psql -U postgres -d billing -f views/02_v_consolidated_overage_report.sql
psql -U postgres -d billing -f views/03_v_iridium_services_info.sql
psql -U postgres -d billing -f views/04_v_consolidated_report_with_billing.sql
```

---

## Verify Setup

```bash
psql -U postgres -d billing << 'EOF'
\echo ''
\echo '========================================' 
\echo 'Database Setup Verification'
\echo '========================================'
\echo ''

-- Check tables
\echo 'Tables:'
SELECT 
    tablename,
    pg_size_pretty(pg_total_relation_size('public.'||tablename)) as size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY tablename;

-- Check data counts
\echo ''
\echo 'Data Counts:'
SELECT 'SPNET_TRAFFIC' as table_name, COUNT(*) as records FROM spnet_traffic
UNION ALL
SELECT 'STECCOM_EXPENSES', COUNT(*) FROM steccom_expenses
UNION ALL
SELECT 'TARIFF_PLANS', COUNT(*) FROM tariff_plans
UNION ALL
SELECT 'IRIDIUM_SERVICES_INFO', COUNT(*) FROM iridium_services_info;

-- Check Iridium services breakdown
\echo ''
\echo 'Iridium Services Breakdown:'
SELECT 
    'Total services' as category,
    COUNT(*)::text as count
FROM iridium_services_info
UNION ALL
SELECT 
    'Active (STATUS=1)',
    COUNT(*)::text
FROM iridium_services_info
WHERE status = 1
UNION ALL
SELECT 
    'Active (no clone)',
    COUNT(*)::text
FROM iridium_services_info
WHERE status = 1 AND contract_id NOT LIKE '%-clone-%'
UNION ALL
SELECT 
    'With CODE_1C',
    COUNT(*)::text
FROM iridium_services_info
WHERE code_1c IS NOT NULL
UNION ALL
SELECT 
    'With IMEI',
    COUNT(*)::text
FROM iridium_services_info
WHERE imei IS NOT NULL;

-- Check views
\echo ''
\echo 'Views:'
\dv

-- Test calculate_overage function
\echo ''
\echo 'Function Test:'
SELECT 
    'Test 1: SBD-10K, 35KB' as test_case,
    calculate_overage('SBD Tiered 1250 10K', 35000) as result,
    6.50 as expected;

-- Sample data from V_IRIDIUM_SERVICES_INFO
\echo ''
\echo 'Sample Active Services:'
SELECT 
    service_id,
    contract_id,
    imei,
    customer_name,
    agreement_number,
    code_1c,
    status
FROM v_iridium_services_info
WHERE status = 1
  AND contract_id NOT LIKE '%-clone-%'
LIMIT 5;

\echo ''
\echo '========================================'
\echo 'Setup verification complete!'
\echo '========================================'
EOF
```

---

## Test Queries

### Find Active Services

```sql
SELECT 
    contract_id,
    imei,
    customer_name,
    agreement_number,
    order_number,
    code_1c
FROM v_iridium_services_info
WHERE status = 1
  AND contract_id NOT LIKE '%-clone-%'
ORDER BY customer_name
LIMIT 10;
```

### Services by Customer

```sql
SELECT 
    customer_name,
    COUNT(*) as service_count,
    ARRAY_AGG(contract_id) as contract_ids
FROM v_iridium_services_info
WHERE status = 1
  AND contract_id NOT LIKE '%-clone-%'
GROUP BY customer_name
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC
LIMIT 10;
```

### Find Service by IMEI

```sql
SELECT 
    service_id,
    contract_id,
    imei,
    customer_name,
    agreement_number,
    status,
    create_date
FROM v_iridium_services_info
WHERE imei = '300234069603430';
```

### Services Without CODE_1C

```sql
SELECT 
    contract_id,
    customer_name,
    agreement_number,
    status
FROM v_iridium_services_info
WHERE status = 1
  AND contract_id NOT LIKE '%-clone-%'
  AND code_1c IS NULL
ORDER BY create_date DESC
LIMIT 20;
```

---

## Load Additional Data (Optional)

If you have SPNET_TRAFFIC and STECCOM_EXPENSES CSV files:

```bash
# From CSV files
psql -U postgres -d billing << EOF
\copy spnet_traffic(imei, contract_id, bill_month, plan_name, usage_type, usage_bytes, total_amount) FROM '/path/to/spnet_traffic.csv' WITH (FORMAT csv, HEADER true);

\copy steccom_expenses(imei, phone_number, subscriber_name, billing_period, charge_amount, charge_description, charge_date) FROM '/path/to/steccom_expenses.csv' WITH (FORMAT csv, HEADER true);
EOF
```

---

## Complete Consolidated Report

Once you have all data loaded:

```sql
SELECT 
    r.contract_id,
    r.imei,
    r.customer_name,
    r.agreement_number,
    r.code_1c,
    r.bill_month,
    r.plan_name,
    r.total_usage_kb,
    r.overage_kb,
    r.calculated_overage,
    r.spnet_total_amount
FROM v_consolidated_report_with_billing r
WHERE r.service_status = 1
  AND r.bill_month >= TO_CHAR(CURRENT_DATE - INTERVAL '3 months', 'YYYY-MM-DD')::DATE
ORDER BY r.bill_month DESC, r.calculated_overage DESC
LIMIT 20;
```

---

## One-Liner Full Setup

```bash
cd /mnt/ai/cnn/ai_report/postgresql && \
psql -U postgres -c "DROP DATABASE IF EXISTS billing; CREATE DATABASE billing;" && \
for f in tables/*.sql; do psql -U postgres -d billing -f "$f"; done && \
psql -U postgres -d billing -f data/tariff_plans_data.sql && \
./data/import_oracle_dump.sh && \
for f in functions/*.sql; do psql -U postgres -d billing -f "$f"; done && \
for f in views/*.sql; do psql -U postgres -d billing -f "$f"; done && \
echo "✓ Setup complete!"
```

---

## Troubleshooting

### Import fails with "format not recognized"

The Oracle dump may have formatting issues. Clean it first:

```bash
# Remove header lines and format
tail -n +4 /mnt/ai/cnn/ai_report/oracle/test/V_IRIDIUM_SERVICES_INFO.txt | \
  sed 's/  */\t/g' > /tmp/cleaned_dump.txt

# Import cleaned file
psql -U postgres -d billing -c "\copy IRIDIUM_SERVICES_INFO FROM '/tmp/cleaned_dump.txt' WITH (FORMAT csv, DELIMITER E'\t', NULL 'NULL');"
```

### Character encoding issues

```bash
# Check file encoding
file -i /mnt/ai/cnn/ai_report/oracle/test/V_IRIDIUM_SERVICES_INFO.txt

# Convert if needed
iconv -f ISO-8859-1 -t UTF-8 oracle/test/V_IRIDIUM_SERVICES_INFO.txt > /tmp/utf8_dump.txt
```

### Permission denied

```bash
# Run as postgres user
sudo -u postgres psql -d billing -f tables/05_iridium_services_info.sql
```

---

## Data Source

- **IRIDIUM_SERVICES_INFO**: Real data from Oracle billing system
- **TARIFF_PLANS**: Static configuration data
- **SPNET_TRAFFIC**: CSV import (optional, for usage testing)
- **STECCOM_EXPENSES**: CSV import (optional, for billing testing)

The key integration data comes from the Oracle dump, giving you ~30,000 real service records with customer names, agreements, and 1C codes for realistic testing!







