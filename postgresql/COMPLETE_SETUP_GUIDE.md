# Complete PostgreSQL Setup with Billing Integration

Step-by-step guide to set up PostgreSQL with real Oracle billing data.

## Prerequisites

- PostgreSQL 12+ installed
- Oracle dump file: `oracle/test/V_IRIDIUM_SERVICES_INFO.txt` (~30K records)
- CSV files for SPNET_TRAFFIC and STECCOM_EXPENSES (optional)

## Complete Setup (6 Steps)

### Step 1: Create Database

```bash
psql -U postgres << EOF
DROP DATABASE IF EXISTS billing;
CREATE DATABASE billing;
\c billing
\echo 'Database created!'
EOF
```

### Step 2: Create Tables

```bash
cd /mnt/ai/cnn/ai_report/postgresql

psql -U postgres -d billing -f tables/01_spnet_traffic.sql
psql -U postgres -d billing -f tables/02_steccom_expenses.sql
psql -U postgres -d billing -f tables/03_tariff_plans.sql
psql -U postgres -d billing -f tables/04_load_logs.sql
psql -U postgres -d billing -f tables/05_iridium_services_info.sql

echo "✓ Tables created"
```

### Step 3: Load Static Data (Tariff Plans)

```bash
psql -U postgres -d billing -f data/tariff_plans_data.sql

echo "✓ Tariff plans loaded"
```

### Step 4: Import Oracle Dump (Billing Integration) ⭐

```bash
cd data
./import_oracle_dump.sh

# OR manually:
# psql -U postgres -d billing -c "\copy iridium_services_info FROM '../oracle/test/V_IRIDIUM_SERVICES_INFO.txt' WITH (FORMAT text, DELIMITER E'\t', HEADER true, NULL 'NULL');"

echo "✓ Oracle billing data imported (~30K records)"
```

### Step 5: Create Functions

```bash
cd ..
psql -U postgres -d billing -f functions/calculate_overage.sql

echo "✓ Functions created"
```

### Step 6: Create Views (Including Billing Integration)

```bash
psql -U postgres -d billing -f views/install_all_views.sql

# This creates:
# - V_SPNET_OVERAGE_ANALYSIS
# - V_CONSOLIDATED_OVERAGE_REPORT  
# - V_IRIDIUM_SERVICES_INFO (wrapper)
# - V_CONSOLIDATED_REPORT_WITH_BILLING ⭐ NEW with billing fields

echo "✓ All views created"
```

---

## Verify Setup

```bash
psql -U postgres -d billing -f test_billing_report.sql
```

This will show:
- Record counts
- Sample data with new fields (SERVICE_ID, CODE_1C, ORGANIZATION_NAME)
- Billing integration statistics

---

## Test Queries

### Query 1: Full report with billing fields

```sql
SELECT 
    imei,
    contract_id,
    service_id,              -- From billing
    code_1c,                 -- From billing
    organization_name,       -- From billing
    customer_name,           -- From billing
    agreement_number,        -- From billing
    total_usage_kb,
    calculated_overage
FROM v_consolidated_report_with_billing
WHERE service_id IS NOT NULL
ORDER BY calculated_overage DESC
LIMIT 20;
```

### Query 2: Services by organization

```sql
SELECT 
    organization_name,
    code_1c,
    COUNT(DISTINCT service_id) as services,
    SUM(calculated_overage) as total_overage
FROM v_consolidated_report_with_billing
WHERE organization_name IS NOT NULL
GROUP BY organization_name, code_1c
ORDER BY SUM(calculated_overage) DESC
LIMIT 10;
```

### Query 3: Find specific customer by CODE_1C

```sql
SELECT 
    service_id,
    imei,
    customer_name,
    agreement_number,
    total_usage_kb,
    calculated_overage
FROM v_consolidated_report_with_billing
WHERE code_1c = '1C00123'  -- Replace with actual CODE_1C
ORDER BY bill_month DESC;
```

---

## What You Get

### New Fields in V_CONSOLIDATED_REPORT_WITH_BILLING:

| Field | Type | Description | Source |
|-------|------|-------------|--------|
| **service_id** | INTEGER | Service ID from billing | Oracle SERVICES table |
| **code_1c** | VARCHAR | 1C customer code | Oracle OUTER_IDS table |
| **organization_name** | VARCHAR | Organization name (юр.лица) | Oracle BM_CUSTOMER_CONTACT |
| **customer_name** | VARCHAR | Org name OR person FIO | Oracle (combined) |
| **agreement_number** | VARCHAR | Contract number | Oracle ACCOUNTS |
| **order_number** | VARCHAR | Order/appendix (бланк) | Oracle SERVICES |
| service_status | INTEGER | Service status (1=active) | Oracle SERVICES |
| customer_id | INTEGER | Customer ID | Oracle CUSTOMERS |
| account_id | INTEGER | Account ID | Oracle ACCOUNTS |

Plus all existing fields:
- bill_month, imei, contract_id, plan_name
- total_usage_kb, overage_kb, calculated_overage
- spnet_total_amount, steccom_total_amount

---

## Optional: Load Traffic Data

If you have SPNET_TRAFFIC and STECCOM_EXPENSES CSV files:

```bash
# From project root
cd python

# Configure database connection in scripts
# Then load:
python load_spnet_traffic.py
python load_steccom_expenses.py
```

---

## One-Liner Complete Setup

```bash
cd /mnt/ai/cnn/ai_report/postgresql && \
psql -U postgres -c "DROP DATABASE IF EXISTS billing; CREATE DATABASE billing;" && \
for f in tables/*.sql; do psql -U postgres -d billing -f "$f"; done && \
psql -U postgres -d billing -f data/tariff_plans_data.sql && \
cd data && ./import_oracle_dump.sh && cd .. && \
psql -U postgres -d billing -f functions/calculate_overage.sql && \
psql -U postgres -d billing -f views/install_all_views.sql && \
psql -U postgres -d billing -f test_billing_report.sql && \
echo "✓✓✓ Setup complete with billing integration! ✓✓✓"
```

---

## Troubleshooting

### Oracle dump not found

```bash
ls -lh ../oracle/test/V_IRIDIUM_SERVICES_INFO.txt
# If missing, export from Oracle first
```

### Import fails

```bash
# Check file format
head -5 ../oracle/test/V_IRIDIUM_SERVICES_INFO.txt

# Clean and retry
tail -n +4 ../oracle/test/V_IRIDIUM_SERVICES_INFO.txt | \
  sed 's/  */\t/g' > /tmp/cleaned_dump.txt
  
psql -U postgres -d billing -c "\copy iridium_services_info FROM '/tmp/cleaned_dump.txt' WITH (FORMAT csv, DELIMITER E'\t', NULL 'NULL');"
```

### View shows NULL for billing fields

```bash
# Check if data loaded
psql -U postgres -d billing -c "SELECT COUNT(*) FROM iridium_services_info;"

# Check if view created correctly
psql -U postgres -d billing -c "\d+ v_consolidated_report_with_billing"

# Recreate view
psql -U postgres -d billing -f views/04_v_consolidated_report_with_billing.sql
```

---

## Next Steps

After successful setup:

1. **Test queries** - Use test_billing_report.sql
2. **Load traffic data** - Optional, for usage analysis
3. **Compare with Oracle** - Export from both and compare results
4. **Develop features** - Safe to test on PostgreSQL before deploying to Oracle

---

## Summary

You now have:
- ✅ PostgreSQL test database
- ✅ ~30,000 real service records from Oracle
- ✅ Billing integration (SERVICE_ID, CODE_1C, ORGANIZATION_NAME)
- ✅ All views including V_CONSOLIDATED_REPORT_WITH_BILLING
- ✅ Calculate_overage function
- ✅ Ready for testing and development!







