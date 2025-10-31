# Billing Integration Export Guide

Quick guide to export customer billing data for 1C integration from Oracle.

## What Gets Exported

Only the fields needed for 1C billing integration:

| Field | Description | Example |
|-------|-------------|---------|
| **CUSTOMER_NAME** | Organization or person FIO | ООО "Рога и Копыта" |
| **AGREEMENT_NUMBER** | Contract number (договор) | Д-2024-001 |
| **ORDER_NUMBER** | Order/blank (бланк) | БЛ-001 |
| **CODE_1C** | 1C customer code | 1C00123 |
| CONTRACT_ID | Service login (for linking) | 123456 |
| IMEI | Device IMEI | 300234012345678 |
| STATUS | Service status (1=active) | 1 |

## Quick Export

### Step 1: Export from Oracle

```bash
cd /mnt/ai/cnn/ai_report/oracle/export
sqlplus billing7/billing@bm7 @export_billing_integration.sql
```

**Files created:**
- `billing_integration.csv` ← **Use this for 1C**
- `billing_integration_with_charges.csv` ← Monthly billing report
- `billing_integration.sql` (SQL format, if needed)
- `billing_integration_summary.txt` (statistics)

### Step 2: Import to 1C

Import `billing_integration.csv` into 1C:

**File format:**
- Delimiter: Pipe `|`
- Encoding: UTF-8
- Header row: Yes

**Sample data:**
```
CONTRACT_ID|IMEI|CUSTOMER_NAME|AGREEMENT_NUMBER|ORDER_NUMBER|CODE_1C|STATUS
123456|300234012345678|ООО "Рога и Копыта"|Д-2024-001|БЛ-001|1C00123|1
234567|300234012345679|Иванов Иван Иванович|Д-2024-002|БЛ-002|1C00124|1
```

## Monthly Billing Report

Use `billing_integration_with_charges.csv` for monthly invoicing:

**Additional fields:**
- BILL_MONTH - Billing period (YYYY-MM)
- PLAN_NAME - Tariff plan
- USAGE_KB - Data usage in KB
- OVERAGE_CHARGE - Calculated overage charge ($)
- TOTAL_AMOUNT - Total charges ($)

**Sample:**
```
CONTRACT_ID|IMEI|CUSTOMER_NAME|...|BILL_MONTH|USAGE_KB|OVERAGE_CHARGE|TOTAL_AMOUNT
123456|300234012345678|ООО "Рога и Копыта"|...|2024-10|45.5|12.50|18.00
```

## Automation

Create a cron job for daily/weekly sync:

```bash
#!/bin/bash
# /path/to/daily_billing_export.sh

cd /mnt/ai/cnn/ai_report/oracle/export

# Run export
sqlplus -s billing7/billing@bm7 @export_billing_integration.sql

# Copy to 1C import location
cp billing_integration.csv /path/to/1c/import/billing_data.csv

# Archive with date
cp billing_integration.csv archive/billing_$(date +%Y%m%d).csv

echo "Export completed: $(date)" >> export.log
```

**Crontab:**
```bash
# Daily at 6 AM
0 6 * * * /path/to/daily_billing_export.sh
```

## Important Notes

### Clone Services Excluded
Services with `-clone-` suffix in CONTRACT_ID are **automatically excluded**.

**What are clones?**
- When a service is transferred to another agreement, the old record gets `-clone-YYYY-MM-DD` suffix
- Clone services have STATUS = -10 (closed) and STOP_DATE populated
- The same login exists with a new SERVICE_ID on the new agreement
- Example: `SUB-5397839455-clone-2018-05-01` (closed May 1, 2018)

**For 1C billing:** Only active, non-clone services are exported.

**To audit transfers:** Use `export_service_history.sql` to see complete history.

### Other Filters
- **Only active services** are exported (STATUS = 1)
- **Data source:** Production billing database (billing7@bm7)
- **No raw table data** is exported (tables loaded from CSV separately)
- **Views must be created** before export (see oracle/views/)

## Support

For full documentation, see:
- `oracle/export/README.md` - Detailed export documentation
- `oracle/views/` - View definitions
- `EXPORT_GUIDE.md` - General export info

