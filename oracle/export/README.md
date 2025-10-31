# Oracle Export Scripts

Export scripts for Oracle database - view results comparison and 1C billing integration.

## Scripts Overview

### 1. export_billing_integration.sql ⭐ MAIN USE
**Purpose:** Export customer billing data for 1C integration

**Fields exported:**
- `CUSTOMER_NAME` - Organization name or person FIO (ФИО)
- `AGREEMENT_NUMBER` - Contract number (номер договора)
- `ORDER_NUMBER` - Order/appendix number (бланк)
- `CODE_1C` - 1C customer code (код 1С)
- Plus: CONTRACT_ID, IMEI, STATUS

**Filters:**
- Only active services (`STATUS = 1`)
- Excludes clone/transferred services (`CONTRACT_ID NOT LIKE '%-clone-%'`)

**Usage:**
```bash
cd /mnt/ai/cnn/ai_report/oracle/export
sqlplus billing7/billing@bm7 @export_billing_integration.sql
```

**Output files:**
1. `billing_integration.csv` - Basic customer data (pipe-delimited)
2. `billing_integration.sql` - INSERT statements for SQL load
3. `billing_integration_with_charges.csv` - Extended with monthly charges
4. `billing_integration_summary.txt` - Export statistics

### 2. export_service_history.sql
**Purpose:** View service transfer/clone history for audit

Shows ALL services including closed/transferred ones with `-clone-` suffix.

**Usage:**
```bash
sqlplus billing7/billing@bm7 @export_service_history.sql
```

**Output:**
- `service_transfer_history.csv` - Complete history with clones
- `service_transfer_summary.txt` - Transfer statistics

**Use when:** Need to audit service transfers or understand service lifecycle

### 3. export_view_results.sql
**Purpose:** Export view results for Oracle/PostgreSQL comparison

**Usage:**
```bash
cd /mnt/ai/cnn/ai_report/oracle/export
sqlplus billing7/billing@bm7 @export_view_results.sql
```

**Output:** Oracle view results for comparison testing

---

## Quick Start: 1C Integration

### Export from Oracle
```bash
cd /mnt/ai/cnn/ai_report/oracle/export
sqlplus billing7/billing@bm7 @export_billing_integration.sql
```

### Import to 1C
The CSV format is pipe-delimited (|) for better handling of commas in names:

```
CONTRACT_ID|IMEI|CUSTOMER_NAME|AGREEMENT_NUMBER|ORDER_NUMBER|CODE_1C|STATUS
123456|300234012345678|ООО "Рога и Копыта"|Д-2024-001|БЛ-001|1C00123|1
```

**Excel/1C import settings:**
- Delimiter: Pipe (|)
- Text qualifier: None
- Encoding: UTF-8
- First row: Headers

### Monthly Billing Report
Use `billing_integration_with_charges.csv` for monthly reports:

```
CONTRACT_ID|IMEI|CUSTOMER_NAME|...|BILL_MONTH|PLAN_NAME|USAGE_KB|OVERAGE_CHARGE|TOTAL_AMOUNT
123456|300234012345678|ООО "Рога и Копыта"|...|2024-10|SBD Tiered 1250 10K|45.5|12.50|18.00
```

This includes:
- Monthly usage data
- Calculated overage charges
- Total amounts for billing

---

## File Formats

### CSV Format (billing_integration.csv)
```csv
CONTRACT_ID|IMEI|CUSTOMER_NAME|AGREEMENT_NUMBER|ORDER_NUMBER|CODE_1C|STATUS
```

**Example:**
```
123456|300234012345678|ООО "Рога и Копыта"|Д-2024-001|БЛ-001|1C00123|1
234567|300234012345679|Иванов Иван Иванович|Д-2024-002|БЛ-002|1C00124|1
```

### SQL Format (billing_integration.sql)
```sql
INSERT INTO billing_integration (...) VALUES (...);
```

Use this format if you want to load data into PostgreSQL or another SQL database for integration.

---

## Data Dictionary

| Field | Description | Source | Example |
|-------|-------------|--------|---------|
| `CONTRACT_ID` | Service login | SERVICES.LOGIN | 123456 |
| `IMEI` | Device IMEI | SERVICES.PASSWD | 300234012345678 |
| `CUSTOMER_NAME` | Organization or person name | BM_CUSTOMER_CONTACT | ООО "Рога и Копыта" |
| `AGREEMENT_NUMBER` | Contract number | ACCOUNTS.DESCRIPTION | Д-2024-001 |
| `ORDER_NUMBER` | Order/appendix (blank) | SERVICES.BLANK | БЛ-001 |
| `CODE_1C` | 1C customer code | OUTER_IDS.EXT_ID | 1C00123 |
| `STATUS` | Service status (1=active) | SERVICES.STATUS | 1 |

---

## Filters Applied

**Active Services Only:**
- Only exports services where `STATUS = 1` (active)
- Inactive/closed services are excluded
- Reduces noise in 1C integration

**Clone Services Excluded:**
- Services with `CONTRACT_ID` like `%-clone-%` are excluded
- These are closed services that were transferred to another agreement
- The `-clone-YYYY-MM-DD` suffix indicates when service was closed
- Example: `SUB-5397839455-clone-2018-05-01` (closed on May 1, 2018)
- The same login exists with new SERVICE_ID on different agreement
- For billing, only the active (non-clone) version is needed

**Service Transfer Pattern:**
```
Original:  SERVICE_ID=350000, CONTRACT_ID=SUB-5397839455, STATUS=1
↓ (transferred to new agreement)
Clone:     SERVICE_ID=350000, CONTRACT_ID=SUB-5397839455-clone-2018-05-01, STATUS=-10
New:       SERVICE_ID=350001, CONTRACT_ID=SUB-5397839455, STATUS=1
```

**To see transfer history**, use:
```bash
sqlplus billing7/billing@bm7 @export_service_history.sql
```

---

## Usage Examples

### Example 1: Daily 1C sync
```bash
#!/bin/bash
# daily_1c_sync.sh

cd /mnt/ai/cnn/ai_report/oracle/export

# Export fresh data
sqlplus -s billing7/billing@bm7 @export_billing_integration.sql

# Copy to 1C import directory
cp billing_integration.csv /path/to/1c/import/

# Archive with date
cp billing_integration.csv "archive/billing_$(date +%Y%m%d).csv"

echo "1C integration data updated: $(date)"
```

### Example 2: Monthly billing report
```bash
# Export monthly charges for current month
sqlplus billing7/billing@bm7 << EOF
SET PAGESIZE 0
SET LINESIZE 500
SET COLSEP '|'
SPOOL monthly_billing_$(date +%Y%m).csv

SELECT 
    CODE_1C,
    CUSTOMER_NAME,
    AGREEMENT_NUMBER,
    COUNT(*) as SERVICES_COUNT,
    SUM(CALCULATED_OVERAGE) as TOTAL_OVERAGE,
    SUM(SPNET_TOTAL_AMOUNT + STECCOM_TOTAL_AMOUNT) as TOTAL_CHARGES
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE BILL_MONTH = TRUNC(SYSDATE, 'MM')
  AND SERVICE_STATUS = 1
GROUP BY CODE_1C, CUSTOMER_NAME, AGREEMENT_NUMBER;

SPOOL OFF
EXIT
EOF
```

---

## Troubleshooting

### No data exported
**Check:**
```sql
-- How many active services?
SELECT COUNT(*) FROM V_IRIDIUM_SERVICES_INFO WHERE STATUS = 1;

-- Are views created?
SELECT view_name FROM user_views WHERE view_name LIKE 'V_%';
```

### Missing CODE_1C values
Some customers may not have 1C codes assigned yet:
```sql
-- Find services without 1C codes
SELECT CONTRACT_ID, IMEI, CUSTOMER_NAME 
FROM V_IRIDIUM_SERVICES_INFO 
WHERE STATUS = 1 AND CODE_1C IS NULL;
```

### Character encoding issues
Set proper encoding before export:
```bash
export NLS_LANG=AMERICAN_AMERICA.UTF8
sqlplus billing7/billing@bm7 @export_billing_integration.sql
```

---

## Notes

- **Data Source:** Queries live production billing database (billing7@bm7)
- **Refresh Frequency:** Run as needed (daily/weekly/monthly)
- **CSV Delimiter:** Pipe (|) to avoid conflicts with commas in names
- **NULL Handling:** Empty strings in CSV, NULL in SQL format
- **Performance:** Exports typically complete in < 30 seconds
