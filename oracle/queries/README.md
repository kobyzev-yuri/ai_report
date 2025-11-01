# Oracle Queries - Iridium Service Management

Practical SQL queries for day-to-day Iridium service management and finance monitoring.

## Scripts

### 1. find_iridium_accounts.sql
**Purpose:** Locate and query Iridium services in billing system

**Queries included:**
1. All active Iridium services
2. Services grouped by customer
3. Find specific service by IMEI or CONTRACT_ID
4. Services with missing IMEI
5. Recent service activity (last 30 days)

**Usage:**
```bash
sqlplus billing7/billing@bm7 @find_iridium_accounts.sql
```

**Interactive:** Prompts for search criteria in Query 3

---

### 2. finance_alerts.sql ⭐ IMPORTANT
**Purpose:** Detect financial issues requiring attention

**Alerts generated:**
1. ⚠️ Services missing in SPNET traffic data
2. 💰 Overage charge discrepancies (>$5 difference)
3. 🔴 High overage charges (>$50)
4. 🏷️ Services missing CODE_1C
5. 📈 Consistent high usage (potential plan upgrade)
6. ⚡ Duplicate IMEI assignments
7. 💤 Services without recent traffic (60+ days)

**Usage:**
```bash
sqlplus billing7/billing@bm7 @finance_alerts.sql > alerts_$(date +%Y%m%d).txt
```

**Recommended:** Run daily or weekly to catch issues early

---

## Common Use Cases

### Daily Operations

**1. Check for new services:**
```sql
SELECT * FROM SERVICES 
WHERE TYPE_ID = 9002 
  AND CREATE_DATE >= TRUNC(SYSDATE) - 7
ORDER BY CREATE_DATE DESC;
```

**2. Find service by IMEI:**
```sql
SELECT SERVICE_ID, LOGIN, PASSWD, STATUS 
FROM SERVICES 
WHERE TYPE_ID = 9002 
  AND PASSWD = '300215010622150';
```

**3. Check customer's all services:**
```sql
SELECT s.SERVICE_ID, s.LOGIN, s.PASSWD, s.STATUS, a.DESCRIPTION
FROM SERVICES s
JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
WHERE s.TYPE_ID = 9002
  AND s.CUSTOMER_ID = 255168;
```

### Finance Checks

**1. Services with high overage this month:**
```sql
SELECT IMEI, CONTRACT_ID, CUSTOMER_NAME, CALCULATED_OVERAGE
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE BILL_MONTH = TO_NUMBER(TO_CHAR(TRUNC(SYSDATE, 'MM'), 'YYYYMM'))
  AND CALCULATED_OVERAGE > 20
ORDER BY CALCULATED_OVERAGE DESC;
```

**2. Find billing discrepancies:**
```sql
SELECT IMEI, BILL_MONTH, 
       CALCULATED_OVERAGE, 
       SPNET_TOTAL_AMOUNT,
       (SPNET_TOTAL_AMOUNT - CALCULATED_OVERAGE) AS DIFFERENCE
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE ABS(SPNET_TOTAL_AMOUNT - CALCULATED_OVERAGE) > 5;
```

**3. Monthly billing summary:**
```sql
SELECT 
    TO_CHAR(BILL_MONTH, 'YYYY-MM') AS MONTH,
    COUNT(*) AS SERVICES,
    SUM(TOTAL_USAGE_KB) AS TOTAL_KB,
    SUM(CALCULATED_OVERAGE) AS TOTAL_OVERAGE
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE BILL_MONTH >= TO_NUMBER(TO_CHAR(ADD_MONTHS(SYSDATE, -6), 'YYYYMM'))
GROUP BY BILL_MONTH
ORDER BY BILL_MONTH DESC;
```

---

## Automated Monitoring

### Daily Alert Email Script

```bash
#!/bin/bash
# daily_finance_alerts.sh

ALERT_FILE="/tmp/iridium_alerts_$(date +%Y%m%d).txt"

# Run finance alerts
sqlplus -s billing7/billing@bm7 @/mnt/ai/cnn/ai_report/oracle/queries/finance_alerts.sql > "$ALERT_FILE"

# Check if there are any alerts
if grep -q "ALERT" "$ALERT_FILE"; then
    # Send email (configure your mail system)
    mail -s "Iridium Finance Alerts - $(date +%Y-%m-%d)" \
         finance@company.com < "$ALERT_FILE"
    
    echo "Alerts found and sent to finance team"
else
    echo "No alerts today"
fi

# Archive
mv "$ALERT_FILE" /archive/alerts/
```

**Setup cron:**
```bash
# Run daily at 8 AM
0 8 * * * /path/to/daily_finance_alerts.sh
```

---

## Quick Reference

### Service Status Codes
- `1` - Active
- `0` - Inactive
- `-10` - Closed (typically for clones)

### Clone Services
- Pattern: `SUB-XXXXXXX-clone-YYYY-MM-DD`
- Indicates service transferred to new agreement
- Exclude from active billing

### Important Tables
- `SERVICES` - Main service records (TYPE_ID = 9002 for Iridium)
- `CUSTOMERS` - Customer information
- `ACCOUNTS` - Account/agreement information
- `SPNET_TRAFFIC` - Usage data from SPNet
- `STECCOM_EXPENSES` - Billing data from STECCOM
- `OUTER_IDS` - External IDs (CODE_1C)

### Key Views
- `V_IRIDIUM_SERVICES_INFO` - Service + customer data
- `V_SPNET_OVERAGE_ANALYSIS` - Usage analysis
- `V_CONSOLIDATED_REPORT_WITH_BILLING` - Complete report

---

## Troubleshooting

### No results from queries?
```sql
-- Check if views exist
SELECT view_name FROM user_views WHERE view_name LIKE 'V_%';

-- Check service count
SELECT COUNT(*) FROM SERVICES WHERE TYPE_ID = 9002;
```

### Slow queries?
```sql
-- Check indexes
SELECT index_name, table_name FROM user_indexes 
WHERE table_name IN ('SERVICES', 'SPNET_TRAFFIC', 'STECCOM_EXPENSES');

-- Add if missing:
CREATE INDEX idx_services_type ON SERVICES(TYPE_ID);
CREATE INDEX idx_services_login ON SERVICES(LOGIN);
```

---

## Production Use

**Remember:**
- Export scripts (`oracle/export/`) are for **testing only**
- Use these query scripts for **daily operations**
- Run `finance_alerts.sql` regularly to catch issues
- Don't export to 1C - query directly from billing system

**For urgent issues:**
1. Run `finance_alerts.sql` immediately
2. Review alert output
3. Take action based on alert type
4. Document resolution







