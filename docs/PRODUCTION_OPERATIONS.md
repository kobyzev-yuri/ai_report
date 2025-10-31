# Iridium Service Operations Guide

## Production vs Testing

### 🧪 Testing (Export Scripts)
**Location:** `oracle/export/`
**Purpose:** Export data for PostgreSQL testing and comparison
**Use case:** Development, testing, validation

### 🚀 Production (Query Scripts)
**Location:** `oracle/queries/` ⭐ **USE THIS**
**Purpose:** Daily operations, monitoring, alerts
**Use case:** Real-world service management

---

## Daily Production Operations

### 1. Find Iridium Accounts

**Script:** `oracle/queries/find_iridium_accounts.sql`

```bash
sqlplus billing7/billing@bm7 @oracle/queries/find_iridium_accounts.sql
```

**What it does:**
- Lists all active Iridium services
- Groups services by customer
- Search by IMEI or CONTRACT_ID (interactive)
- Find services with missing IMEI
- Show recent activity (30 days)

**Use when:**
- Looking up customer services
- Investigating specific IMEI
- Checking service status
- Auditing service inventory

---

### 2. Finance Alerts ⭐ IMPORTANT

**Script:** `oracle/queries/finance_alerts.sql`

```bash
# Run and save to file
sqlplus billing7/billing@bm7 @oracle/queries/finance_alerts.sql > alerts_$(date +%Y%m%d).txt

# Or run and email results
sqlplus billing7/billing@bm7 @oracle/queries/finance_alerts.sql | mail -s "Iridium Alerts" finance@company.com
```

**Alerts detected:**

| Alert | Issue | Action Required |
|-------|-------|-----------------|
| **#1** | Services missing in SPNET data | Check device transmission or data load |
| **#2** | Overage discrepancies (>$5) | Review pricing, investigate errors |
| **#3** | High overage charges (>$50) | Notify customer, suggest plan upgrade |
| **#4** | Missing CODE_1C | Add mapping in OUTER_IDS table |
| **#5** | Consistent high usage (3+ months) | Recommend higher tier plan |
| **#6** | Duplicate IMEI assignments | Review and deactivate duplicates |
| **#7** | No traffic 60+ days | Check if service should be suspended |

**Recommended schedule:**
- **Daily:** Check for critical alerts (#2, #3, #6)
- **Weekly:** Full alert review
- **Monthly:** Alert trend analysis

---

## Common Scenarios

### Scenario 1: Customer Calls About High Bill

```bash
# 1. Find their services
sqlplus billing7/billing@bm7 << EOF
SELECT * FROM V_IRIDIUM_SERVICES_INFO 
WHERE CUSTOMER_NAME LIKE '%ИмяКлиента%';
EOF

# 2. Check usage and charges
sqlplus billing7/billing@bm7 << EOF
SELECT 
    BILL_MONTH,
    IMEI,
    PLAN_NAME,
    TOTAL_USAGE_KB,
    OVERAGE_KB,
    CALCULATED_OVERAGE,
    SPNET_TOTAL_AMOUNT
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE CONTRACT_ID = 'SUB-XXXXXXX'
ORDER BY BILL_MONTH DESC;
EOF
```

### Scenario 2: Check Monthly Billing Before Invoice

```bash
# Run finance alerts for current month
sqlplus billing7/billing@bm7 @oracle/queries/finance_alerts.sql

# Review:
# - Alert #2: Fix discrepancies before invoicing
# - Alert #3: Notify customers with high charges
# - Alert #4: Add missing CODE_1C for 1C integration
```

### Scenario 3: New Service Activation

```sql
-- 1. Verify service created
SELECT SERVICE_ID, LOGIN, PASSWD, STATUS, CREATE_DATE
FROM SERVICES
WHERE TYPE_ID = 9002
  AND LOGIN = 'SUB-XXXXXXX';

-- 2. Check if traffic appears (wait 24 hours)
SELECT COUNT(*), MAX(BILL_MONTH)
FROM SPNET_TRAFFIC
WHERE CONTRACT_ID = 'SUB-XXXXXXX';

-- 3. If no traffic → Alert #1 will catch it
```

### Scenario 4: Service Transfer/Migration

```sql
-- Find old and new service records
SELECT 
    SERVICE_ID,
    LOGIN,
    STATUS,
    START_DATE,
    STOP_DATE,
    CASE WHEN LOGIN LIKE '%-clone-%' THEN 'OLD' ELSE 'NEW' END AS TYPE
FROM SERVICES
WHERE TYPE_ID = 9002
  AND (LOGIN LIKE 'SUB-XXXXXXX%' OR LOGIN = 'SUB-XXXXXXX')
ORDER BY START_DATE;
```

---

## Automated Monitoring Setup

### Daily Alert Script

Create: `/usr/local/bin/iridium_daily_alerts.sh`

```bash
#!/bin/bash
set -e

DATE=$(date +%Y%m%d)
ALERT_FILE="/var/log/iridium/alerts_${DATE}.txt"
RECIPIENT="finance@company.com"

# Create log directory
mkdir -p /var/log/iridium

# Run alerts
sqlplus -s billing7/billing@bm7 @/mnt/ai/cnn/ai_report/oracle/queries/finance_alerts.sql > "$ALERT_FILE" 2>&1

# Count alerts by type
ALERT_COUNT=$(grep -c "^\[ALERT" "$ALERT_FILE" || echo "0")

# Send email if alerts found
if [ "$ALERT_COUNT" -gt 0 ]; then
    {
        echo "Found $ALERT_COUNT finance alerts requiring attention"
        echo ""
        echo "===== SUMMARY ====="
        grep "^\[ALERT" "$ALERT_FILE"
        echo ""
        echo "===== FULL REPORT ====="
        cat "$ALERT_FILE"
    } | mail -s "⚠️ Iridium Finance Alerts - $DATE" "$RECIPIENT"
    
    logger "Iridium: $ALERT_COUNT finance alerts sent to $RECIPIENT"
else
    logger "Iridium: No finance alerts today"
fi

# Cleanup old logs (keep 30 days)
find /var/log/iridium -name "alerts_*.txt" -mtime +30 -delete

exit 0
```

**Setup:**
```bash
chmod +x /usr/local/bin/iridium_daily_alerts.sh

# Add to crontab
crontab -e
# Add line:
0 8 * * * /usr/local/bin/iridium_daily_alerts.sh
```

---

## Integration with 1C

**In production**, you DON'T export to 1C. Instead:

### Option 1: Direct Database Link
1C connects directly to billing database and queries `V_IRIDIUM_SERVICES_INFO`

### Option 2: API/Web Service
Create API endpoint that queries billing and returns JSON:
```json
{
  "contract_id": "SUB-XXXXXXX",
  "imei": "300215010622150",
  "customer_name": "ООО Рога и Копыта",
  "agreement": "076/18",
  "code_1c": "1C00123",
  "status": 1
}
```

### Option 3: Scheduled Sync
Run query and insert into 1C database table (use export scripts for this)

---

## Quick Reference Commands

```bash
# Find all active services
sqlplus -s billing7/billing@bm7 << EOF
SELECT COUNT(*) FROM SERVICES WHERE TYPE_ID=9002 AND STATUS=1;
EXIT
EOF

# Check today's activity
sqlplus -s billing7/billing@bm7 << EOF
SELECT COUNT(*) FROM SERVICES 
WHERE TYPE_ID=9002 AND TRUNC(CREATE_DATE)=TRUNC(SYSDATE);
EXIT
EOF

# Run finance alerts
sqlplus billing7/billing@bm7 @oracle/queries/finance_alerts.sql | tee alerts.txt

# Find specific IMEI
sqlplus -s billing7/billing@bm7 << EOF
SELECT SERVICE_ID, LOGIN, STATUS FROM SERVICES 
WHERE TYPE_ID=9002 AND PASSWD='300215010622150';
EXIT
EOF
```

---

## Directory Structure

```
oracle/
├── queries/              ⭐ PRODUCTION USE
│   ├── find_iridium_accounts.sql    # Lookup services
│   ├── finance_alerts.sql           # Daily monitoring
│   └── README.md                    # This guide
│
├── export/               🧪 TESTING ONLY
│   ├── export_billing_integration.sql
│   ├── export_view_results.sql
│   └── README.md
│
├── views/                📊 Database objects
│   ├── 01_v_spnet_overage_analysis.sql
│   ├── 02_v_consolidated_overage_report.sql
│   ├── 03_v_iridium_services_info.sql
│   └── 04_v_consolidated_report_with_billing.sql
│
├── functions/            🔧 Database functions
│   └── calculate_overage.sql
│
└── tables/               💾 Database tables
    ├── 01_spnet_traffic.sql
    ├── 02_steccom_expenses.sql
    ├── 03_tariff_plans.sql
    └── 04_load_logs.sql
```

---

## Support Contacts

- **Database Issues:** DBA team
- **Finance Alerts:** Finance department
- **Service Issues:** Operations team
- **Billing Discrepancies:** Accounting department

## Related Documentation

- `oracle/queries/README.md` - Detailed query documentation
- `oracle/export/README.md` - Export scripts (testing only)
- `BILLING_EXPORT_GUIDE.md` - Export guide (testing only)

