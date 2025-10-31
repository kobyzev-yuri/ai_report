# Quick Reference - Iridium Operations

## 🚀 Most Used Commands

### Daily Finance Check
```bash
sqlplus billing7/billing@bm7 @oracle/queries/finance_alerts.sql
```

### Find Service by IMEI
```bash
sqlplus billing7/billing@bm7 << EOF
SELECT SERVICE_ID, LOGIN, STATUS, CREATE_DATE 
FROM SERVICES 
WHERE TYPE_ID = 9002 AND PASSWD = 'YOUR_IMEI_HERE';
EXIT
EOF
```

### Find All Services for Customer
```bash
sqlplus billing7/billing@bm7 << EOF
SELECT * FROM V_IRIDIUM_SERVICES_INFO 
WHERE CUSTOMER_NAME LIKE '%CustomerName%';
EXIT
EOF
```

---

## 📊 Key Views

| View | Purpose | Use For |
|------|---------|---------|
| `V_IRIDIUM_SERVICES_INFO` | Service + customer data | Lookup services, customer info |
| `V_SPNET_OVERAGE_ANALYSIS` | Usage analysis by IMEI | Check usage, overage |
| `V_CONSOLIDATED_REPORT_WITH_BILLING` | Complete report | Billing, finance reports |

---

## ⚠️ Finance Alert Types

1. **Missing traffic** → Check device or data load
2. **Billing mismatch** → Review pricing
3. **High charges** → Notify customer  
4. **Missing CODE_1C** → Update OUTER_IDS
5. **Upgrade needed** → Contact customer
6. **Duplicate IMEI** → Deactivate duplicate
7. **No traffic 60d** → Suspend service?

---

## 🔍 Quick Queries

```sql
-- Active Iridium services count
SELECT COUNT(*) FROM SERVICES 
WHERE TYPE_ID = 9002 AND STATUS = 1 
AND LOGIN NOT LIKE '%-clone-%';

-- This month's high charges
SELECT IMEI, CUSTOMER_NAME, CALCULATED_OVERAGE 
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE BILL_MONTH = TO_NUMBER(TO_CHAR(SYSDATE, 'YYYYMM'))
AND CALCULATED_OVERAGE > 50;

-- Services created today
SELECT SERVICE_ID, LOGIN, PASSWD 
FROM SERVICES 
WHERE TYPE_ID = 9002 
AND TRUNC(CREATE_DATE) = TRUNC(SYSDATE);
```

---

## 📁 File Locations

```
Production:  oracle/queries/
Testing:     oracle/export/
Views:       oracle/views/
Functions:   oracle/functions/
```

---

## 🆘 Common Issues

| Problem | Solution |
|---------|----------|
| No SPNET data | Check Alert #1 |
| High bill complaint | Check usage in V_CONSOLIDATED_REPORT_WITH_BILLING |
| Missing CODE_1C | Run Alert #4, update OUTER_IDS |
| Service not found | Check if LOGIN has `-clone-` suffix |

---

## 📖 Full Docs

- `PRODUCTION_OPERATIONS.md` - Complete guide
- `oracle/queries/README.md` - All queries
- `oracle/export/README.md` - Testing only

