# Quick Start Guide

## Choose Your Path

### 🔴 Production Operations (Oracle)

**I need to:** Work with real billing data, run finance alerts, lookup services

**Go to:**
```bash
cd oracle/queries/

# Daily finance alerts
sqlplus billing7/billing@bm7 @finance_alerts.sql

# Find services
sqlplus billing7/billing@bm7 @find_iridium_accounts.sql
```

**Docs:** `PRODUCTION_OPERATIONS.md`, `QUICK_REFERENCE.md`

---

### 🟢 Testing/Development (PostgreSQL)

**I need to:** Test changes, validate functions, develop new features

**Setup:**
```bash
cd postgresql/

# 1. Create database & tables
psql -U postgres -c "CREATE DATABASE billing;"
for f in tables/*.sql; do psql -U postgres -d billing -f "$f"; done

# 2. Load tariff plans
psql -U postgres -d billing -f data/tariff_plans_data.sql

# 3. Import Oracle dump (real service data!)
cd data && ./import_oracle_dump.sh

# 4. Create functions & views
cd ..
for f in functions/*.sql; do psql -U postgres -d billing -f "$f"; done
for f in views/*.sql; do psql -U postgres -d billing -f "$f"; done
```

**Docs:** `postgresql/SETUP_WITH_ORACLE_DATA.md`

---

## Key Differences

| Aspect | Oracle (Production) | PostgreSQL (Testing) |
|--------|-------------------|---------------------|
| **Data** | Real billing tables | Imported dump + CSV |
| **Views** | Complex JOINs | Simple wrappers |
| **Use** | Daily operations | Development/testing |
| **Risk** | High (production) | Low (local) |

---

## Common Tasks

### Find Service by IMEI (Production)
```bash
sqlplus billing7/billing@bm7 << EOF
SELECT SERVICE_ID, LOGIN, STATUS 
FROM SERVICES 
WHERE TYPE_ID = 9002 AND PASSWD = 'YOUR_IMEI';
EXIT
EOF
```

### Run Finance Alerts (Production)
```bash
sqlplus billing7/billing@bm7 @oracle/queries/finance_alerts.sql > alerts_$(date +%Y%m%d).txt
```

### Test Function (PostgreSQL)
```bash
psql -U postgres -d billing << EOF
SELECT calculate_overage('SBD Tiered 1250 10K', 35000);
EOF
```

### Compare Results (Both)
```bash
# Export from Oracle
cd oracle/export
sqlplus billing7/billing@bm7 @export_view_results.sql

# Export from PostgreSQL
cd ../../postgresql/export
psql -U postgres -d billing -f export_view_results.sql

# Compare
diff oracle_function_tests.txt postgres_function_tests.txt
```

---

## Project Documentation

| Document | Purpose |
|----------|---------|
| `PRODUCTION_OPERATIONS.md` | Oracle production guide |
| `QUICK_REFERENCE.md` | Cheat sheet |
| `postgresql/SETUP_WITH_ORACLE_DATA.md` | PostgreSQL setup |
| `DUAL_CODEBASE_STRATEGY.md` | Architecture & maintenance |
| `BILLING_EXPORT_GUIDE.md` | Export guide (testing) |

---

## Need Help?

**Production issues:** See `oracle/queries/README.md`

**Testing setup:** See `postgresql/SETUP_WITH_ORACLE_DATA.md`

**Architecture:** See `DUAL_CODEBASE_STRATEGY.md`

