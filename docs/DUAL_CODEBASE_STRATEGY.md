# Dual Codebase Strategy: Oracle Production + PostgreSQL Testing

## Overview

This project maintains **two parallel implementations**:

1. **Oracle Production** - Real production environment
2. **PostgreSQL Testing** - Development/testing environment

## Architecture Comparison

### Oracle Production (billing7@bm7)

```
┌─────────────────────────────────────────────────┐
│  PRODUCTION TABLES (Live Data)                  │
├─────────────────────────────────────────────────┤
│  • SERVICES          - Service records          │
│  • CUSTOMERS         - Customer data            │
│  • ACCOUNTS          - Account/agreement info   │
│  • BM_CUSTOMER_CONTACT - Customer details       │
│  • BM_CONTACT_DICT   - Contact dictionary       │
│  • OUTER_IDS         - External IDs (CODE_1C)   │
│  • SPNET_TRAFFIC     - Usage data (CSV loaded)  │
│  • STECCOM_EXPENSES  - Billing data (CSV)       │
│  • TARIFF_PLANS      - Tariff configuration     │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│  VIEWS (Complex JOINs)                          │
├─────────────────────────────────────────────────┤
│  • V_IRIDIUM_SERVICES_INFO                      │
│    ↳ Joins SERVICES + CUSTOMERS + ACCOUNTS +    │
│      BM_CUSTOMER_CONTACT + OUTER_IDS            │
│                                                  │
│  • V_SPNET_OVERAGE_ANALYSIS                     │
│  • V_CONSOLIDATED_REPORT_WITH_BILLING           │
└─────────────────────────────────────────────────┘
```

### PostgreSQL Testing (localhost)

```
┌─────────────────────────────────────────────────┐
│  TEST TABLES (Imported Data)                    │
├─────────────────────────────────────────────────┤
│  • IRIDIUM_SERVICES_INFO  ← Oracle dump (mock)  │
│    (Pre-joined data from Oracle production)     │
│                                                  │
│  • SPNET_TRAFFIC     - CSV test data            │
│  • STECCOM_EXPENSES  - CSV test data            │
│  • TARIFF_PLANS      - Static config            │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│  VIEWS (Simple wrappers)                        │
├─────────────────────────────────────────────────┤
│  • V_IRIDIUM_SERVICES_INFO                      │
│    ↳ Simple SELECT from IRIDIUM_SERVICES_INFO   │
│                                                  │
│  • V_SPNET_OVERAGE_ANALYSIS                     │
│  • V_CONSOLIDATED_REPORT_WITH_BILLING           │
└─────────────────────────────────────────────────┘
```

## Key Differences

### V_IRIDIUM_SERVICES_INFO Implementation

#### Oracle (Production) - Complex
```sql
CREATE OR REPLACE VIEW V_IRIDIUM_SERVICES_INFO AS
SELECT 
    s.SERVICE_ID,
    s.LOGIN AS CONTRACT_ID,
    s.PASSWD AS IMEI,
    -- ... complex joins ...
    NVL(
        MAX(CASE WHEN cd.MNEMONIC = 'b_name' THEN cc.VALUE END),
        TRIM(...)
    ) AS CUSTOMER_NAME,
    oi.EXT_ID AS CODE_1C
FROM SERVICES s
JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
JOIN CUSTOMERS c ON s.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN BM_CUSTOMER_CONTACT cc ON c.CUSTOMER_ID = cc.CUSTOMER_ID
LEFT JOIN BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID
LEFT JOIN OUTER_IDS oi ON c.CUSTOMER_ID = oi.ID
WHERE s.TYPE_ID = 9002
GROUP BY ...;
```

#### PostgreSQL (Testing) - Simple Wrapper
```sql
CREATE OR REPLACE VIEW V_IRIDIUM_SERVICES_INFO AS
SELECT 
    service_id,
    contract_id,
    imei,
    customer_name,
    agreement_number,
    order_number,
    code_1c,
    status,
    -- ... all columns ...
FROM IRIDIUM_SERVICES_INFO;  -- Pre-joined data from Oracle dump
```

## Directory Structure

```
ai_report/
├── oracle/                      # PRODUCTION CODE
│   ├── tables/                  # Production table definitions
│   ├── views/                   # Production views (complex JOINs)
│   ├── functions/               # Production functions
│   ├── queries/                 # ⭐ Daily operations queries
│   ├── export/                  # Data export for testing
│   └── test/                    # Oracle dump for PostgreSQL
│       └── V_IRIDIUM_SERVICES_INFO.txt
│
├── postgresql/                  # TEST CODE
│   ├── tables/                  # Test table definitions
│   │   ├── 01_spnet_traffic.sql
│   │   ├── 02_steccom_expenses.sql
│   │   ├── 03_tariff_plans.sql
│   │   └── 05_iridium_services_info.sql  ← Import table
│   ├── views/                   # Test views (simple wrappers)
│   │   └── 03_v_iridium_services_info.sql  ← Wrapper
│   ├── functions/               # Test functions (same logic)
│   └── data/                    # Import scripts
│       └── import_oracle_dump.sh
│
├── PRODUCTION_OPERATIONS.md     # Production guide
├── SETUP_WITH_ORACLE_DATA.md   # Testing setup guide
└── DUAL_CODEBASE_STRATEGY.md   # This file
```

## Data Flow

### Production (Oracle)

```
CSV Files → Load → SPNET_TRAFFIC, STECCOM_EXPENSES
                   ↓
Billing Tables → V_IRIDIUM_SERVICES_INFO (complex view)
                   ↓
              Daily Queries
              Finance Alerts
```

### Testing (PostgreSQL)

```
Oracle Dump → Import → IRIDIUM_SERVICES_INFO (table)
                        ↓
CSV Files → Load → SPNET_TRAFFIC, STECCOM_EXPENSES
                        ↓
              V_IRIDIUM_SERVICES_INFO (wrapper view)
                        ↓
              Test Queries
              Validate Functions
              Compare Results
```

## Maintenance Strategy

### When Updating Views

**1. Update Oracle Production First:**
```bash
cd oracle/views
# Edit view definition
sqlplus billing7/billing@bm7 @03_v_iridium_services_info.sql
```

**2. Export Fresh Dump for Testing:**
```bash
cd oracle/export
sqlplus billing7/billing@bm7 << EOF
SPOOL ../test/V_IRIDIUM_SERVICES_INFO.txt
SELECT * FROM V_IRIDIUM_SERVICES_INFO;
SPOOL OFF
EOF
```

**3. Update PostgreSQL Test:**
```bash
cd postgresql/data
./import_oracle_dump.sh
```

**4. Verify Both Match:**
```bash
cd oracle/export
sqlplus billing7/billing@bm7 @export_view_results.sql

cd ../../postgresql/export
psql -U postgres -d billing -f export_view_results.sql

# Compare
diff oracle_v_iridium_services_info.txt postgres_v_iridium_services_info.txt
```

### When Adding New Queries

**Production:**
```bash
# Add to oracle/queries/
vim oracle/queries/new_query.sql
sqlplus billing7/billing@bm7 @oracle/queries/new_query.sql
```

**Testing:**
```bash
# Port to PostgreSQL syntax if needed
vim postgresql/queries/new_query.sql
psql -U postgres -d billing -f postgresql/queries/new_query.sql
```

## Code Compatibility

### What's SHARED (Same Logic)

✅ **Functions**
- `calculate_overage()` - Same algorithm, just syntax differences

✅ **Business Logic**
- Overage calculations
- Tariff tier pricing
- Report generation

✅ **Test Data**
- TARIFF_PLANS (same data both sides)
- CSV files (SPNET_TRAFFIC, STECCOM_EXPENSES)

### What's DIFFERENT (Platform-Specific)

❌ **Data Source**
- Oracle: Real billing tables with complex JOINs
- PostgreSQL: Pre-joined dump data

❌ **V_IRIDIUM_SERVICES_INFO Implementation**
- Oracle: Complex multi-table JOIN
- PostgreSQL: Simple wrapper over imported table

❌ **Syntax**
- Oracle: PL/SQL, NUMBER, VARCHAR2, SYSDATE
- PostgreSQL: PL/pgSQL, NUMERIC, VARCHAR, CURRENT_TIMESTAMP

## Testing Workflow

### 1. Develop on PostgreSQL
```bash
# Quick iteration on local PostgreSQL
psql -U postgres -d billing -f views/new_view.sql
psql -U postgres -d billing -c "SELECT * FROM new_view LIMIT 5;"
```

### 2. Port to Oracle
```bash
# Convert PostgreSQL → Oracle syntax
# Test on production (or Oracle dev if available)
sqlplus billing7/billing@bm7 @views/new_view.sql
```

### 3. Validate Results Match
```bash
# Export from both and compare
diff oracle_results.txt postgres_results.txt
```

## Deployment Checklist

### Production Deployment (Oracle)

- [ ] Test query on Oracle dev/test instance
- [ ] Review performance (EXPLAIN PLAN)
- [ ] Check for locking issues
- [ ] Deploy during maintenance window
- [ ] Verify results
- [ ] Update documentation

### Test Environment Update (PostgreSQL)

- [ ] Export fresh dump from Oracle
- [ ] Import to PostgreSQL
- [ ] Recreate views
- [ ] Run test suite
- [ ] Update documentation

## Documentation Rules

### Oracle Production Docs

Location: `oracle/queries/README.md`, `PRODUCTION_OPERATIONS.md`

Focus:
- Daily operations
- Finance alerts
- Production queries
- Performance considerations

### PostgreSQL Test Docs

Location: `postgresql/SETUP_WITH_ORACLE_DATA.md`

Focus:
- Setup instructions
- Test data import
- Development workflow
- Comparison testing

## Version Control Strategy

### Branching

```
main
├── oracle/          # Production-ready code
└── postgresql/      # Test environment code
```

### Commit Messages

```bash
# Production changes
git commit -m "oracle: Add finance alert for duplicate IMEIs"

# Test changes
git commit -m "postgres: Update import script for new dump format"

# Both
git commit -m "both: Add calculate_overage function"
```

## Benefits of Dual Approach

✅ **Advantages:**

1. **Safe Testing** - Test on PostgreSQL without touching production
2. **Fast Iteration** - Local dev/test cycles
3. **Cost Effective** - No Oracle license needed for testing
4. **Compatibility Check** - Validate cross-platform logic
5. **Backup Data** - PostgreSQL has copy of service data

⚠️ **Considerations:**

1. **Sync Required** - Must keep dumps updated
2. **Two Codebases** - Double maintenance
3. **Syntax Differences** - Oracle ↔ PostgreSQL conversions
4. **Mock Limitations** - PostgreSQL uses pre-joined data, not live

## Summary

| Aspect | Oracle Production | PostgreSQL Testing |
|--------|-------------------|-------------------|
| **Purpose** | Live operations | Development/testing |
| **Data** | Real billing tables | Imported dumps + CSV |
| **Views** | Complex JOINs | Simple wrappers |
| **Users** | Finance, operations | Developers |
| **Updates** | Careful, scheduled | Frequent, experimental |
| **Performance** | Critical | Not critical |
| **Cost** | Oracle license | Free |

---

**Key Principle:** Production code is the source of truth. PostgreSQL mirrors functionality for safe testing and development.

