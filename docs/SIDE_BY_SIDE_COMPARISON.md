# Side-by-Side: Oracle Production vs PostgreSQL Testing

## V_IRIDIUM_SERVICES_INFO - Implementation Comparison

### Oracle Production (Real Billing Integration)

**File:** `oracle/views/03_v_iridium_services_info.sql`

```sql
CREATE OR REPLACE VIEW V_IRIDIUM_SERVICES_INFO AS
SELECT 
    s.SERVICE_ID,
    s.LOGIN AS CONTRACT_ID,
    s.PASSWD AS IMEI,
    s.TARIFF_ID,
    a.DESCRIPTION AS AGREEMENT_NUMBER,
    s.BLANK AS ORDER_NUMBER,
    s.STATUS,
    s.ACTUAL_STATUS,
    c.CUSTOMER_ID,
    -- Organization name (for business entities)
    MAX(CASE WHEN cd.MNEMONIC = 'b_name' THEN cc.VALUE END) AS ORGANIZATION_NAME,
    -- Person name (for individuals)
    TRIM(
        NVL(MAX(CASE WHEN cd.MNEMONIC = 'last_name' THEN cc.VALUE END), '') || ' ' ||
        NVL(MAX(CASE WHEN cd.MNEMONIC = 'first_name' THEN cc.VALUE END), '') || ' ' ||
        NVL(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' THEN cc.VALUE END), '')
    ) AS PERSON_NAME,
    -- Universal field (org name OR person name)
    NVL(
        MAX(CASE WHEN cd.MNEMONIC = 'b_name' THEN cc.VALUE END),
        TRIM(
            NVL(MAX(CASE WHEN cd.MNEMONIC = 'last_name' THEN cc.VALUE END), '') || ' ' ||
            NVL(MAX(CASE WHEN cd.MNEMONIC = 'first_name' THEN cc.VALUE END), '') || ' ' ||
            NVL(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' THEN cc.VALUE END), '')
        )
    ) AS CUSTOMER_NAME,
    s.CREATE_DATE,
    s.START_DATE,
    s.STOP_DATE,
    a.ACCOUNT_ID,
    oi.EXT_ID AS CODE_1C
FROM SERVICES s
JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
JOIN CUSTOMERS c ON s.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN BM_CUSTOMER_CONTACT cc ON c.CUSTOMER_ID = cc.CUSTOMER_ID
LEFT JOIN BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID
    AND cd.MNEMONIC IN ('b_name', 'first_name', 'last_name', 'middle_name')
LEFT JOIN OUTER_IDS oi ON c.CUSTOMER_ID = oi.ID 
    AND oi.TBL = 'CUSTOMERS'
WHERE s.TYPE_ID = 9002  -- Only Iridium SBD services
GROUP BY 
    s.SERVICE_ID,
    s.LOGIN,
    s.PASSWD,
    s.TARIFF_ID,
    a.DESCRIPTION,
    s.BLANK,
    s.STATUS,
    s.ACTUAL_STATUS,
    c.CUSTOMER_ID,
    s.CREATE_DATE,
    s.START_DATE,
    s.STOP_DATE,
    a.ACCOUNT_ID,
    oi.EXT_ID;
```

**Complexity:** 5-table JOIN with conditional aggregation

**Data Source:** Live billing tables

**Use:** Production queries

---

### PostgreSQL Testing (Imported Mock Data)

**File:** `postgresql/views/03_v_iridium_services_info.sql`

```sql
DROP VIEW IF EXISTS V_IRIDIUM_SERVICES_INFO CASCADE;

CREATE OR REPLACE VIEW V_IRIDIUM_SERVICES_INFO AS
SELECT 
    service_id,
    contract_id,
    imei,
    tariff_id,
    agreement_number,
    order_number,
    status,
    actual_status,
    customer_id,
    organization_name,
    person_name,
    customer_name,
    create_date,
    start_date,
    stop_date,
    account_id,
    code_1c
FROM iridium_services_info;  -- ← Pre-joined data from Oracle dump

COMMENT ON VIEW v_iridium_services_info IS 
'View wrapper for IRIDIUM_SERVICES_INFO table imported from Oracle.
For production, this would query SERVICES, CUSTOMERS, ACCOUNTS tables directly.';
```

**Complexity:** Simple SELECT (wrapper)

**Data Source:** Imported table (Oracle dump)

**Use:** Testing, development

---

## Supporting Table Structure

### Oracle Production - NO TABLE NEEDED

Data comes from real billing tables:
- `SERVICES`
- `CUSTOMERS`
- `ACCOUNTS`
- `BM_CUSTOMER_CONTACT`
- `BM_CONTACT_DICT`
- `OUTER_IDS`

---

### PostgreSQL Testing - IMPORT TABLE REQUIRED

**File:** `postgresql/tables/05_iridium_services_info.sql`

```sql
CREATE TABLE IRIDIUM_SERVICES_INFO (
    SERVICE_ID INTEGER,
    CONTRACT_ID VARCHAR(50),
    IMEI VARCHAR(50),
    TARIFF_ID INTEGER,
    AGREEMENT_NUMBER VARCHAR(200),
    ORDER_NUMBER VARCHAR(100),
    STATUS INTEGER,
    ACTUAL_STATUS INTEGER,
    CUSTOMER_ID INTEGER,
    ORGANIZATION_NAME VARCHAR(500),
    PERSON_NAME VARCHAR(500),
    CUSTOMER_NAME VARCHAR(500),
    CREATE_DATE TIMESTAMP,
    START_DATE TIMESTAMP,
    STOP_DATE TIMESTAMP,
    ACCOUNT_ID INTEGER,
    CODE_1C VARCHAR(100)
);

-- Indexes for performance
CREATE INDEX idx_iridium_contract_id ON IRIDIUM_SERVICES_INFO(CONTRACT_ID);
CREATE INDEX idx_iridium_imei ON IRIDIUM_SERVICES_INFO(IMEI);
CREATE INDEX idx_iridium_status ON IRIDIUM_SERVICES_INFO(STATUS);
```

**Populated by:** `oracle/test/V_IRIDIUM_SERVICES_INFO.txt` (~30,000 records)

**Import script:** `oracle/test/import_iridium.py`

---

## Query Comparison

### Find Active Services

#### Oracle Production
```sql
SELECT 
    s.SERVICE_ID,
    s.LOGIN,
    s.PASSWD,
    c.CUSTOMER_ID
FROM SERVICES s
JOIN CUSTOMERS c ON s.CUSTOMER_ID = c.CUSTOMER_ID
WHERE s.TYPE_ID = 9002
  AND s.STATUS = 1
  AND s.LOGIN NOT LIKE '%-clone-%';
```

#### PostgreSQL Testing
```sql
SELECT 
    service_id,
    contract_id AS login,
    imei AS passwd,
    customer_id
FROM iridium_services_info
WHERE status = 1
  AND contract_id NOT LIKE '%-clone-%';
```

**OR** (using view):
```sql
SELECT 
    service_id,
    contract_id,
    imei,
    customer_id
FROM v_iridium_services_info
WHERE status = 1
  AND contract_id NOT LIKE '%-clone-%';
```

---

## Data Refresh Workflow

### Step 1: Export from Oracle Production

```bash
cd oracle/export

sqlplus billing7/billing@bm7 << EOF
SET PAGESIZE 0
SET LINESIZE 500
SET FEEDBACK OFF
SET HEADING ON
SPOOL ../test/V_IRIDIUM_SERVICES_INFO.txt

SELECT * FROM V_IRIDIUM_SERVICES_INFO;

SPOOL OFF
EXIT
EOF
```

### Step 2: Import to PostgreSQL Testing

```py310) cnn@cnn-G5-5590:~/ai_report/oracle/test$ PGPASSWORD=1234 psql -h 127.0.0.1 -U postgres -d billing -c "TRUNCATE iridium_services_info;"
PGPASSWORD=1234 psql -h 127.0.0.1 -U postgres -d billing -c "INSERT INTO iridium_services_info SELECT * FROM iridium_services_info_fixed;"
TRUNCATE TABLE
INSERT 0 30868
(py310) cnn@cnn-G5-5590:~/ai_report/oracle/test$ PGPASSWORD=1234 psql -h 127.0.0.1 -U postgres -d billing -c "SELECT COUNT(*) FROM iridium_services_info;"
 count 
-------
 30868
(1 row)

bash
cd ../../postgresql/data
./u
```

### Step 3: Verify

```bash
# Oracle count
sqlplus -s billing7/billing@bm7 << EOF
SELECT COUNT(*) FROM V_IRIDIUM_SERVICES_INFO WHERE STATUS = 1;
EXIT
EOF

# PostgreSQL count
psql -U postgres -d billing -t -c \
  "SELECT COUNT(*) FROM v_iridium_services_info WHERE status = 1;"
```

Counts should match!

---

## Function Implementation - Same Logic, Different Syntax

### calculate_overage() Function

#### Oracle (PL/SQL)
```sql
CREATE OR REPLACE FUNCTION CALCULATE_OVERAGE(
    p_plan_name VARCHAR2,
    p_usage_bytes NUMBER
) RETURN NUMBER
IS
    v_usage_kb NUMBER;
    -- ... variables ...
BEGIN
    v_usage_kb := p_usage_bytes / 1000;
    -- ... calculation logic ...
    RETURN ROUND(v_total_charge, 2);
END CALCULATE_OVERAGE;
/
```

#### PostgreSQL (PL/pgSQL)
```sql
CREATE OR REPLACE FUNCTION calculate_overage(
    p_plan_name VARCHAR,
    p_usage_bytes NUMERIC
) RETURNS NUMERIC
AS $$
DECLARE
    v_usage_kb NUMERIC;
    -- ... variables ...
BEGIN
    v_usage_kb := p_usage_bytes / 1000;
    -- ... calculation logic (same) ...
    RETURN ROUND(v_total_charge, 2);
END;
$$ LANGUAGE plpgsql;
```

**Key differences:**
- Oracle: `VARCHAR2`, `NUMBER`, `IS...END`
- PostgreSQL: `VARCHAR`, `NUMERIC`, `AS $$ ... $$ LANGUAGE plpgsql`

**Business logic:** Identical!

---

## Testing Strategy

### 1. Test Function on Both Platforms

```sql
-- Oracle
SELECT CALCULATE_OVERAGE('SBD Tiered 1250 10K', 35000) FROM DUAL;
-- Expected: 6.50

-- PostgreSQL
SELECT calculate_overage('SBD Tiered 1250 10K', 35000);
-- Expected: 6.50
```

### 2. Compare View Results

```bash
# Export from both
cd oracle/export
sqlplus billing7/billing@bm7 @export_view_results.sql

cd ../../postgresql/export
psql -U postgres -d billing -f export_view_results.sql

# Compare
diff oracle_function_tests.txt postgres_function_tests.txt
```

### 3. Validate Consolidated Report

```sql
-- Both should return similar structure
SELECT 
    contract_id,
    customer_name,
    bill_month,
    calculated_overage
FROM v_consolidated_report_with_billing
WHERE bill_month >= '2024-01-01'
LIMIT 10;
```

---

## Maintenance Checklist

### When Updating Views in Production (Oracle)

- [ ] Update `oracle/views/03_v_iridium_services_info.sql`
- [ ] Deploy to Oracle production
- [ ] Test queries
- [ ] Export fresh dump for PostgreSQL
- [ ] Update PostgreSQL import
- [ ] Verify both environments match

### When Adding New Queries

- [ ] Write for Oracle first (production syntax)
- [ ] Test on Oracle
- [ ] Port to PostgreSQL syntax if needed
- [ ] Test on PostgreSQL
- [ ] Document differences
- [ ] Update this comparison doc

---

## Key Takeaways

| Aspect | Oracle | PostgreSQL |
|--------|--------|------------|
| **Data** | Real-time from billing | Snapshot from dump |
| **Complexity** | Complex JOINs | Pre-joined data |
| **Speed** | Varies (production load) | Fast (local, indexed) |
| **Risk** | High (production) | Low (test) |
| **Purpose** | Operations, reporting | Development, testing |
| **Update** | Real-time | Manual refresh |

**Golden Rule:** Oracle is source of truth. PostgreSQL mirrors for safe development.

