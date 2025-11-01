# PostgreSQL Export Scripts

Export scripts for PostgreSQL database - for comparison with Oracle and testing.

## Scripts

### 1. export_billing_integration.sql
**Purpose:** Export customer billing data (PostgreSQL version)

Mirrors the Oracle export for comparison and testing.

**Usage:**
```bash
cd /mnt/ai/cnn/ai_report/postgresql/export
psql -U postgres -d billing -f export_billing_integration.sql
```

**Output:** Same format as Oracle export for comparison

### 2. export_view_results.sql
**Purpose:** Export view results for comparison with Oracle

**Usage:**
```bash
cd /mnt/ai/cnn/ai_report/postgresql/export
psql -U postgres -d billing -f export_view_results.sql
```

**Output:** PostgreSQL view results for comparison testing

---

## Comparison Workflow

After setting up both databases with same CSV data:

```bash
# 1. Export from Oracle
cd ../../oracle/export
sqlplus billing7/billing@bm7 @export_view_results.sql

# 2. Export from PostgreSQL
cd ../../postgresql/export
psql -U postgres -d billing -f export_view_results.sql

# 3. Compare results
diff oracle_function_tests.txt postgres_function_tests.txt
diff oracle_v_spnet_overage_analysis.txt postgres_v_spnet_overage_analysis.txt
```

---

## Notes

- **Test Database:** These scripts query PostgreSQL test database
- **Same CSV Data:** Load same CSV files to both Oracle and PostgreSQL
- **Views Must Exist:** Create views before running exports
- **Comparison:** Results should match Oracle exactly







