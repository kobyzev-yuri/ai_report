-- ============================================================================
-- Export SAMPLE data from Oracle for PostgreSQL testing
-- Generates small dataset for testing purposes
-- ============================================================================

SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
SET PAGESIZE 0
SET LINESIZE 32767
SET LONG 2000000000
SET LONGCHUNKSIZE 32767
SET TRIMSPOOL ON
SET VERIFY OFF

-- Export sample SPNET_TRAFFIC (limit to 100 records)
SPOOL sample_spnet_traffic.sql

PROMPT -- ============================================================================
PROMPT -- SPNET_TRAFFIC Sample Data (100 records)
PROMPT -- Generated from Oracle for PostgreSQL testing
PROMPT -- ============================================================================
PROMPT
PROMPT BEGIN;
PROMPT

SELECT 
    'INSERT INTO spnet_traffic (imei, contract_id, bill_month, plan_name, usage_type, usage_bytes, total_amount) VALUES (' ||
    '''' || imei || ''', ' ||
    '''' || contract_id || ''', ' ||
    'TO_DATE(''' || TO_CHAR(bill_month, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD''), ' ||
    '''' || REPLACE(plan_name, '''', '''''') || ''', ' ||
    '''' || REPLACE(usage_type, '''', '''''') || ''', ' ||
    usage_bytes || ', ' ||
    total_amount ||
    ');'
FROM (
    SELECT * FROM spnet_traffic
    WHERE ROWNUM <= 100
    ORDER BY bill_month DESC, imei
);

PROMPT
PROMPT COMMIT;
PROMPT
PROMPT -- Sample export complete: 100 records
PROMPT

SPOOL OFF

-- Export sample STECCOM_EXPENSES (limit to 50 records)
SPOOL sample_steccom_expenses.sql

PROMPT -- ============================================================================
PROMPT -- STECCOM_EXPENSES Sample Data (50 records)
PROMPT -- Generated from Oracle for PostgreSQL testing
PROMPT -- ============================================================================
PROMPT
PROMPT BEGIN;
PROMPT

SELECT 
    'INSERT INTO steccom_expenses (imei, phone_number, subscriber_name, billing_period, charge_amount, charge_description, charge_date) VALUES (' ||
    '''' || imei || ''', ' ||
    CASE WHEN phone_number IS NULL THEN 'NULL' ELSE '''' || phone_number || '''' END || ', ' ||
    CASE WHEN subscriber_name IS NULL THEN 'NULL' ELSE '''' || REPLACE(subscriber_name, '''', '''''') || '''' END || ', ' ||
    'TO_DATE(''' || TO_CHAR(billing_period, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD''), ' ||
    charge_amount || ', ' ||
    CASE WHEN charge_description IS NULL THEN 'NULL' ELSE '''' || REPLACE(charge_description, '''', '''''') || '''' END || ', ' ||
    CASE WHEN charge_date IS NULL THEN 'NULL' ELSE 'TO_DATE(''' || TO_CHAR(charge_date, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD'')' END ||
    ');'
FROM (
    SELECT * FROM steccom_expenses
    WHERE ROWNUM <= 50
    ORDER BY billing_period DESC, imei
);

PROMPT
PROMPT COMMIT;
PROMPT
PROMPT -- Sample export complete: 50 records
PROMPT

SPOOL OFF

-- Sample summary
SPOOL sample_summary.txt

PROMPT ============================================================================
PROMPT Sample Data Export Summary
PROMPT ============================================================================
PROMPT

SELECT 'Total SPNET_TRAFFIC records: ' || COUNT(*) FROM spnet_traffic;
SELECT 'Exported: 100 sample records' FROM DUAL;
PROMPT

SELECT 'Total STECCOM_EXPENSES records: ' || COUNT(*) FROM steccom_expenses;
SELECT 'Exported: 50 sample records' FROM DUAL;

PROMPT
PROMPT Files generated:
PROMPT   - sample_spnet_traffic.sql
PROMPT   - sample_steccom_expenses.sql
PROMPT
PROMPT Usage in PostgreSQL:
PROMPT   \i sample_spnet_traffic.sql
PROMPT   \i sample_steccom_expenses.sql
PROMPT
PROMPT ============================================================================

SPOOL OFF

SET FEEDBACK ON
SET HEADING ON
SET ECHO ON

PROMPT
PROMPT Sample export scripts generated!
PROMPT


