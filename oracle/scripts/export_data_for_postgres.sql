-- ============================================================================
-- Export Oracle data for PostgreSQL testing
-- Generates INSERT statements compatible with PostgreSQL
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

-- Создаем директорию для экспорта
SPOOL export_spnet_traffic.sql

PROMPT -- ============================================================================
PROMPT -- SPNET_TRAFFIC Data Export
PROMPT -- Generated from Oracle for PostgreSQL
PROMPT -- ============================================================================
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
FROM spnet_traffic
ORDER BY bill_month, imei;

PROMPT
PROMPT -- Export complete
PROMPT

SPOOL OFF

-- Экспорт STECCOM_EXPENSES
SPOOL export_steccom_expenses.sql

PROMPT -- ============================================================================
PROMPT -- STECCOM_EXPENSES Data Export
PROMPT -- Generated from Oracle for PostgreSQL
PROMPT -- ============================================================================
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
FROM steccom_expenses
ORDER BY billing_period, imei;

PROMPT
PROMPT -- Export complete
PROMPT

SPOOL OFF

-- Экспорт LOAD_LOGS
SPOOL export_load_logs.sql

PROMPT -- ============================================================================
PROMPT -- LOAD_LOGS Data Export
PROMPT -- Generated from Oracle for PostgreSQL
PROMPT -- ============================================================================
PROMPT

SELECT 
    'INSERT INTO load_logs (table_name, load_date, records_loaded, status, error_message) VALUES (' ||
    '''' || table_name || ''', ' ||
    'TIMESTAMP ''' || TO_CHAR(load_date, 'YYYY-MM-DD HH24:MI:SS') || ''', ' ||
    NVL(TO_CHAR(records_loaded), 'NULL') || ', ' ||
    '''' || status || ''', ' ||
    CASE WHEN error_message IS NULL THEN 'NULL' ELSE '''' || REPLACE(error_message, '''', '''''') || '''' END ||
    ');'
FROM load_logs
ORDER BY load_date;

PROMPT
PROMPT -- Export complete
PROMPT

SPOOL OFF

-- Сводная информация
SPOOL export_summary.txt

PROMPT ============================================================================
PROMPT Data Export Summary
PROMPT ============================================================================
PROMPT

SELECT 'SPNET_TRAFFIC: ' || COUNT(*) || ' records' FROM spnet_traffic;
SELECT 'STECCOM_EXPENSES: ' || COUNT(*) || ' records' FROM steccom_expenses;
SELECT 'LOAD_LOGS: ' || COUNT(*) || ' records' FROM load_logs;

PROMPT
PROMPT Files generated:
PROMPT   - export_spnet_traffic.sql
PROMPT   - export_steccom_expenses.sql
PROMPT   - export_load_logs.sql
PROMPT
PROMPT Usage in PostgreSQL:
PROMPT   psql -U postgres -d billing -f export_spnet_traffic.sql
PROMPT   psql -U postgres -d billing -f export_steccom_expenses.sql
PROMPT   psql -U postgres -d billing -f export_load_logs.sql
PROMPT
PROMPT ============================================================================

SPOOL OFF

SET FEEDBACK ON
SET HEADING ON
SET ECHO ON

PROMPT
PROMPT Export scripts generated successfully!
PROMPT Run them in Oracle directory to create PostgreSQL-compatible INSERT files.
PROMPT


