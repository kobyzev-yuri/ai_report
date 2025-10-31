-- ============================================================================
-- Export V_IRIDIUM_SERVICES_INFO as proper CSV for PostgreSQL
-- Run in Oracle: sqlplus billing7/billing@bm7 @export_for_postgresql_csv.sql
-- ============================================================================

SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
SET PAGESIZE 0
SET LINESIZE 32767
SET TRIMSPOOL ON
SET VERIFY OFF
SET TERMOUT ON

SPOOL ../test/V_IRIDIUM_SERVICES_INFO.csv

SELECT 
    SERVICE_ID || '|' ||
    NVL(CONTRACT_ID, '') || '|' ||
    NVL(IMEI, '') || '|' ||
    NVL(TO_CHAR(TARIFF_ID), '') || '|' ||
    NVL(REPLACE(AGREEMENT_NUMBER, '|', '-'), '') || '|' ||
    NVL(REPLACE(ORDER_NUMBER, '|', '-'), '') || '|' ||
    NVL(TO_CHAR(STATUS), '') || '|' ||
    NVL(TO_CHAR(ACTUAL_STATUS), '') || '|' ||
    NVL(TO_CHAR(CUSTOMER_ID), '') || '|' ||
    NVL(REPLACE(ORGANIZATION_NAME, '|', '-'), '') || '|' ||
    NVL(REPLACE(PERSON_NAME, '|', '-'), '') || '|' ||
    NVL(REPLACE(CUSTOMER_NAME, '|', '-'), '') || '|' ||
    NVL(TO_CHAR(CREATE_DATE, 'YYYY-MM-DD HH24:MI:SS'), '') || '|' ||
    NVL(TO_CHAR(START_DATE, 'YYYY-MM-DD HH24:MI:SS'), '') || '|' ||
    NVL(TO_CHAR(STOP_DATE, 'YYYY-MM-DD HH24:MI:SS'), '') || '|' ||
    NVL(TO_CHAR(ACCOUNT_ID), '') || '|' ||
    NVL(CODE_1C, '')
FROM V_IRIDIUM_SERVICES_INFO;

SPOOL OFF

SET FEEDBACK ON
SET HEADING ON
SET TERMOUT ON

PROMPT
PROMPT ============================================================================
PROMPT Export completed!
PROMPT ============================================================================
PROMPT File: oracle/test/V_IRIDIUM_SERVICES_INFO.csv
PROMPT
PROMPT To import in PostgreSQL:
PROMPT   cp oracle/test/V_IRIDIUM_SERVICES_INFO.csv /tmp/
PROMPT   psql -d billing -c "\copy iridium_services_info FROM '/tmp/V_IRIDIUM_SERVICES_INFO.csv' WITH (FORMAT csv, DELIMITER '|', NULL '');"
PROMPT
PROMPT ============================================================================

EXIT



