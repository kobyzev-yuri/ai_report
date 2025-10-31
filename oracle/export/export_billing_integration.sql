-- ============================================================================
-- Export Billing Integration Data for 1C
-- Minimal data extract: customer_name, agreement_no, blank, code_1c
-- ============================================================================

SET ECHO OFF
SET FEEDBACK OFF
SET HEADING ON
SET PAGESIZE 50000
SET LINESIZE 500
SET TRIMSPOOL ON
SET VERIFY OFF
SET COLSEP '|'

-- Export to CSV for 1C integration
SPOOL billing_integration.csv

PROMPT CONTRACT_ID|IMEI|CUSTOMER_NAME|AGREEMENT_NUMBER|ORDER_NUMBER|CODE_1C|STATUS

SELECT 
    CONTRACT_ID || '|' ||
    NVL(IMEI, '') || '|' ||
    NVL(CUSTOMER_NAME, '') || '|' ||
    NVL(AGREEMENT_NUMBER, '') || '|' ||
    NVL(ORDER_NUMBER, '') || '|' ||
    NVL(CODE_1C, '') || '|' ||
    NVL(TO_CHAR(STATUS), '')
FROM V_IRIDIUM_SERVICES_INFO
WHERE STATUS = 1  -- Только активные сервисы (исключает -clone и закрытые)
  AND CONTRACT_ID NOT LIKE '%-clone-%'  -- Исключаем клонированные/перенесенные сервисы
ORDER BY CODE_1C, CONTRACT_ID;

SPOOL OFF

-- Export to SQL INSERT format (alternative)
SPOOL billing_integration.sql

PROMPT -- ============================================================================
PROMPT -- Billing Integration Data - SQL Format
PROMPT -- ============================================================================
PROMPT
PROMPT -- DROP TABLE IF EXISTS billing_integration CASCADE;
PROMPT -- CREATE TABLE billing_integration (
PROMPT --     contract_id VARCHAR(50),
PROMPT --     imei VARCHAR(50),
PROMPT --     customer_name VARCHAR(500),
PROMPT --     agreement_number VARCHAR(200),
PROMPT --     order_number VARCHAR(100),
PROMPT --     code_1c VARCHAR(100),
PROMPT --     status VARCHAR(20)
PROMPT -- );
PROMPT

SELECT 
    'INSERT INTO billing_integration (contract_id, imei, customer_name, agreement_number, order_number, code_1c, status) VALUES (' ||
    '''' || CONTRACT_ID || ''', ' ||
    CASE WHEN IMEI IS NULL THEN 'NULL' ELSE '''' || IMEI || '''' END || ', ' ||
    CASE WHEN CUSTOMER_NAME IS NULL THEN 'NULL' ELSE '''' || REPLACE(CUSTOMER_NAME, '''', '''''') || '''' END || ', ' ||
    CASE WHEN AGREEMENT_NUMBER IS NULL THEN 'NULL' ELSE '''' || REPLACE(AGREEMENT_NUMBER, '''', '''''') || '''' END || ', ' ||
    CASE WHEN ORDER_NUMBER IS NULL THEN 'NULL' ELSE '''' || ORDER_NUMBER || '''' END || ', ' ||
    CASE WHEN CODE_1C IS NULL THEN 'NULL' ELSE '''' || CODE_1C || '''' END || ', ' ||
    '''' || STATUS || '''' ||
    ');'
FROM V_IRIDIUM_SERVICES_INFO
WHERE STATUS = 1
  AND CONTRACT_ID NOT LIKE '%-clone-%'
ORDER BY CODE_1C, CONTRACT_ID;

SPOOL OFF

-- Extended report with usage and charges
SPOOL billing_integration_with_charges.csv

PROMPT CONTRACT_ID|IMEI|CUSTOMER_NAME|AGREEMENT_NUMBER|ORDER_NUMBER|CODE_1C|BILL_MONTH|PLAN_NAME|USAGE_KB|OVERAGE_CHARGE|TOTAL_AMOUNT

SELECT 
    r.CONTRACT_ID || '|' ||
    r.IMEI || '|' ||
    NVL(r.CUSTOMER_NAME, '') || '|' ||
    NVL(r.AGREEMENT_NUMBER, '') || '|' ||
    NVL(r.ORDER_NUMBER, '') || '|' ||
    NVL(r.CODE_1C, '') || '|' ||
    TO_CHAR(r.BILL_MONTH, 'YYYY-MM') || '|' ||
    NVL(r.PLAN_NAME, '') || '|' ||
    NVL(TO_CHAR(r.TOTAL_USAGE_KB), '0') || '|' ||
    NVL(TO_CHAR(r.CALCULATED_OVERAGE), '0') || '|' ||
    NVL(TO_CHAR(r.SPNET_TOTAL_AMOUNT + r.STECCOM_TOTAL_AMOUNT), '0')
FROM V_CONSOLIDATED_REPORT_WITH_BILLING r
WHERE r.SERVICE_STATUS = 1
ORDER BY r.CODE_1C, r.BILL_MONTH DESC, r.CONTRACT_ID;

SPOOL OFF

-- Summary
SPOOL billing_integration_summary.txt

PROMPT ============================================================================
PROMPT Billing Integration Export Summary
PROMPT ============================================================================
PROMPT

SELECT 'Total active services: ' || COUNT(*) FROM V_IRIDIUM_SERVICES_INFO WHERE STATUS = 1 AND CONTRACT_ID NOT LIKE '%-clone-%';
SELECT 'Services with CODE_1C: ' || COUNT(*) FROM V_IRIDIUM_SERVICES_INFO WHERE STATUS = 1 AND CONTRACT_ID NOT LIKE '%-clone-%' AND CODE_1C IS NOT NULL;
SELECT 'Services without CODE_1C: ' || COUNT(*) FROM V_IRIDIUM_SERVICES_INFO WHERE STATUS = 1 AND CONTRACT_ID NOT LIKE '%-clone-%' AND CODE_1C IS NULL;
PROMPT
SELECT 'Excluded clone services: ' || COUNT(*) FROM V_IRIDIUM_SERVICES_INFO WHERE CONTRACT_ID LIKE '%-clone-%';
SELECT 'Excluded inactive services: ' || COUNT(*) FROM V_IRIDIUM_SERVICES_INFO WHERE STATUS != 1;

PROMPT
PROMPT Files generated:
PROMPT   1. billing_integration.csv - Basic customer data (CSV format)
PROMPT   2. billing_integration.sql - Basic customer data (SQL format)
PROMPT   3. billing_integration_with_charges.csv - With usage and charges
PROMPT   4. billing_integration_summary.txt - This summary
PROMPT
PROMPT CSV Fields:
PROMPT   - CONTRACT_ID: Login from services
PROMPT   - IMEI: Device identifier
PROMPT   - CUSTOMER_NAME: Organization name or person FIO
PROMPT   - AGREEMENT_NUMBER: Contract number (договор)
PROMPT   - ORDER_NUMBER: Order/appendix number (бланк)
PROMPT   - CODE_1C: Customer code from 1C system
PROMPT   - STATUS: Service status (1=active)
PROMPT
PROMPT Usage in 1C:
PROMPT   - Import billing_integration.csv into 1C
PROMPT   - Use CODE_1C for matching with existing customers
PROMPT   - Use billing_integration_with_charges.csv for monthly billing
PROMPT
PROMPT ============================================================================

SPOOL OFF

SET FEEDBACK ON
SET HEADING ON
SET ECHO ON

PROMPT
PROMPT Billing integration data exported successfully!
PROMPT

