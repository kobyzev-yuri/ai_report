-- ============================================================================
-- Экспорт всех данных из Oracle VIEW
-- Использование: sqlplus billing7/billing@bm7 @export_all.sql
-- Создает все необходимые экспорты за один раз
-- ============================================================================

SET ECHO OFF
SET FEEDBACK OFF
SET VERIFY OFF
SET TERMOUT ON

PROMPT
PROMPT ============================================================================
PROMPT Экспорт данных из Oracle VIEW
PROMPT ============================================================================
PROMPT

-- 1. Экспорт для 1С (billing_integration.csv)
PROMPT [1/3] Экспорт для 1С...
SET HEADING ON
SET PAGESIZE 50000
SET LINESIZE 500
SET COLSEP '|'
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
WHERE STATUS = 10
  AND CONTRACT_ID NOT LIKE '%-clone-%'
  AND (STOP_DATE IS NULL OR STOP_DATE > SYSDATE)
  AND IS_SUSPENDED = 'N'
ORDER BY CODE_1C, CONTRACT_ID;

SPOOL OFF

-- 2. Экспорт для PostgreSQL (полные данные V_IRIDIUM_SERVICES_INFO)
PROMPT [2/3] Экспорт для PostgreSQL...
SET HEADING OFF
SET PAGESIZE 0
SET LINESIZE 32767
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
    NVL(IS_SUSPENDED, 'N') || '|' ||
    NVL(CODE_1C, '')
FROM V_IRIDIUM_SERVICES_INFO;

SPOOL OFF

-- 3. Экспорт истории переводов (для аудита)
PROMPT [3/3] Экспорт истории переводов...
SET HEADING ON
SET PAGESIZE 50000
SET LINESIZE 600
SET COLSEP '|'
SPOOL service_transfer_history.csv

PROMPT CONTRACT_ID|IMEI|CUSTOMER_NAME|AGREEMENT_NUMBER|ORDER_NUMBER|CODE_1C|STATUS|ACTUAL_STATUS|START_DATE|STOP_DATE|IS_CLONE

SELECT 
    CONTRACT_ID || '|' ||
    NVL(IMEI, '') || '|' ||
    NVL(CUSTOMER_NAME, '') || '|' ||
    NVL(AGREEMENT_NUMBER, '') || '|' ||
    NVL(ORDER_NUMBER, '') || '|' ||
    NVL(CODE_1C, '') || '|' ||
    NVL(TO_CHAR(STATUS), '') || '|' ||
    NVL(TO_CHAR(ACTUAL_STATUS), '') || '|' ||
    NVL(TO_CHAR(START_DATE, 'YYYY-MM-DD'), '') || '|' ||
    NVL(TO_CHAR(STOP_DATE, 'YYYY-MM-DD'), '') || '|' ||
    CASE WHEN CONTRACT_ID LIKE '%-clone-%' THEN 'YES' ELSE 'NO' END
FROM V_IRIDIUM_SERVICES_INFO
ORDER BY 
    CASE 
        WHEN CONTRACT_ID LIKE '%-clone-%' THEN SUBSTR(CONTRACT_ID, 1, INSTR(CONTRACT_ID, '-clone-') - 1)
        ELSE CONTRACT_ID
    END,
    START_DATE;

SPOOL OFF

-- Сводка
PROMPT
PROMPT ============================================================================
PROMPT Экспорт завершен!
PROMPT ============================================================================
PROMPT
PROMPT Созданные файлы:
PROMPT   1. billing_integration.csv - для импорта в 1С
PROMPT   2. ../test/V_IRIDIUM_SERVICES_INFO.csv - для импорта в PostgreSQL
PROMPT   3. service_transfer_history.csv - история переводов (для аудита)
PROMPT
PROMPT ============================================================================

SET FEEDBACK ON
SET HEADING ON
SET ECHO ON

