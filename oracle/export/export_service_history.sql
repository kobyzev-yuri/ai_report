-- ============================================================================
-- Export Service Transfer History
-- Shows services including clones/transfers for audit purposes
-- ============================================================================

SET ECHO OFF
SET FEEDBACK OFF
SET HEADING ON
SET PAGESIZE 50000
SET LINESIZE 600
SET TRIMSPOOL ON
SET VERIFY OFF
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
    -- Extract base login (before -clone-)
    CASE 
        WHEN CONTRACT_ID LIKE '%-clone-%' THEN SUBSTR(CONTRACT_ID, 1, INSTR(CONTRACT_ID, '-clone-') - 1)
        ELSE CONTRACT_ID
    END,
    START_DATE;

SPOOL OFF

-- Summary by login showing transfers
SPOOL service_transfer_summary.txt

PROMPT ============================================================================
PROMPT Service Transfer History - Summary
PROMPT ============================================================================
PROMPT
PROMPT Services with multiple records (transfers/clones):
PROMPT

SELECT 
    CASE 
        WHEN CONTRACT_ID LIKE '%-clone-%' THEN SUBSTR(CONTRACT_ID, 1, INSTR(CONTRACT_ID, '-clone-') - 1)
        ELSE CONTRACT_ID
    END AS BASE_LOGIN,
    COUNT(*) AS RECORD_COUNT,
    MIN(START_DATE) AS FIRST_START,
    MAX(NVL(STOP_DATE, SYSDATE)) AS LAST_ACTIVITY,
    SUM(CASE WHEN CONTRACT_ID LIKE '%-clone-%' THEN 1 ELSE 0 END) AS CLONE_COUNT,
    SUM(CASE WHEN STATUS = 1 THEN 1 ELSE 0 END) AS ACTIVE_COUNT
FROM V_IRIDIUM_SERVICES_INFO
GROUP BY 
    CASE 
        WHEN CONTRACT_ID LIKE '%-clone-%' THEN SUBSTR(CONTRACT_ID, 1, INSTR(CONTRACT_ID, '-clone-') - 1)
        ELSE CONTRACT_ID
    END
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;

PROMPT
PROMPT ============================================================================
PROMPT Understanding clone services:
PROMPT
PROMPT - CONTRACT_ID with '-clone-YYYY-MM-DD' suffix = service was closed/transferred
PROMPT - The date indicates when service was closed
PROMPT - Same login exists with new SERVICE_ID on different agreement
PROMPT - Clone records have STATUS = -10 (closed) and STOP_DATE populated
PROMPT - For billing, use only STATUS = 1 without '-clone-' suffix
PROMPT
PROMPT ============================================================================

SPOOL OFF

SET FEEDBACK ON
SET HEADING ON
SET ECHO ON

PROMPT
PROMPT Service history exported!
PROMPT Files: service_transfer_history.csv, service_transfer_summary.txt
PROMPT







