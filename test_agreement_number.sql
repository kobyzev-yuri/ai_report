-- ============================================================================
-- Тест получения AGREEMENT_NUMBER для IMEI 300434069401140
-- ============================================================================

SET LINESIZE 200
SET PAGESIZE 100

PROMPT ============================================================================
PROMPT Тест 1: Проверка данных в SERVICES_EXT для IMEI 300434069401140
PROMPT ============================================================================

SELECT 
    se.SERVICE_ID,
    se.VALUE AS IMEI,
    se.DATE_BEG,
    se.DATE_END,
    s.ACCOUNT_ID,
    a.DESCRIPTION AS AGREEMENT_NUMBER
FROM SERVICES_EXT se
JOIN SERVICES s ON se.SERVICE_ID = s.SERVICE_ID
JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
WHERE se.VALUE = '300434069401140'
ORDER BY 
    CASE WHEN se.DATE_END IS NULL THEN 0 ELSE 1 END,
    se.DATE_BEG DESC,
    se.SERVICE_ID DESC;

PROMPT
PROMPT ============================================================================
PROMPT Тест 2: Проверка подзапроса imei_service_ext_info
PROMPT ============================================================================

SELECT 
    se_ranked.VALUE AS IMEI,
    se_ranked.SERVICE_ID,
    se_ranked.AGREEMENT_NUMBER,
    se_ranked.DATE_END,
    se_ranked.rn
FROM (
    SELECT 
        se.VALUE,
        se.SERVICE_ID,
        a.DESCRIPTION AS AGREEMENT_NUMBER,
        se.DATE_END,
        ROW_NUMBER() OVER (
            PARTITION BY se.VALUE 
            ORDER BY 
                CASE WHEN se.DATE_END IS NULL THEN 0 ELSE 1 END,
                se.DATE_BEG DESC NULLS LAST,
                se.SERVICE_ID DESC
        ) AS rn
    FROM SERVICES_EXT se
    JOIN SERVICES s ON se.SERVICE_ID = s.SERVICE_ID
    JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
    WHERE se.VALUE IS NOT NULL
) se_ranked
WHERE se_ranked.VALUE = '300434069401140'
ORDER BY se_ranked.rn;

PROMPT
PROMPT ============================================================================
PROMPT Тест 3: Проверка view V_CONSOLIDATED_REPORT_WITH_BILLING для IMEI
PROMPT ============================================================================

SELECT 
    FINANCIAL_PERIOD,
    BILL_MONTH,
    IMEI,
    CONTRACT_ID,
    SERVICE_ID,
    AGREEMENT_NUMBER,
    CUSTOMER_NAME,
    CODE_1C
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE IMEI = '300434069401140'
  AND ROWNUM <= 5
ORDER BY BILL_MONTH DESC;

PROMPT
PROMPT ============================================================================
PROMPT Тест 4: Проверка JOIN'ов в view (через EXPLAIN PLAN)
PROMPT ============================================================================

EXPLAIN PLAN FOR
SELECT 
    FINANCIAL_PERIOD,
    BILL_MONTH,
    IMEI,
    CONTRACT_ID,
    SERVICE_ID,
    AGREEMENT_NUMBER
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE IMEI = '300434069401140'
  AND ROWNUM <= 1;

SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

PROMPT
PROMPT ============================================================================
PROMPT Тест 5: Проверка данных из V_IRIDIUM_SERVICES_INFO по CONTRACT_ID
PROMPT ============================================================================

SELECT 
    SERVICE_ID,
    CONTRACT_ID,
    IMEI,
    AGREEMENT_NUMBER,
    CUSTOMER_NAME,
    CODE_1C
FROM V_IRIDIUM_SERVICES_INFO
WHERE CONTRACT_ID = 'SUB-56682935528'
  AND ROWNUM <= 5;

PROMPT
PROMPT ============================================================================
PROMPT Тест 6: Проверка данных из V_IRIDIUM_SERVICES_INFO по IMEI
PROMPT ============================================================================

SELECT 
    SERVICE_ID,
    CONTRACT_ID,
    IMEI,
    AGREEMENT_NUMBER,
    CUSTOMER_NAME,
    CODE_1C
FROM V_IRIDIUM_SERVICES_INFO
WHERE IMEI = '300434069401140'
  AND ROWNUM <= 5;

