SET PAGESIZE 50000
SET LINESIZE 32767
SET HEADING ON
SET FEEDBACK ON
SET VERIFY OFF

PROMPT ============================================================================
PROMPT Проверка наличия CODE_1C в OUTER_IDS для клиентов из V_IRIDIUM_SERVICES_INFO
PROMPT ============================================================================
PROMPT

-- 1. Проверим несколько CUSTOMER_ID из view
PROMPT 1. Примеры CUSTOMER_ID из V_IRIDIUM_SERVICES_INFO:
SELECT 
    c.CUSTOMER_ID,
    (SELECT oi.EXT_ID FROM OUTER_IDS oi 
     WHERE oi.ID = c.CUSTOMER_ID AND UPPER(oi.TBL) = 'CUSTOMERS' AND ROWNUM = 1) AS CODE_1C_FROM_VIEW,
    MAX(CASE WHEN cd.MNEMONIC = 'b_name' THEN cc.VALUE END) AS ORG_NAME
FROM SERVICES s
JOIN CUSTOMERS c ON s.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN BM_CUSTOMER_CONTACT cc ON c.CUSTOMER_ID = cc.CUSTOMER_ID
LEFT JOIN BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID
    AND cd.MNEMONIC = 'b_name'
WHERE s.TYPE_ID IN (9002, 9014)
  AND ROWNUM <= 10
GROUP BY c.CUSTOMER_ID;

PROMPT
PROMPT ============================================================================
PROMPT 2. Проверка OUTER_IDS напрямую (все возможные значения TBL):
PROMPT ============================================================================
SELECT DISTINCT TBL, COUNT(*) AS CNT, MIN(ID) AS SAMPLE_ID
FROM OUTER_IDS
GROUP BY TBL
ORDER BY TBL;

PROMPT
PROMPT ============================================================================
PROMPT 3. Примеры записей OUTER_IDS для CUSTOMERS:
PROMPT ============================================================================
SELECT 
    oi.ID AS CUSTOMER_ID,
    oi.TBL,
    oi.EXT_ID AS CODE_1C,
    (SELECT MAX(CASE WHEN cd.MNEMONIC = 'b_name' THEN cc.VALUE END)
     FROM BM_CUSTOMER_CONTACT cc
     JOIN BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID
     WHERE cc.CUSTOMER_ID = oi.ID) AS ORG_NAME
FROM OUTER_IDS oi
WHERE UPPER(oi.TBL) = 'CUSTOMERS'
  AND ROWNUM <= 10;

PROMPT
PROMPT ============================================================================
PROMPT 4. Проверка: есть ли записи OUTER_IDS для CUSTOMER_ID из SERVICES:
PROMPT ============================================================================
SELECT 
    COUNT(DISTINCT s.CUSTOMER_ID) AS TOTAL_CUSTOMERS_IN_SERVICES,
    COUNT(DISTINCT oi.ID) AS CUSTOMERS_WITH_OUTER_IDS,
    COUNT(DISTINCT s.CUSTOMER_ID) - COUNT(DISTINCT oi.ID) AS CUSTOMERS_WITHOUT_OUTER_IDS
FROM SERVICES s
JOIN CUSTOMERS c ON s.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN OUTER_IDS oi ON oi.ID = c.CUSTOMER_ID AND UPPER(oi.TBL) = 'CUSTOMERS'
WHERE s.TYPE_ID IN (9002, 9014);

PROMPT
PROMPT ============================================================================
PROMPT 5. Проверка V_IRIDIUM_SERVICES_INFO: сколько записей с CODE_1C:
PROMPT ============================================================================
SELECT 
    COUNT(*) AS TOTAL_SERVICES,
    COUNT(CODE_1C) AS WITH_CODE_1C,
    COUNT(*) - COUNT(CODE_1C) AS WITHOUT_CODE_1C
FROM V_IRIDIUM_SERVICES_INFO
WHERE STATUS = 1;

PROMPT
PROMPT ============================================================================
PROMPT 6. Примеры записей из V_IRIDIUM_SERVICES_INFO с CODE_1C:
PROMPT ============================================================================
SELECT 
    SERVICE_ID,
    CUSTOMER_ID,
    CODE_1C,
    ORGANIZATION_NAME
FROM V_IRIDIUM_SERVICES_INFO
WHERE CODE_1C IS NOT NULL
  AND ROWNUM <= 5;

EXIT;

