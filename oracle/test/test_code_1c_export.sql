-- ============================================================================
-- Test script to verify CODE_1C export from Oracle
-- Проверка экспорта CODE_1C из OUTER_IDS
-- ============================================================================
-- Usage: sqlplus -s $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @test_code_1c_export.sql

SET PAGESIZE 1000
SET LINESIZE 200

-- Проверка: есть ли записи с CODE_1C в view
PROMPT ============================================================================
PROMPT [TEST 1] Check if V_IRIDIUM_SERVICES_INFO has CODE_1C values
PROMPT ============================================================================

SELECT 
    COUNT(*) AS TOTAL_RECORDS,
    COUNT(CODE_1C) AS WITH_CODE_1C,
    COUNT(*) - COUNT(CODE_1C) AS NULL_CODE_1C
FROM V_IRIDIUM_SERVICES_INFO;

-- Проверка: примеры записей с CODE_1C
PROMPT ============================================================================
PROMPT [TEST 2] Sample records WITH CODE_1C
PROMPT ============================================================================

SELECT 
    SERVICE_ID,
    CONTRACT_ID,
    CUSTOMER_ID,
    CUSTOMER_NAME,
    CODE_1C
FROM V_IRIDIUM_SERVICES_INFO
WHERE CODE_1C IS NOT NULL
  AND ROWNUM <= 10
ORDER BY SERVICE_ID;

-- Проверка: связь через OUTER_IDS
PROMPT ============================================================================
PROMPT [TEST 3] Check OUTER_IDS connection
PROMPT ============================================================================

SELECT 
    COUNT(DISTINCT c.CUSTOMER_ID) AS TOTAL_CUSTOMERS,
    COUNT(DISTINCT oi.ID) AS CUSTOMERS_WITH_OUTER_ID,
    COUNT(DISTINCT CASE WHEN oi.EXT_ID IS NOT NULL THEN oi.ID END) AS WITH_CODE_1C
FROM SERVICES s
JOIN CUSTOMERS c ON s.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN OUTER_IDS oi ON c.CUSTOMER_ID = oi.ID AND oi.TBL = 'CUSTOMERS'
WHERE s.TYPE_ID = 9002
  AND s.STATUS = 1;

-- Проверка: примеры связей CUSTOMER_ID -> CODE_1C
PROMPT ============================================================================
PROMPT [TEST 4] Sample CUSTOMER_ID -> CODE_1C mappings
PROMPT ============================================================================

SELECT 
    c.CUSTOMER_ID,
    oi.EXT_ID AS CODE_1C,
    oi.TBL
FROM CUSTOMERS c
JOIN SERVICES s ON c.CUSTOMER_ID = s.CUSTOMER_ID
LEFT JOIN OUTER_IDS oi ON c.CUSTOMER_ID = oi.ID AND oi.TBL = 'CUSTOMERS'
WHERE s.TYPE_ID = 9002
  AND s.STATUS = 1
  AND oi.EXT_ID IS NOT NULL
  AND ROWNUM <= 10;

PROMPT ============================================================================
PROMPT [TEST 5] Services without CODE_1C (need mapping)
PROMPT ============================================================================

SELECT 
    s.SERVICE_ID,
    s.LOGIN AS CONTRACT_ID,
    c.CUSTOMER_ID,
    (SELECT VALUE FROM BM_CUSTOMER_CONTACT cc 
     JOIN BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID 
     WHERE cc.CUSTOMER_ID = c.CUSTOMER_ID AND cd.MNEMONIC = 'b_name' AND ROWNUM = 1) AS CUSTOMER_NAME
FROM SERVICES s
JOIN CUSTOMERS c ON s.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN OUTER_IDS oi ON c.CUSTOMER_ID = oi.ID AND oi.TBL = 'CUSTOMERS'
WHERE s.TYPE_ID = 9002
  AND s.STATUS = 1
  AND oi.EXT_ID IS NULL
  AND ROWNUM <= 10;

EXIT

