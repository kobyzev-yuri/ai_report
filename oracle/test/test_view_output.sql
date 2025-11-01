-- ============================================================================
-- Test V_IRIDIUM_SERVICES_INFO view output
-- Проверка корректности данных после применения исправленного view
-- ============================================================================
-- Usage: sqlplus -s $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @test_view_output.sql

SET PAGESIZE 1000
SET LINESIZE 200
SET FEEDBACK ON
SET HEADING ON

-- Проверка количества записей
PROMPT ============================================================================
PROMPT [TEST 1] Total records count
PROMPT ============================================================================

SELECT 
    COUNT(*) AS TOTAL_RECORDS,
    COUNT(DISTINCT TYPE_ID) AS TYPE_COUNT,
    COUNT(CASE WHEN SERVICE_ID IS NOT NULL THEN 1 END) AS WITH_SERVICE_ID,
    COUNT(CASE WHEN CODE_1C IS NOT NULL THEN 1 END) AS WITH_CODE_1C,
    COUNT(CASE WHEN CUSTOMER_NAME IS NOT NULL THEN 1 END) AS WITH_CUSTOMER_NAME
FROM V_IRIDIUM_SERVICES_INFO;

-- Проверка по типам услуг
PROMPT ============================================================================
PROMPT [TEST 2] Records by service type (from SERVICES table)
PROMPT ============================================================================

SELECT 
    s.TYPE_ID,
    COUNT(*) AS COUNT
FROM SERVICES s
JOIN CUSTOMERS c ON s.CUSTOMER_ID = c.CUSTOMER_ID
WHERE s.TYPE_ID IN (9002, 9014)
GROUP BY s.TYPE_ID
ORDER BY s.TYPE_ID;

-- Примеры записей
PROMPT ============================================================================
PROMPT [TEST 3] Sample records (first 5)
PROMPT ============================================================================

SELECT 
    SERVICE_ID,
    CONTRACT_ID,
    LEFT(IMEI, 20) AS IMEI,
    CUSTOMER_NAME,
    CODE_1C,
    AGREEMENT_NUMBER
FROM V_IRIDIUM_SERVICES_INFO
WHERE ROWNUM <= 5
ORDER BY SERVICE_ID;

-- Проверка CODE_1C
PROMPT ============================================================================
PROMPT [TEST 4] CODE_1C statistics
PROMPT ============================================================================

SELECT 
    COUNT(*) AS TOTAL,
    COUNT(CODE_1C) AS WITH_CODE_1C,
    COUNT(*) - COUNT(CODE_1C) AS NULL_CODE_1C
FROM V_IRIDIUM_SERVICES_INFO;

-- Примеры с CODE_1C
PROMPT ============================================================================
PROMPT [TEST 5] Sample records WITH CODE_1C
PROMPT ============================================================================

SELECT 
    SERVICE_ID,
    CONTRACT_ID,
    CUSTOMER_NAME,
    CODE_1C,
    AGREEMENT_NUMBER
FROM V_IRIDIUM_SERVICES_INFO
WHERE CODE_1C IS NOT NULL
  AND ROWNUM <= 5
ORDER BY SERVICE_ID;

EXIT


