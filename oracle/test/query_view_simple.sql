-- ============================================================================
-- Simple query to view V_IRIDIUM_SERVICES_INFO data
-- Простой запрос для просмотра данных view
-- ============================================================================
-- Usage: sqlplus -s $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @query_view_simple.sql

SET PAGESIZE 50
SET LINESIZE 300
SET FEEDBACK ON
SET HEADING ON
SET COLSEP '|'

-- Простой запрос с основными полями
SELECT 
    SERVICE_ID,
    CONTRACT_ID,
    LEFT(IMEI, 20) AS IMEI,
    CUSTOMER_NAME,
    CODE_1C,
    AGREEMENT_NUMBER,
    STATUS
FROM V_IRIDIUM_SERVICES_INFO
WHERE ROWNUM <= 20
ORDER BY SERVICE_ID;

-- Статистика
PROMPT
PROMPT ============================================================================
SELECT 
    COUNT(*) AS TOTAL,
    COUNT(CODE_1C) AS WITH_CODE_1C,
    COUNT(CUSTOMER_NAME) AS WITH_CUSTOMER_NAME
FROM V_IRIDIUM_SERVICES_INFO;

EXIT

