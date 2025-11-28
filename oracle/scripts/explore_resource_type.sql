-- ============================================================================
-- Изучение структуры таблицы BM_RESOURCE_TYPE - расшифровки услуг для бухгалтерии
-- ============================================================================

SET LINESIZE 200
SET PAGESIZE 1000
SET FEEDBACK OFF

PROMPT ============================================================================
PROMPT 1. Структура таблицы BM_RESOURCE_TYPE
PROMPT ============================================================================

SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    DATA_LENGTH,
    DATA_PRECISION,
    DATA_SCALE,
    NULLABLE
FROM ALL_TAB_COLUMNS
WHERE OWNER = 'BILLING7'
  AND TABLE_NAME = 'BM_RESOURCE_TYPE'
ORDER BY COLUMN_ID;

PROMPT
PROMPT ============================================================================
PROMPT 2. Комментарии к таблице BM_RESOURCE_TYPE
PROMPT ============================================================================

SELECT 
    COLUMN_NAME,
    COMMENTS
FROM ALL_COL_COMMENTS
WHERE OWNER = 'BILLING7'
  AND TABLE_NAME = 'BM_RESOURCE_TYPE'
ORDER BY COLUMN_NAME;

PROMPT
PROMPT ============================================================================
PROMPT 3. Примеры данных из BM_RESOURCE_TYPE (первые 20 записей)
PROMPT ============================================================================

SELECT * FROM BM_RESOURCE_TYPE
ORDER BY RESOURCE_TYPE_ID
FETCH FIRST 20 ROWS ONLY;

PROMPT
PROMPT ============================================================================
PROMPT 4. Использование RESOURCE_TYPE_ID в финансовых таблицах для Iridium
PROMPT ============================================================================

SELECT 
    rt.RESOURCE_TYPE_ID,
    rt.MNEMONIC,
    rt.NAME,
    COUNT(DISTINCT a.AID) as ANALYTICS_COUNT,
    COUNT(DISTINCT ii.INVOICE_ITEM_ID) as INVOICE_ITEMS_COUNT
FROM BM_RESOURCE_TYPE rt
LEFT JOIN ANALYTICS a ON rt.RESOURCE_TYPE_ID = a.RESOURCE_TYPE_ID
    AND a.SERVICE_ID IN (SELECT SERVICE_ID FROM SERVICES WHERE TYPE_ID IN (9002, 9005, 9008, 9013, 9014))
LEFT JOIN BM_INVOICE_ITEM ii ON rt.RESOURCE_TYPE_ID = ii.RESOURCE_TYPE_ID
    AND ii.SERVICE_ID IN (SELECT SERVICE_ID FROM SERVICES WHERE TYPE_ID IN (9002, 9005, 9008, 9013, 9014))
WHERE rt.RESOURCE_TYPE_ID IN (
    SELECT DISTINCT RESOURCE_TYPE_ID FROM ANALYTICS 
    WHERE SERVICE_ID IN (SELECT SERVICE_ID FROM SERVICES WHERE TYPE_ID IN (9002, 9005, 9008, 9013, 9014))
)
GROUP BY rt.RESOURCE_TYPE_ID, rt.MNEMONIC, rt.NAME
ORDER BY rt.RESOURCE_TYPE_ID;

PROMPT
PROMPT ============================================================================
PROMPT 5. Примеры использования RESOURCE_TYPE_ID в ANALYTICS для Iridium
PROMPT ============================================================================

SELECT 
    a.AID,
    a.SERVICE_ID,
    s.LOGIN,
    s.TYPE_ID,
    a.RESOURCE_TYPE_ID,
    rt.MNEMONIC,
    rt.NAME,
    a.MONEY,
    a.CLASS_NAME
FROM ANALYTICS a
JOIN SERVICES s ON a.SERVICE_ID = s.SERVICE_ID
LEFT JOIN BM_RESOURCE_TYPE rt ON a.RESOURCE_TYPE_ID = rt.RESOURCE_TYPE_ID
WHERE s.TYPE_ID IN (9002, 9005, 9008, 9013, 9014)
  AND ROWNUM <= 10;

PROMPT
PROMPT ============================================================================
PROMPT Изучение завершено
PROMPT ============================================================================

SET FEEDBACK ON


