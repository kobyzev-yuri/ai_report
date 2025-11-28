-- ============================================================================
-- Изучение структуры таблицы ANALYTICS - основа для формирования счетов-фактур
-- ============================================================================

SET LINESIZE 200
SET PAGESIZE 1000
SET FEEDBACK OFF

PROMPT ============================================================================
PROMPT 1. Структура таблицы ANALYTICS
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
  AND TABLE_NAME = 'ANALYTICS'
ORDER BY COLUMN_ID;

PROMPT
PROMPT ============================================================================
PROMPT 2. Комментарии к таблице ANALYTICS
PROMPT ============================================================================

SELECT 
    COLUMN_NAME,
    COMMENTS
FROM ALL_COL_COMMENTS
WHERE OWNER = 'BILLING7'
  AND TABLE_NAME = 'ANALYTICS'
ORDER BY COLUMN_NAME;

PROMPT
PROMPT ============================================================================
PROMPT 3. Примеры данных из ANALYTICS для Iridium услуг (первые 5 записей)
PROMPT ============================================================================

SELECT a.* 
FROM ANALYTICS a
JOIN SERVICES s ON a.SERVICE_ID = s.SERVICE_ID
WHERE s.TYPE_ID IN (9002, 9005, 9008, 9013)
  AND ROWNUM <= 5;

PROMPT
PROMPT ============================================================================
PROMPT 4. Связь ANALYTICS с BM_INVOICE_ITEM
PROMPT ============================================================================

SELECT 
    a.ANALYTICS_ID,
    a.SERVICE_ID,
    a.PERIOD_ID,
    a.MONEY as ANALYTICS_MONEY,
    ii.INVOICE_ITEM_ID,
    ii.INVOICE_ID,
    ii.MONEY as INVOICE_MONEY
FROM ANALYTICS a
LEFT JOIN BM_INVOICE_ITEM ii ON a.ANALYTICS_ID = ii.BILL_ITEM_ID
WHERE a.SERVICE_ID IN (
    SELECT SERVICE_ID FROM SERVICES WHERE TYPE_ID IN (9002, 9005, 9008, 9013)
)
AND ROWNUM <= 5;

PROMPT
PROMPT ============================================================================
PROMPT 5. Статистика: ANALYTICS vs BM_INVOICE_ITEM для Iridium услуг
PROMPT ============================================================================

SELECT 
    'Всего записей в ANALYTICS для Iridium' as metric,
    COUNT(*) as count
FROM ANALYTICS a
JOIN SERVICES s ON a.SERVICE_ID = s.SERVICE_ID
WHERE s.TYPE_ID IN (9002, 9005, 9008, 9013)
UNION ALL
SELECT 
    'Записей в BM_INVOICE_ITEM для Iridium',
    COUNT(*)
FROM BM_INVOICE_ITEM ii
JOIN SERVICES s ON ii.SERVICE_ID = s.SERVICE_ID
WHERE s.TYPE_ID IN (9002, 9005, 9008, 9013)
UNION ALL
SELECT 
    'Записей ANALYTICS без счетов-фактур',
    COUNT(*)
FROM ANALYTICS a
JOIN SERVICES s ON a.SERVICE_ID = s.SERVICE_ID
LEFT JOIN BM_INVOICE_ITEM ii ON a.ANALYTICS_ID = ii.BILL_ITEM_ID
WHERE s.TYPE_ID IN (9002, 9005, 9008, 9013)
  AND ii.INVOICE_ITEM_ID IS NULL;

PROMPT
PROMPT ============================================================================
PROMPT Изучение завершено
PROMPT ============================================================================

SET FEEDBACK ON

