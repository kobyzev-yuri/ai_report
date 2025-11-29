-- ============================================================================
-- Изучение структуры таблиц счетов-фактур для расчета доходов
-- BM_INVOICE, BM_INVOICE_ITEM
-- ============================================================================

SET LINESIZE 200
SET PAGESIZE 1000
SET FEEDBACK OFF

PROMPT ============================================================================
PROMPT 1. Структура таблицы BM_INVOICE (счета-фактуры)
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
  AND TABLE_NAME = 'BM_INVOICE'
ORDER BY COLUMN_ID;

PROMPT
PROMPT ============================================================================
PROMPT 2. Комментарии к таблице BM_INVOICE
PROMPT ============================================================================

SELECT 
    COLUMN_NAME,
    COMMENTS
FROM ALL_COL_COMMENTS
WHERE OWNER = 'BILLING7'
  AND TABLE_NAME = 'BM_INVOICE'
ORDER BY COLUMN_NAME;

PROMPT
PROMPT ============================================================================
PROMPT 3. Структура таблицы BM_INVOICE_ITEM (детали счетов-фактур)
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
  AND TABLE_NAME = 'BM_INVOICE_ITEM'
ORDER BY COLUMN_ID;

PROMPT
PROMPT ============================================================================
PROMPT 4. Комментарии к таблице BM_INVOICE_ITEM
PROMPT ============================================================================

SELECT 
    COLUMN_NAME,
    COMMENTS
FROM ALL_COL_COMMENTS
WHERE OWNER = 'BILLING7'
  AND TABLE_NAME = 'BM_INVOICE_ITEM'
ORDER BY COLUMN_NAME;

PROMPT
PROMPT ============================================================================
PROMPT 5. Примеры данных из BM_INVOICE (первые 5 записей)
PROMPT ============================================================================

SELECT * FROM BM_INVOICE
WHERE ROWNUM <= 5;

PROMPT
PROMPT ============================================================================
PROMPT 6. Примеры данных из BM_INVOICE_ITEM для Iridium услуг (первые 5 записей)
PROMPT ============================================================================

SELECT ii.* 
FROM BM_INVOICE_ITEM ii
JOIN SERVICES s ON ii.SERVICE_ID = s.SERVICE_ID
WHERE s.TYPE_ID IN (9002, 9005, 9008, 9013, 9014)
  AND ROWNUM <= 5;

PROMPT
PROMPT ============================================================================
PROMPT 7. Связь между BM_INVOICE и BM_INVOICE_ITEM
PROMPT ============================================================================

SELECT 
    i.INVOICE_ID,
    i.INVOICE_DATE,
    i.CUSTOMER_ID,
    i.ACCOUNT_ID,
    COUNT(ii.INVOICE_ITEM_ID) as ITEMS_COUNT,
    SUM(ii.MONEY) as TOTAL_AMOUNT
FROM BM_INVOICE i
LEFT JOIN BM_INVOICE_ITEM ii ON i.INVOICE_ID = ii.INVOICE_ID
WHERE ROWNUM <= 5
GROUP BY i.INVOICE_ID, i.INVOICE_DATE, i.CUSTOMER_ID, i.ACCOUNT_ID;

PROMPT
PROMPT ============================================================================
PROMPT 8. Связь BM_INVOICE_ITEM с SERVICES для Iridium
PROMPT ============================================================================

SELECT 
    ii.INVOICE_ITEM_ID,
    ii.INVOICE_ID,
    ii.SERVICE_ID,
    s.LOGIN,
    s.TYPE_ID,
    s.TARIFF_ID,
    ii.MONEY,
    ii.DESCRIPTION
FROM BM_INVOICE_ITEM ii
JOIN SERVICES s ON ii.SERVICE_ID = s.SERVICE_ID
WHERE s.TYPE_ID IN (9002, 9005, 9008, 9013, 9014)
  AND ROWNUM <= 5;

PROMPT
PROMPT ============================================================================
PROMPT Изучение завершено
PROMPT ============================================================================

SET FEEDBACK ON





