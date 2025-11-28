-- ============================================================================
-- Изучение структуры таблицы BM_PERIODS - финансовые периоды
-- ============================================================================

SET LINESIZE 200
SET PAGESIZE 1000
SET FEEDBACK OFF

PROMPT ============================================================================
PROMPT 1. Структура таблицы BM_PERIODS
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
  AND TABLE_NAME = 'BM_PERIODS'
ORDER BY COLUMN_ID;

PROMPT
PROMPT ============================================================================
PROMPT 2. Комментарии к таблице BM_PERIODS
PROMPT ============================================================================

SELECT 
    COLUMN_NAME,
    COMMENTS
FROM ALL_COL_COMMENTS
WHERE OWNER = 'BILLING7'
  AND TABLE_NAME = 'BM_PERIODS'
ORDER BY COLUMN_NAME;

PROMPT
PROMPT ============================================================================
PROMPT 3. Примеры данных из BM_PERIODS (последние 12 периодов)
PROMPT ============================================================================

SELECT * FROM BM_PERIODS
ORDER BY PERIOD_ID DESC
FETCH FIRST 12 ROWS ONLY;

PROMPT
PROMPT ============================================================================
PROMPT 4. Использование PERIOD_ID в финансовых таблицах
PROMPT ============================================================================

SELECT 
    'BM_INVOICE' as table_name,
    COUNT(DISTINCT PERIOD_ID) as unique_periods,
    MIN(PERIOD_ID) as min_period,
    MAX(PERIOD_ID) as max_period
FROM BM_INVOICE
UNION ALL
SELECT 
    'BM_INVOICE_ITEM',
    COUNT(DISTINCT PERIOD_ID),
    MIN(PERIOD_ID),
    MAX(PERIOD_ID)
FROM BM_INVOICE_ITEM
UNION ALL
SELECT 
    'ANALYTICS',
    COUNT(DISTINCT PERIOD_ID),
    MIN(PERIOD_ID),
    MAX(PERIOD_ID)
FROM ANALYTICS
WHERE SERVICE_ID IN (SELECT SERVICE_ID FROM SERVICES WHERE TYPE_ID IN (9002, 9005, 9008, 9013, 9014));

PROMPT
PROMPT ============================================================================
PROMPT 5. Связь PERIOD_ID с датами в BM_PERIODS
PROMPT ============================================================================

SELECT 
    p.PERIOD_ID,
    p.DATE_BEG,
    p.DATE_END,
    TO_CHAR(p.DATE_BEG, 'DD.MM.YYYY') as START_DATE,
    TO_CHAR(p.DATE_END, 'DD.MM.YYYY') as END_DATE,
    CASE 
        WHEN TO_NUMBER(TO_CHAR(p.DATE_BEG, 'DD')) = 2 
         AND TO_NUMBER(TO_CHAR(p.DATE_END, 'DD')) = 1 
         AND TO_NUMBER(TO_CHAR(p.DATE_END, 'MM')) = TO_NUMBER(TO_CHAR(p.DATE_BEG, 'MM')) + 1
        THEN 'Iridium period (2nd to 1st)'
        ELSE 'STECCOM period (standard)'
    END as PERIOD_TYPE
FROM BM_PERIODS p
ORDER BY p.PERIOD_ID DESC
FETCH FIRST 12 ROWS ONLY;

PROMPT
PROMPT ============================================================================
PROMPT Изучение завершено
PROMPT ============================================================================

SET FEEDBACK ON


