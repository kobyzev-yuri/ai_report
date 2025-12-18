-- Проверка структуры BM_PERIOD
SET PAGESIZE 50
SET LINESIZE 200
SET FEEDBACK OFF

PROMPT ============================================================================
PROMPT Структура таблицы BM_PERIOD
PROMPT ============================================================================
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    DATA_LENGTH,
    NULLABLE
FROM 
    USER_TAB_COLUMNS
WHERE 
    TABLE_NAME = 'BM_PERIOD'
ORDER BY 
    COLUMN_ID;

PROMPT 
PROMPT ============================================================================
PROMPT Пример данных из BM_PERIOD
PROMPT ============================================================================
SELECT 
    PERIOD_ID,
    DATE_BEG,
    DATE_END,
    START_DATE,
    END_DATE
FROM 
    BM_PERIOD
WHERE 
    ROWNUM <= 5;

EXIT;







