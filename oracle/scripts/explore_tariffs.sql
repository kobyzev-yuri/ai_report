-- ============================================================================
-- Изучение структуры таблиц тарифов для расчета доходов
-- BM_TARIFF, BM_SERVICE_TARIFF, BM_TARIFFEL
-- ============================================================================

SET LINESIZE 200
SET PAGESIZE 1000
SET FEEDBACK OFF

PROMPT ============================================================================
PROMPT 1. Структура таблицы BM_TARIFF (наши тарифы для клиентов)
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
  AND TABLE_NAME = 'BM_TARIFF'
ORDER BY COLUMN_ID;

PROMPT
PROMPT ============================================================================
PROMPT 2. Комментарии к таблице BM_TARIFF
PROMPT ============================================================================

SELECT 
    COLUMN_NAME,
    COMMENTS
FROM ALL_COL_COMMENTS
WHERE OWNER = 'BILLING7'
  AND TABLE_NAME = 'BM_TARIFF'
ORDER BY COLUMN_NAME;

PROMPT
PROMPT ============================================================================
PROMPT 3. Структура таблицы BM_SERVICE_TARIFF (связь услуг с тарифами)
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
  AND TABLE_NAME = 'BM_SERVICE_TARIFF'
ORDER BY COLUMN_ID;

PROMPT
PROMPT ============================================================================
PROMPT 4. Комментарии к таблице BM_SERVICE_TARIFF
PROMPT ============================================================================

SELECT 
    COLUMN_NAME,
    COMMENTS
FROM ALL_COL_COMMENTS
WHERE OWNER = 'BILLING7'
  AND TABLE_NAME = 'BM_SERVICE_TARIFF'
ORDER BY COLUMN_NAME;

PROMPT
PROMPT ============================================================================
PROMPT 5. Примеры данных из BM_TARIFF (первые 5 записей)
PROMPT ============================================================================

SELECT * FROM BM_TARIFF
WHERE ROWNUM <= 5;

PROMPT
PROMPT ============================================================================
PROMPT 6. Примеры данных из BM_SERVICE_TARIFF для TYPE_ID = 9002 (первые 5 записей)
PROMPT ============================================================================

SELECT st.* 
FROM BM_SERVICE_TARIFF st
JOIN SERVICES s ON st.SERVICE_ID = s.SERVICE_ID
WHERE s.TYPE_ID = 9002
  AND ROWNUM <= 5;

PROMPT
PROMPT ============================================================================
PROMPT 7. Структура таблицы BM_TARIFFEL (тарифные элементы)
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
  AND TABLE_NAME = 'BM_TARIFFEL'
ORDER BY COLUMN_ID
FETCH FIRST 30 ROWS ONLY;

PROMPT
PROMPT ============================================================================
PROMPT 8. Примеры данных из BM_TARIFFEL (первые 5 записей)
PROMPT ============================================================================

SELECT * FROM BM_TARIFFEL
WHERE ROWNUM <= 5;

PROMPT
PROMPT ============================================================================
PROMPT 9. Связь между SERVICES.TARIFF_ID и BM_TARIFF
PROMPT ============================================================================

SELECT 
    s.SERVICE_ID,
    s.LOGIN,
    s.TYPE_ID,
    s.TARIFF_ID,
    t.TARIFF_ID as BM_TARIFF_ID,
    t.NAME as TARIFF_NAME
FROM SERVICES s
LEFT JOIN BM_TARIFF t ON s.TARIFF_ID = t.TARIFF_ID
WHERE s.TYPE_ID = 9002
  AND ROWNUM <= 5;

PROMPT
PROMPT ============================================================================
PROMPT Изучение завершено
PROMPT ============================================================================

SET FEEDBACK ON






