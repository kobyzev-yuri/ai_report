-- ============================================================================
-- Изучение таблиц BM_SERVICE_MONEY и BM_CDR_ACCT - основа для формирования ANALYTICS
-- ============================================================================

SET LINESIZE 200
SET PAGESIZE 1000
SET FEEDBACK OFF

PROMPT ============================================================================
PROMPT 1. Структура таблицы BM_SERVICE_MONEY (сессии услуг с объемами трафика и начислениями)
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
  AND TABLE_NAME = 'BM_SERVICE_MONEY'
ORDER BY COLUMN_ID
FETCH FIRST 50 ROWS ONLY;

PROMPT
PROMPT ============================================================================
PROMPT 2. Комментарии к таблице BM_SERVICE_MONEY
PROMPT ============================================================================

SELECT 
    COLUMN_NAME,
    COMMENTS
FROM ALL_COL_COMMENTS
WHERE OWNER = 'BILLING7'
  AND TABLE_NAME = 'BM_SERVICE_MONEY'
ORDER BY COLUMN_NAME
FETCH FIRST 30 ROWS ONLY;

PROMPT
PROMPT ============================================================================
PROMPT 3. Структура таблицы BM_CDR_ACCT (таблица трафика)
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
  AND TABLE_NAME = 'BM_CDR_ACCT'
ORDER BY COLUMN_ID
FETCH FIRST 50 ROWS ONLY;

PROMPT
PROMPT ============================================================================
PROMPT 4. Комментарии к таблице BM_CDR_ACCT
PROMPT ============================================================================

SELECT 
    COLUMN_NAME,
    COMMENTS
FROM ALL_COL_COMMENTS
WHERE OWNER = 'BILLING7'
  AND TABLE_NAME = 'BM_CDR_ACCT'
ORDER BY COLUMN_NAME
FETCH FIRST 30 ROWS ONLY;

PROMPT
PROMPT ============================================================================
PROMPT 5. Примеры данных из BM_SERVICE_MONEY для Iridium услуг (первые 5 записей)
PROMPT ============================================================================

SELECT sm.* 
FROM BM_SERVICE_MONEY sm
JOIN SERVICES s ON sm.SERVICE_ID = s.SERVICE_ID
WHERE s.TYPE_ID IN (9002, 9005, 9008, 9013, 9014)
  AND ROWNUM <= 5;

PROMPT
PROMPT ============================================================================
PROMPT 6. Примеры данных из BM_CDR_ACCT для Iridium услуг (первые 5 записей)
PROMPT ============================================================================

SELECT cdr.* 
FROM BM_CDR_ACCT cdr
JOIN SERVICES s ON cdr.SERVICE_ID = s.SERVICE_ID
WHERE s.TYPE_ID IN (9002, 9005, 9008, 9013, 9014)
  AND ROWNUM <= 5;

PROMPT
PROMPT ============================================================================
PROMPT 7. Связь BM_SERVICE_MONEY с ANALYTICS
PROMPT ============================================================================

SELECT 
    sm.SERVICE_MONEY_ID,
    sm.SERVICE_ID,
    sm.MONEY as SERVICE_MONEY,
    a.AID,
    a.MONEY as ANALYTICS_MONEY
FROM BM_SERVICE_MONEY sm
LEFT JOIN ANALYTICS a ON sm.SERVICE_MONEY_ID = a.SERVICE_MONEY_ID
WHERE sm.SERVICE_ID IN (
    SELECT SERVICE_ID FROM SERVICES WHERE TYPE_ID IN (9002, 9005, 9008, 9013, 9014) AND ROWNUM <= 100
)
AND ROWNUM <= 5;

PROMPT
PROMPT ============================================================================
PROMPT 8. Связь BM_CDR_ACCT с BM_SERVICE_MONEY
PROMPT ============================================================================

SELECT 
    cdr.CDR_ID,
    cdr.SERVICE_ID,
    cdr.BYTES,
    sm.SERVICE_MONEY_ID,
    sm.MONEY
FROM BM_CDR_ACCT cdr
LEFT JOIN BM_SERVICE_MONEY sm ON cdr.CDR_ID = sm.CDR_ID
WHERE cdr.SERVICE_ID IN (
    SELECT SERVICE_ID FROM SERVICES WHERE TYPE_ID IN (9002, 9005, 9008, 9013, 9014) AND ROWNUM <= 100
)
AND ROWNUM <= 5;

PROMPT
PROMPT ============================================================================
PROMPT Изучение завершено
PROMPT ============================================================================

SET FEEDBACK ON


