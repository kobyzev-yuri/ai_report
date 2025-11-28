-- ============================================================================
-- Изучение структуры таблиц биллинга для базы знаний
-- Получение информации о таблицах: SERVICES, TARIFFS, CUSTOMERS, ACCOUNTS, BM_TYPE
-- ============================================================================

SET LINESIZE 200
SET PAGESIZE 1000
SET FEEDBACK OFF

PROMPT ============================================================================
PROMPT 1. Структура таблицы SERVICES
PROMPT ============================================================================

SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    DATA_LENGTH,
    DATA_PRECISION,
    DATA_SCALE,
    NULLABLE,
    DATA_DEFAULT
FROM ALL_TAB_COLUMNS
WHERE OWNER = 'BILLING7'
  AND TABLE_NAME = 'SERVICES'
ORDER BY COLUMN_ID;

PROMPT
PROMPT ============================================================================
PROMPT 2. Комментарии к таблице SERVICES
PROMPT ============================================================================

SELECT 
    COLUMN_NAME,
    COMMENTS
FROM ALL_COL_COMMENTS
WHERE OWNER = 'BILLING7'
  AND TABLE_NAME = 'SERVICES'
ORDER BY COLUMN_NAME;

PROMPT
PROMPT ============================================================================
PROMPT 3. Поиск таблиц с тарифами (TARIFF, TARIFFS, TARIFF_PLANS и т.д.)
PROMPT ============================================================================

SELECT 
    TABLE_NAME,
    NUM_ROWS
FROM ALL_TABLES
WHERE OWNER = 'BILLING7'
  AND UPPER(TABLE_NAME) LIKE '%TARIFF%'
ORDER BY TABLE_NAME;

PROMPT
PROMPT ============================================================================
PROMPT 4. Структура таблицы CUSTOMERS
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
  AND TABLE_NAME = 'CUSTOMERS'
ORDER BY COLUMN_ID;

PROMPT
PROMPT ============================================================================
PROMPT 5. Структура таблицы ACCOUNTS
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
  AND TABLE_NAME = 'ACCOUNTS'
ORDER BY COLUMN_ID;

PROMPT
PROMPT ============================================================================
PROMPT 6. Поиск таблицы BM_TYPE (справочник типов услуг)
PROMPT ============================================================================

SELECT 
    TABLE_NAME,
    NUM_ROWS
FROM ALL_TABLES
WHERE OWNER = 'BILLING7'
  AND UPPER(TABLE_NAME) LIKE '%TYPE%'
ORDER BY TABLE_NAME;

PROMPT
PROMPT ============================================================================
PROMPT 7. Структура BM_TYPE (если найдена)
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
  AND TABLE_NAME = 'BM_TYPE'
ORDER BY COLUMN_ID;

PROMPT
PROMPT ============================================================================
PROMPT 8. Примеры данных из SERVICES (первые 3 записи для TYPE_ID = 9002)
PROMPT ============================================================================

SELECT 
    SERVICE_ID,
    LOGIN,
    VSAT,
    TYPE_ID,
    TARIFF_ID,
    STATUS,
    ACCOUNT_ID,
    CUSTOMER_ID,
    CREATE_DATE,
    START_DATE,
    STOP_DATE
FROM SERVICES
WHERE TYPE_ID = 9002
  AND ROWNUM <= 3;

PROMPT
PROMPT ============================================================================
PROMPT 9. Примеры данных из BM_TYPE (если найдена)
PROMPT ============================================================================

SELECT * FROM BM_TYPE
WHERE TYPE_ID IN (9002, 9005, 9008, 9013, 9014)
ORDER BY TYPE_ID;

PROMPT
PROMPT ============================================================================
PROMPT 10. Поиск таблиц с ценами/стоимостью (PRICE, COST, CHARGE, AMOUNT)
PROMPT ============================================================================

SELECT 
    TABLE_NAME,
    NUM_ROWS
FROM ALL_TABLES
WHERE OWNER = 'BILLING7'
  AND (
    UPPER(TABLE_NAME) LIKE '%PRICE%'
    OR UPPER(TABLE_NAME) LIKE '%COST%'
    OR UPPER(TABLE_NAME) LIKE '%CHARGE%'
    OR UPPER(TABLE_NAME) LIKE '%TARIFF%'
  )
ORDER BY TABLE_NAME;

PROMPT
PROMPT ============================================================================
PROMPT Изучение завершено
PROMPT ============================================================================

SET FEEDBACK ON


