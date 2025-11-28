-- ============================================================================
-- Изучение структуры таблицы BM_ZONE - тарифные зоны
-- ============================================================================

SET LINESIZE 200
SET PAGESIZE 1000
SET FEEDBACK OFF

PROMPT ============================================================================
PROMPT 1. Структура таблицы BM_ZONE
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
  AND TABLE_NAME = 'BM_ZONE'
ORDER BY COLUMN_ID;

PROMPT
PROMPT ============================================================================
PROMPT 2. Комментарии к таблице BM_ZONE
PROMPT ============================================================================

SELECT 
    COLUMN_NAME,
    COMMENTS
FROM ALL_COL_COMMENTS
WHERE OWNER = 'BILLING7'
  AND TABLE_NAME = 'BM_ZONE'
ORDER BY COLUMN_NAME;

PROMPT
PROMPT ============================================================================
PROMPT 3. Примеры данных из BM_ZONE (первые 20 записей)
PROMPT ============================================================================

SELECT * FROM BM_ZONE
ORDER BY ZONE_ID
FETCH FIRST 20 ROWS ONLY;

PROMPT
PROMPT ============================================================================
PROMPT 4. Использование ZONE_ID в тарифных таблицах
PROMPT ============================================================================

SELECT 
    'BM_TARIFF' as table_name,
    COUNT(DISTINCT PH_ZONE_GROUP_ID) as ph_zones,
    COUNT(DISTINCT TR_ZONE_GROUP_ID) as tr_zones
FROM BM_TARIFF
UNION ALL
SELECT 
    'BM_TARIFFEL',
    COUNT(DISTINCT ZONE_ID),
    0
FROM BM_TARIFFEL
WHERE ZONE_ID IS NOT NULL;

PROMPT
PROMPT ============================================================================
PROMPT 5. Связь BM_ZONE с BM_TARIFFEL (тарифные элементы с фиксированной ценой зоны)
PROMPT ============================================================================

SELECT 
    z.ZONE_ID,
    z.NAME,
    z.MNEMONIC,
    COUNT(t.TARIFFEL_ID) as tariffel_count,
    MIN(t.MONEY) as min_price,
    MAX(t.MONEY) as max_price
FROM BM_ZONE z
LEFT JOIN BM_TARIFFEL t ON z.ZONE_ID = t.ZONE_ID
WHERE z.ZONE_ID IN (
    SELECT DISTINCT ZONE_ID FROM BM_TARIFFEL WHERE ZONE_ID IS NOT NULL
)
GROUP BY z.ZONE_ID, z.NAME, z.MNEMONIC
ORDER BY z.ZONE_ID
FETCH FIRST 10 ROWS ONLY;

PROMPT
PROMPT ============================================================================
PROMPT Изучение завершено
PROMPT ============================================================================

SET FEEDBACK ON


