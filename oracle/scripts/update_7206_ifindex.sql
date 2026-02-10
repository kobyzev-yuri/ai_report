не нужно -- ============================================================================
-- План UPDATE для замены MAC адресов в services_ext
-- Заменяет MAC адреса из working на spare для услуг типа 10 с dict_id=14009
-- База данных: Oracle (billing7@bm7)
-- ============================================================================
-- 
-- ВАЖНО: Перед выполнением UPDATE рекомендуется:
-- 1. Создать резервную копию данных
-- 2. Проверить VIEW V_7206_IFINDEX_REPLACEMENT для просмотра изменений
-- 3. Выполнить UPDATE в транзакции с возможностью отката
--
-- ============================================================================

SET SQLBLANKLINES ON
SET DEFINE OFF

-- Шаг 1: Проверка количества записей для обновления
PROMPT ========================================
PROMPT Шаг 1: Проверка количества записей
PROMPT ========================================
SELECT 
    COUNT(*) AS TOTAL_RECORDS,
    COUNT(DISTINCT SERVICE_ID) AS UNIQUE_SERVICES
FROM SERVICES_EXT se
JOIN SERVICES s ON se.SERVICE_ID = s.SERVICE_ID
WHERE se.DATE_END IS NULL
  AND se.DICT_ID = 14009
  AND s.TYPE_ID = 10
  AND EXISTS (
      SELECT 1 FROM V_7206_IFINDEX_MAPPING m 
      WHERE se.VALUE LIKE '%mac ' || m.working_mac || '%'
  );

-- Шаг 2: Просмотр примеров изменений через VIEW
PROMPT ========================================
PROMPT Шаг 2: Примеры изменений (первые 10 записей)
PROMPT ========================================
SELECT 
    SERVICES_EXT_ID,
    SERVICE_ID,
    SUBSTR(original_value, 1, 80) AS OLD_VALUE,
    SUBSTR(new_value, 1, 80) AS NEW_VALUE
FROM (
    SELECT 
        se.SERVICES_EXT_ID,
        se.SERVICE_ID,
        se.VALUE AS original_value,
        (SELECT new_value FROM (
            SELECT rv.new_value, rv.iteration
            FROM (
                WITH mac_replacements AS (
                    SELECT 
                        se2.SERVICES_EXT_ID,
                        se2.VALUE AS original_value,
                        m.working_mac,
                        m.spare_mac,
                        ROW_NUMBER() OVER (PARTITION BY se2.SERVICES_EXT_ID ORDER BY m.working_index) AS rn
                    FROM SERVICES_EXT se2
                    JOIN V_7206_IFINDEX_MAPPING m ON se2.VALUE LIKE '%mac ' || m.working_mac || '%'
                    WHERE se2.SERVICES_EXT_ID = se.SERVICES_EXT_ID
                ),
                replaced_values AS (
                    SELECT 
                        SERVICES_EXT_ID,
                        original_value AS current_value,
                        working_mac,
                        spare_mac,
                        REPLACE(original_value, 'mac ' || working_mac, 'mac ' || spare_mac) AS new_value,
                        rn,
                        1 AS iteration
                    FROM mac_replacements
                    WHERE rn = 1
                    UNION ALL
                    SELECT 
                        rv.SERVICES_EXT_ID,
                        rv.new_value AS current_value,
                        mr.working_mac,
                        mr.spare_mac,
                        REPLACE(rv.new_value, 'mac ' || mr.working_mac, 'mac ' || mr.spare_mac) AS new_value,
                        mr.rn,
                        rv.iteration + 1
                    FROM replaced_values rv
                    JOIN mac_replacements mr ON rv.SERVICES_EXT_ID = mr.SERVICES_EXT_ID AND mr.rn = rv.iteration + 1
                )
                SELECT new_value, iteration FROM replaced_values
                ORDER BY iteration DESC
            ) rv WHERE ROWNUM = 1
        )) AS new_value
    FROM SERVICES_EXT se
    JOIN SERVICES s ON se.SERVICE_ID = s.SERVICE_ID
    WHERE se.DATE_END IS NULL
      AND se.DICT_ID = 14009
      AND s.TYPE_ID = 10
      AND EXISTS (
          SELECT 1 FROM V_7206_IFINDEX_MAPPING m 
          WHERE se.VALUE LIKE '%mac ' || m.working_mac || '%'
      )
) WHERE ROWNUM <= 10;

-- Шаг 3: UPDATE записей (выполнять только после проверки!)
PROMPT ========================================
PROMPT Шаг 3: UPDATE записей
PROMPT ========================================
PROMPT ВНИМАНИЕ: Этот UPDATE изменяет данные в таблице SERVICES_EXT!
PROMPT Рекомендуется выполнить в транзакции с возможностью отката.
PROMPT 

-- Начинаем транзакцию
-- BEGIN;

-- UPDATE с использованием рекурсивной замены всех MAC адресов
MERGE INTO SERVICES_EXT se
USING (
    WITH mac_replacements AS (
        SELECT 
            se2.SERVICES_EXT_ID,
            se2.VALUE AS original_value,
            m.working_mac,
            m.spare_mac,
            ROW_NUMBER() OVER (PARTITION BY se2.SERVICES_EXT_ID ORDER BY m.working_index) AS rn
        FROM SERVICES_EXT se2
        JOIN SERVICES s2 ON se2.SERVICE_ID = s2.SERVICE_ID
        JOIN V_7206_IFINDEX_MAPPING m ON se2.VALUE LIKE '%mac ' || m.working_mac || '%'
        WHERE se2.DATE_END IS NULL
          AND se2.DICT_ID = 14009
          AND s2.TYPE_ID = 10
    ),
    replaced_values AS (
        еще не завершил настройку фильтраSELECT 
            SERVICES_EXT_ID,
            original_value AS current_value,
            working_mac,
            spare_mac,
            REPLACE(original_value, 'mac ' || working_mac, 'mac ' || spare_mac) AS new_value,
            rn,
            1 AS iteration
        FROM mac_replacements
        WHERE rn = 1
        UNION ALL
        SELECT 
            rv.SERVICES_EXT_ID,
            rv.new_value AS current_value,
            mr.working_mac,
            mr.spare_mac,
            REPLACE(rv.new_value, 'mac ' || mr.working_mac, 'mac ' || mr.spare_mac) AS new_value,
            mr.rn,
            rv.iteration + 1
        FROM replaced_values rv
        JOIN mac_replacements mr ON rv.SERVICES_EXT_ID = mr.SERVICES_EXT_ID AND mr.rn = rv.iteration + 1
    )
    SELECT 
        SERVICES_EXT_ID,
        new_value AS VALUE
    FROM replaced_values rv
    WHERE rv.iteration = (SELECT MAX(iteration) FROM replaced_values rv2 WHERE rv2.SERVICES_EXT_ID = rv.SERVICES_EXT_ID)
) new_values
ON (se.SERVICES_EXT_ID = new_values.SERVICES_EXT_ID)
WHEN MATCHED THEN
    UPDATE SET se.VALUE = new_values.VALUE;

-- Проверка результата
PROMPT ========================================
PROMPT Проверка результата UPDATE
PROMPT ========================================
SELECT 
    COUNT(*) AS UPDATED_RECORDS
FROM SERVICES_EXT se
JOIN SERVICES s ON se.SERVICE_ID = s.SERVICE_ID
WHERE se.DATE_END IS NULL
  AND se.DICT_ID = 14009
  AND s.TYPE_ID = 10
  AND EXISTS (
      SELECT 1 FROM V_7206_IFINDEX_MAPPING m 
      WHERE se.VALUE LIKE '%mac ' || m.spare_mac || '%'
  );

-- Если все правильно, закомментируйте следующую строку для коммита:
-- COMMIT;
-- Если нужно откатить изменения:
-- ROLLBACK;

PROMPT ========================================
PROMPT UPDATE завершен
PROMPT ========================================




















