-- ============================================================================
-- Этап 2: Создание кэша для маппинга планов
-- Приоритет: Средний (средний эффект, средний риск)
-- Ожидаемый эффект: 50-70% ускорение для представлений с маппингом
-- ============================================================================

SET SQLBLANKLINES ON
SET DEFINE OFF

PROMPT ============================================================================
PROMPT Создание таблицы для кэширования маппинга планов
PROMPT ============================================================================

-- Таблица для кэширования маппинга планов
CREATE TABLE PLAN_MAPPING_CACHE (
    MAPPING_TYPE VARCHAR2(10) NOT NULL,  -- 'CONTRACT' или 'IMEI'
    KEY_VALUE VARCHAR2(50) NOT NULL,     -- CONTRACT_ID или IMEI
    PLAN_NAME VARCHAR2(100) NOT NULL,
    LAST_UPDATED DATE DEFAULT SYSDATE,
    CONSTRAINT PK_PLAN_MAPPING_CACHE PRIMARY KEY (MAPPING_TYPE, KEY_VALUE)
);

-- Индекс для быстрого поиска
CREATE INDEX IDX_PLAN_MAPPING_LOOKUP ON PLAN_MAPPING_CACHE(MAPPING_TYPE, KEY_VALUE);

-- Комментарии
COMMENT ON TABLE PLAN_MAPPING_CACHE IS 'Кэш для маппинга планов по CONTRACT_ID и IMEI из SPNET_TRAFFIC';
COMMENT ON COLUMN PLAN_MAPPING_CACHE.MAPPING_TYPE IS 'Тип маппинга: CONTRACT или IMEI';
COMMENT ON COLUMN PLAN_MAPPING_CACHE.KEY_VALUE IS 'Значение ключа: CONTRACT_ID или IMEI';
COMMENT ON COLUMN PLAN_MAPPING_CACHE.PLAN_NAME IS 'Название тарифного плана (самый частый для данного ключа)';
COMMENT ON COLUMN PLAN_MAPPING_CACHE.LAST_UPDATED IS 'Дата последнего обновления записи';

PROMPT ============================================================================
PROMPT Создание процедуры обновления кэша
PROMPT ============================================================================

CREATE OR REPLACE PROCEDURE REFRESH_PLAN_MAPPING AS
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_contract_count NUMBER;
    v_imei_count NUMBER;
BEGIN
    v_start_time := SYSTIMESTAMP;
    
    PROMPT Начало обновления кэша маппинга планов...
    
    -- Очистка старых данных
    DELETE FROM PLAN_MAPPING_CACHE;
    PROMPT Старые данные удалены
    
    -- Заполнение по CONTRACT_ID
    INSERT INTO PLAN_MAPPING_CACHE (MAPPING_TYPE, KEY_VALUE, PLAN_NAME, LAST_UPDATED)
    SELECT 'CONTRACT', CONTRACT_ID, PLAN_NAME, SYSDATE
    FROM (
        SELECT 
            CONTRACT_ID, 
            PLAN_NAME,
            ROW_NUMBER() OVER (PARTITION BY CONTRACT_ID ORDER BY COUNT(*) DESC, PLAN_NAME) AS rn
        FROM SPNET_TRAFFIC
        WHERE CONTRACT_ID IS NOT NULL 
          AND PLAN_NAME IS NOT NULL 
          AND PLAN_NAME != ''
        GROUP BY CONTRACT_ID, PLAN_NAME
    ) 
    WHERE rn = 1;
    
    v_contract_count := SQL%ROWCOUNT;
    PROMPT Заполнено записей по CONTRACT_ID: v_contract_count
    
    -- Заполнение по IMEI
    INSERT INTO PLAN_MAPPING_CACHE (MAPPING_TYPE, KEY_VALUE, PLAN_NAME, LAST_UPDATED)
    SELECT 'IMEI', IMEI, PLAN_NAME, SYSDATE
    FROM (
        SELECT 
            IMEI, 
            PLAN_NAME,
            ROW_NUMBER() OVER (PARTITION BY IMEI ORDER BY COUNT(*) DESC, PLAN_NAME) AS rn
        FROM SPNET_TRAFFIC
        WHERE IMEI IS NOT NULL 
          AND PLAN_NAME IS NOT NULL 
          AND PLAN_NAME != ''
        GROUP BY IMEI, PLAN_NAME
    ) 
    WHERE rn = 1;
    
    v_imei_count := SQL%ROWCOUNT;
    PROMPT Заполнено записей по IMEI: v_imei_count
    
    COMMIT;
    
    v_end_time := SYSTIMESTAMP;
    PROMPT Обновление кэша завершено за: TO_CHAR(EXTRACT(SECOND FROM (v_end_time - v_start_time)), '999.99') секунд
    PROMPT Всего записей в кэше: v_contract_count + v_imei_count
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        PROMPT Ошибка при обновлении кэша: SQLERRM
        RAISE;
END;
/

PROMPT ============================================================================
PROMPT Создание функции для получения плана из кэша
PROMPT ============================================================================

CREATE OR REPLACE FUNCTION GET_PLAN_FROM_CACHE(
    p_mapping_type VARCHAR2,
    p_key_value VARCHAR2
) RETURN VARCHAR2
DETERMINISTIC
AS
    v_plan_name VARCHAR2(100);
BEGIN
    SELECT PLAN_NAME
    INTO v_plan_name
    FROM PLAN_MAPPING_CACHE
    WHERE MAPPING_TYPE = p_mapping_type
      AND KEY_VALUE = p_key_value
      AND ROWNUM = 1;
    
    RETURN v_plan_name;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
END;
/

PROMPT ============================================================================
PROMPT Кэш маппинга планов создан успешно!
PROMPT ============================================================================
PROMPT 
PROMPT Следующие шаги:
PROMPT 1. Заполнить кэш: EXEC REFRESH_PLAN_MAPPING;
PROMPT 2. Проверить данные: SELECT COUNT(*) FROM PLAN_MAPPING_CACHE;
PROMPT 3. Настроить автоматическое обновление (через DBMS_SCHEDULER или при загрузке данных)
PROMPT 4. Модифицировать представления для использования кэша
PROMPT ============================================================================

SET DEFINE ON

