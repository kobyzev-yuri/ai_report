# План оптимизации представлений Oracle

## Анализ текущих проблем производительности

### 1. V_CONSOLIDATED_OVERAGE_REPORT

#### Проблемы:
- **contract_plan_mapping** и **imei_plan_mapping**: Сканируют всю таблицу `SPNET_TRAFFIC` без фильтров
- **steccom_data**: Использует `UPPER(TRIM(se.SERVICE))` в WHERE - не использует индекс
- **FULL OUTER JOIN**: Может быть медленным на больших объемах
- Повторяющиеся вычисления `CASE` в GROUP BY и SELECT

#### Узкие места:
1. `contract_plan_mapping` - полное сканирование SPNET_TRAFFIC
2. `imei_plan_mapping` - полное сканирование SPNET_TRAFFIC  
3. `steccom_data` - функция в WHERE блокирует индекс
4. Дублирование CASE выражений в GROUP BY

### 2. V_CONSOLIDATED_REPORT_WITH_BILLING

#### Проблемы:
- **unique_steccom_expenses**: ROW_NUMBER() по всей таблице `STECCOM_EXPENSES`
- **steccom_fees**: Множественные CASE WHEN с LIKE в агрегации
- Подзапрос `cor` группирует весь `V_CONSOLIDATED_OVERAGE_REPORT`
- Два LEFT JOIN к `steccom_fees` с вычислениями в ON (не используют индексы)
- CASE выражения в JOIN условиях

#### Узкие места:
1. `unique_steccom_expenses` - ROW_NUMBER() по всей таблице
2. Два JOIN к `steccom_fees` с вычислениями в ON
3. Подзапрос `cor` - дополнительная группировка

---

## План оптимизации (по приоритетам)

### Приоритет 1: Индексы (быстрый эффект, низкий риск)

#### 1.1 Составные индексы для частых JOIN

```sql
-- Для STECCOM_EXPENSES - частые JOIN по (CONTRACT_ID, IMEI, INVOICE_DATE)
CREATE INDEX IDX_STECCOM_COMPOSITE_JOIN ON STECCOM_EXPENSES(
    CONTRACT_ID, 
    ICC_ID_IMEI, 
    INVOICE_DATE
);

-- Для STECCOM_EXPENSES - фильтрация по SERVICE
CREATE INDEX IDX_STECCOM_SERVICE_FILTER ON STECCOM_EXPENSES(
    SERVICE, 
    CONTRACT_ID, 
    ICC_ID_IMEI
) WHERE SERVICE IS NULL OR UPPER(TRIM(SERVICE)) != 'BROADBAND';

-- Функциональный индекс для BILL_MONTH (YYYYMM)
CREATE INDEX IDX_STECCOM_BILL_MONTH ON STECCOM_EXPENSES(
    TO_CHAR(INVOICE_DATE, 'YYYYMM'),
    CONTRACT_ID,
    ICC_ID_IMEI
);

-- Для SPNET_TRAFFIC - маппинг планов
CREATE INDEX IDX_SPNET_PLAN_MAPPING ON SPNET_TRAFFIC(
    CONTRACT_ID, 
    PLAN_NAME
) WHERE CONTRACT_ID IS NOT NULL AND PLAN_NAME IS NOT NULL AND PLAN_NAME != '';

CREATE INDEX IDX_SPNET_IMEI_PLAN_MAPPING ON SPNET_TRAFFIC(
    IMEI, 
    PLAN_NAME
) WHERE IMEI IS NOT NULL AND PLAN_NAME IS NOT NULL AND PLAN_NAME != '';
```

**Ожидаемый эффект**: 30-50% ускорение JOIN операций

---

### Приоритет 2: Оптимизация CTE (средний эффект, средний риск)

#### 2.1 Кэширование маппинга планов

**Проблема**: `contract_plan_mapping` и `imei_plan_mapping` пересчитываются каждый раз

**Решение**: Создать материализованные представления или таблицы

```sql
-- Создать таблицу для кэширования маппинга планов
CREATE TABLE PLAN_MAPPING_CACHE (
    MAPPING_TYPE VARCHAR2(10), -- 'CONTRACT' или 'IMEI'
    KEY_VALUE VARCHAR2(50),   -- CONTRACT_ID или IMEI
    PLAN_NAME VARCHAR2(100),
    LAST_UPDATED DATE DEFAULT SYSDATE,
    PRIMARY KEY (MAPPING_TYPE, KEY_VALUE)
);

-- Индекс для быстрого поиска
CREATE INDEX IDX_PLAN_MAPPING_LOOKUP ON PLAN_MAPPING_CACHE(MAPPING_TYPE, KEY_VALUE);

-- Процедура обновления (запускать периодически или при загрузке данных)
CREATE OR REPLACE PROCEDURE REFRESH_PLAN_MAPPING AS
BEGIN
    -- Очистка старых данных
    DELETE FROM PLAN_MAPPING_CACHE;
    
    -- Заполнение по CONTRACT_ID
    INSERT INTO PLAN_MAPPING_CACHE (MAPPING_TYPE, KEY_VALUE, PLAN_NAME)
    SELECT 'CONTRACT', CONTRACT_ID, PLAN_NAME
    FROM (
        SELECT CONTRACT_ID, PLAN_NAME,
               ROW_NUMBER() OVER (PARTITION BY CONTRACT_ID ORDER BY COUNT(*) DESC, PLAN_NAME) AS rn
        FROM SPNET_TRAFFIC
        WHERE CONTRACT_ID IS NOT NULL AND PLAN_NAME IS NOT NULL AND PLAN_NAME != ''
        GROUP BY CONTRACT_ID, PLAN_NAME
    ) WHERE rn = 1;
    
    -- Заполнение по IMEI
    INSERT INTO PLAN_MAPPING_CACHE (MAPPING_TYPE, KEY_VALUE, PLAN_NAME)
    SELECT 'IMEI', IMEI, PLAN_NAME
    FROM (
        SELECT IMEI, PLAN_NAME,
               ROW_NUMBER() OVER (PARTITION BY IMEI ORDER BY COUNT(*) DESC, PLAN_NAME) AS rn
        FROM SPNET_TRAFFIC
        WHERE IMEI IS NOT NULL AND PLAN_NAME IS NOT NULL AND PLAN_NAME != ''
        GROUP BY IMEI, PLAN_NAME
    ) WHERE rn = 1;
    
    COMMIT;
END;
/
```

**Ожидаемый эффект**: 50-70% ускорение для представлений с маппингом

---

#### 2.2 Оптимизация unique_steccom_expenses

**Проблема**: ROW_NUMBER() по всей таблице STECCOM_EXPENSES

**Решение**: Использовать DISTINCT или предварительно отфильтровать

```sql
-- Вместо ROW_NUMBER() использовать DISTINCT в CTE
unique_steccom_expenses AS (
    SELECT DISTINCT
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS invoice_month,
        se.CONTRACT_ID,
        se.ICC_ID_IMEI,
        UPPER(TRIM(se.DESCRIPTION)) AS description,
        se.AMOUNT,
        se.TRANSACTION_DATE,
        -- Остальные поля берем через MAX/MIN для группировки
        MAX(se.ID) AS id
    FROM STECCOM_EXPENSES se
    WHERE se.CONTRACT_ID IS NOT NULL
      AND se.ICC_ID_IMEI IS NOT NULL
      AND se.INVOICE_DATE IS NOT NULL
      AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
    GROUP BY 
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
        se.CONTRACT_ID,
        se.ICC_ID_IMEI,
        UPPER(TRIM(se.DESCRIPTION)),
        se.AMOUNT,
        se.TRANSACTION_DATE
)
```

**Ожидаемый эффект**: 20-30% ускорение для steccom_fees

---

### Приоритет 3: Оптимизация JOIN (высокий эффект, высокий риск)

#### 3.1 Предвычисление BILL_MONTH для JOIN

**Проблема**: CASE выражения в JOIN условиях не используют индексы

**Решение**: Вычислять BILL_MONTH заранее в CTE

```sql
-- В 04_v_consolidated_report_with_billing.sql
FROM (
    SELECT 
        IMEI,
        CONTRACT_ID,
        BILL_MONTH,
        -- Предвычисляем BILL_MONTH_YYYMM для JOIN
        CASE 
            WHEN BILL_MONTH IS NOT NULL AND LENGTH(TRIM(BILL_MONTH)) >= 6 THEN
                SUBSTR(TRIM(BILL_MONTH), 1, 6)
            ELSE BILL_MONTH
        END AS BILL_MONTH_YYYMM,
        -- Предвычисляем предыдущий месяц для JOIN
        CASE 
            WHEN BILL_MONTH IS NOT NULL AND LENGTH(TRIM(BILL_MONTH)) >= 6 THEN
                TO_CHAR(ADD_MONTHS(TO_DATE(SUBSTR(TRIM(BILL_MONTH), 1, 6), 'YYYYMM'), -1), 'YYYYMM')
            ELSE NULL
        END AS PREV_BILL_MONTH_YYYMM,
        -- Остальные поля...
    FROM V_CONSOLIDATED_OVERAGE_REPORT
    GROUP BY IMEI, CONTRACT_ID, BILL_MONTH
) cor
LEFT JOIN steccom_fees sf
    ON cor.BILL_MONTH_YYYMM = sf.bill_month  -- Простое сравнение, использует индекс
    AND RTRIM(cor.CONTRACT_ID) = RTRIM(sf.CONTRACT_ID)
    AND cor.IMEI = sf.imei
LEFT JOIN steccom_fees sf_prev
    ON sf_prev.bill_month = cor.PREV_BILL_MONTH_YYYMM  -- Простое сравнение
    AND RTRIM(cor.CONTRACT_ID) = RTRIM(sf_prev.CONTRACT_ID)
    AND cor.IMEI = sf_prev.imei
```

**Ожидаемый эффект**: 40-60% ускорение JOIN операций

---

#### 3.2 Оптимизация steccom_fees агрегации

**Проблема**: Множественные CASE WHEN с LIKE в агрегации

**Решение**: Использовать DECODE или предварительно классифицировать типы fees

```sql
-- Создать функцию для классификации типа fee
CREATE OR REPLACE FUNCTION GET_FEE_TYPE(p_description VARCHAR2) RETURN VARCHAR2 AS
BEGIN
    IF UPPER(TRIM(p_description)) LIKE '%ACTIVATION%' OR UPPER(TRIM(p_description)) = 'ACTIVATION FEE' THEN
        RETURN 'ACTIVATION_FEE';
    ELSIF UPPER(TRIM(p_description)) LIKE '%ADVANCE CHARGE%' OR UPPER(TRIM(p_description)) = 'ADVANCE CHARGE' THEN
        RETURN 'ADVANCE_CHARGE';
    ELSIF UPPER(TRIM(p_description)) LIKE '%CREDIT%' AND UPPER(TRIM(p_description)) NOT LIKE '%CREDITED%' THEN
        RETURN 'CREDIT';
    ELSIF UPPER(TRIM(p_description)) LIKE '%CREDITED%' THEN
        RETURN 'CREDITED';
    ELSIF UPPER(TRIM(p_description)) LIKE '%PRORATED%' OR UPPER(TRIM(p_description)) = 'PRORATED' THEN
        RETURN 'PRORATED';
    ELSE
        RETURN 'OTHER';
    END IF;
END;
/

-- Использовать в CTE с предварительной классификацией
steccom_fees AS (
    SELECT 
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS bill_month,
        se.CONTRACT_ID,
        se.ICC_ID_IMEI AS imei,
        SUM(CASE WHEN GET_FEE_TYPE(se.DESCRIPTION) = 'ACTIVATION_FEE' THEN se.AMOUNT ELSE 0 END) AS fee_activation_fee,
        SUM(CASE WHEN GET_FEE_TYPE(se.DESCRIPTION) = 'ADVANCE_CHARGE' THEN se.AMOUNT ELSE 0 END) AS fee_advance_charge,
        -- и т.д.
    FROM unique_steccom_expenses se
    WHERE se.rn = 1
    GROUP BY TO_CHAR(se.INVOICE_DATE, 'YYYYMM'), se.CONTRACT_ID, se.ICC_ID_IMEI
)
```

**Альтернатива**: Добавить колонку FEE_TYPE в таблицу STECCOM_EXPENSES при загрузке

**Ожидаемый эффект**: 15-25% ускорение агрегации

---

### Приоритет 4: Материализованные представления (высокий эффект, требует обслуживания)

#### 4.1 Материализованное представление для steccom_fees

```sql
CREATE MATERIALIZED VIEW MV_STECCOM_FEES
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
ENABLE QUERY REWRITE
AS
SELECT 
    TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS bill_month,
    se.CONTRACT_ID,
    se.ICC_ID_IMEI AS imei,
    SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ACTIVATION%' OR UPPER(TRIM(se.DESCRIPTION)) = 'ACTIVATION FEE' THEN se.AMOUNT ELSE 0 END) AS fee_activation_fee,
    SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' OR UPPER(TRIM(se.DESCRIPTION)) = 'ADVANCE CHARGE' THEN se.AMOUNT ELSE 0 END) AS fee_advance_charge,
    SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%CREDIT%' AND UPPER(TRIM(se.DESCRIPTION)) NOT LIKE '%CREDITED%' THEN se.AMOUNT ELSE 0 END) AS fee_credit,
    SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%CREDITED%' THEN se.AMOUNT ELSE 0 END) AS fee_credited,
    SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%PRORATED%' OR UPPER(TRIM(se.DESCRIPTION)) = 'PRORATED' THEN se.AMOUNT ELSE 0 END) AS fee_prorated,
    SUM(se.AMOUNT) AS fees_total
FROM STECCOM_EXPENSES se
WHERE se.CONTRACT_ID IS NOT NULL
  AND se.ICC_ID_IMEI IS NOT NULL
  AND se.INVOICE_DATE IS NOT NULL
  AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
GROUP BY 
    TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
    se.CONTRACT_ID, 
    se.ICC_ID_IMEI;

-- Индексы для материализованного представления
CREATE INDEX IDX_MV_STECCOM_FEES_JOIN ON MV_STECCOM_FEES(bill_month, CONTRACT_ID, imei);
CREATE INDEX IDX_MV_STECCOM_FEES_BILL_MONTH ON MV_STECCOM_FEES(bill_month);
```

**Ожидаемый эффект**: 70-90% ускорение для запросов с fees

**Недостаток**: Требует обновления при загрузке новых данных

---

## План выполнения (поэтапно)

### Этап 1: Быстрые победы (1-2 дня)
1. ✅ Создать составные индексы (Приоритет 1.1)
2. ✅ Создать функциональный индекс для BILL_MONTH (Приоритет 1.1)
3. ✅ Протестировать на тестовой базе

**Ожидаемый результат**: 30-50% улучшение производительности

---

### Этап 2: Оптимизация CTE (3-5 дней)
1. ✅ Создать таблицу PLAN_MAPPING_CACHE (Приоритет 2.1)
2. ✅ Создать процедуру REFRESH_PLAN_MAPPING (Приоритет 2.1)
3. ✅ Модифицировать представления для использования кэша
4. ✅ Оптимизировать unique_steccom_expenses (Приоритет 2.2)
5. ✅ Протестировать и сравнить производительность

**Ожидаемый результат**: Дополнительно 20-30% улучшение

---

### Этап 3: Оптимизация JOIN (5-7 дней)
1. ✅ Предвычислить BILL_MONTH в CTE (Приоритет 3.1)
2. ✅ Упростить JOIN условия
3. ✅ Оптимизировать steccom_fees агрегацию (Приоритет 3.2)
4. ✅ Протестировать на production-like данных

**Ожидаемый результат**: Дополнительно 30-40% улучшение

---

### Этап 4: Материализованные представления (опционально, 7-10 дней)
1. ⚠️ Создать MV_STECCOM_FEES (Приоритет 4.1)
2. ⚠️ Настроить автоматическое обновление
3. ⚠️ Модифицировать представления для использования MV
4. ⚠️ Мониторинг производительности

**Ожидаемый результат**: Дополнительно 50-70% улучшение (но требует обслуживания)

---

## Метрики для оценки эффективности

### До оптимизации (базовая линия):
- Время выполнения `V_CONSOLIDATED_REPORT_WITH_BILLING` для одного периода: ~X секунд
- Время выполнения для всех периодов: ~Y секунд
- Размер execution plan: проверить через EXPLAIN PLAN

### После каждого этапа:
- Измерить время выполнения тех же запросов
- Проверить execution plan (должно быть меньше TABLE ACCESS FULL)
- Мониторить использование CPU и I/O

---

## Риски и митигация

### Риск 1: Индексы увеличивают время INSERT/UPDATE
**Митигация**: 
- Создавать индексы в нерабочее время
- Мониторить время загрузки данных
- Использовать ONLINE создание индексов

### Риск 2: Кэш PLAN_MAPPING_CACHE устаревает
**Митигация**:
- Автоматическое обновление при загрузке данных
- Периодическое обновление (например, раз в час)
- Версионирование кэша

### Риск 3: Материализованные представления требуют обслуживания
**Митигация**:
- Автоматическое обновление через DBMS_SCHEDULER
- Мониторинг актуальности данных
- Fallback на обычные представления при проблемах

---

## Рекомендации по мониторингу

1. **Включить SQL tracing** для медленных запросов:
```sql
ALTER SESSION SET SQL_TRACE = TRUE;
ALTER SESSION SET TIMED_STATISTICS = TRUE;
```

2. **Использовать AWR/Statspack** для анализа производительности

3. **Мониторить использование индексов**:
```sql
SELECT index_name, num_rows, distinct_keys, clustering_factor
FROM user_indexes
WHERE table_name IN ('STECCOM_EXPENSES', 'SPNET_TRAFFIC');
```

4. **Проверять execution plans** регулярно:
```sql
EXPLAIN PLAN FOR <your_query>;
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);
```

