# Оптимизация представлений Oracle

## Структура

- `01_create_indexes.sql` - Создание индексов (Этап 1)
- `02_create_plan_mapping_cache.sql` - Создание кэша маппинга планов (Этап 2)
- `README.md` - Этот файл

## Порядок выполнения

### Этап 1: Индексы (быстро, безопасно)

```bash
# Подключиться к Oracle
sqlplus billing7/billing@bm7

# Выполнить скрипт создания индексов
@optimization/01_create_indexes.sql

# Собрать статистику для оптимизатора
EXEC DBMS_STATS.GATHER_TABLE_STATS('BILLING7', 'STECCOM_EXPENSES');
EXEC DBMS_STATS.GATHER_TABLE_STATS('BILLING7', 'SPNET_TRAFFIC');

# Проверить созданные индексы
SELECT index_name, table_name, num_rows, distinct_keys 
FROM user_indexes 
WHERE index_name LIKE 'IDX_%' 
ORDER BY table_name, index_name;
```

**Время выполнения**: ~10-30 минут (зависит от размера таблиц)

**Риски**: Минимальные. Индексы могут замедлить INSERT/UPDATE, но ускоряют SELECT.

---

### Этап 2: Кэш маппинга планов (требует модификации представлений)

```bash
# Выполнить скрипт создания кэша
@optimization/02_create_plan_mapping_cache.sql

# Заполнить кэш
EXEC REFRESH_PLAN_MAPPING;

# Проверить данные
SELECT mapping_type, COUNT(*) as cnt 
FROM PLAN_MAPPING_CACHE 
GROUP BY mapping_type;

# Настроить автоматическое обновление (опционально)
BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name => 'REFRESH_PLAN_MAPPING_JOB',
        job_type => 'PLSQL_BLOCK',
        job_action => 'BEGIN REFRESH_PLAN_MAPPING; END;',
        start_date => SYSTIMESTAMP,
        repeat_interval => 'FREQ=HOURLY; INTERVAL=1',
        enabled => TRUE
    );
END;
/
```

**Время выполнения**: ~5-10 минут

**Риски**: Средние. Требует модификации представлений для использования кэша.

---

## Мониторинг производительности

### Проверка использования индексов

```sql
-- Статистика использования индексов
SELECT 
    i.index_name,
    i.table_name,
    i.num_rows,
    i.distinct_keys,
    i.clustering_factor,
    s.leaf_blocks,
    s.distinct_keys as stats_distinct_keys
FROM user_indexes i
LEFT JOIN user_ind_statistics s ON i.index_name = s.index_name
WHERE i.index_name LIKE 'IDX_%'
ORDER BY i.table_name, i.index_name;
```

### Проверка execution plan

```sql
-- Для тестового запроса
EXPLAIN PLAN FOR
SELECT * FROM V_CONSOLIDATED_REPORT_WITH_BILLING 
WHERE FINANCIAL_PERIOD = '2025-10' 
  AND IMEI = '301434061220930';

SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);
```

### Мониторинг времени выполнения

```sql
-- Включить SQL tracing
ALTER SESSION SET SQL_TRACE = TRUE;
ALTER SESSION SET TIMED_STATISTICS = TRUE;

-- Выполнить запрос
SELECT * FROM V_CONSOLIDATED_REPORT_WITH_BILLING 
WHERE FINANCIAL_PERTH = '2025-10';

-- Проверить trace файл
-- (расположение зависит от настройки DIAGNOSTIC_DEST)
```

---

## Откат изменений

### Откат индексов (если нужно)

```sql
-- Удалить индексы
DROP INDEX IDX_STECCOM_COMPOSITE_JOIN;
DROP INDEX IDX_STECCOM_BILL_MONTH;
DROP INDEX IDX_STECCOM_SERVICE_FILTER;
DROP INDEX IDX_STECCOM_DESCRIPTION;
DROP INDEX IDX_SPNET_PLAN_MAPPING;
DROP INDEX IDX_SPNET_IMEI_PLAN_MAPPING;
DROP INDEX IDX_SPNET_COMPOSITE_JOIN;
```

### Откат кэша (если нужно)

```sql
-- Удалить объекты
DROP FUNCTION GET_PLAN_FROM_CACHE;
DROP PROCEDURE REFRESH_PLAN_MAPPING;
DROP TABLE PLAN_MAPPING_CACHE;
```

---

## Следующие этапы

После выполнения Этапов 1 и 2, можно переходить к:

1. **Этап 3**: Оптимизация JOIN условий (предвычисление BILL_MONTH)
2. **Этап 4**: Материализованные представления (опционально)

Подробности в `optimization_plan.md`

