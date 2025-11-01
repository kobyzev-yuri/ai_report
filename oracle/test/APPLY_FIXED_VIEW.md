# Применение исправленного Oracle View

## Проблема
В view `V_IRIDIUM_SERVICES_INFO` поле `CODE_1C` не собиралось правильно из `OUTER_IDS`.

## Решение
Использовать подзапрос вместо `LEFT JOIN` в `GROUP BY` для более надежного получения `CODE_1C`.

## Применение на сервере Oracle

### Вариант 1: Применить файл напрямую

```bash
sqlplus -s $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @oracle/views/03_v_iridium_services_info.sql
```

### Вариант 2: Скопировать SQL в Oracle

Скопируйте содержимое файла `oracle/views/03_v_iridium_services_info.sql` и выполните в SQL*Plus или SQL Developer.

## Что изменилось

### Было (с ошибкой):
```sql
LEFT JOIN OUTER_IDS oi ON c.CUSTOMER_ID = oi.ID 
    AND oi.TBL = 'CUSTOMERS'
...
oi.EXT_ID AS CODE_1C
...
GROUP BY 
    ...
    oi.EXT_ID;  -- Проблема: может быть несколько записей
```

### Стало (исправлено):
```sql
-- CODE_1C из OUTER_IDS через подзапрос (более надежно)
(SELECT oi.EXT_ID 
 FROM OUTER_IDS oi 
 WHERE oi.ID = c.CUSTOMER_ID 
   AND oi.TBL = 'CUSTOMERS' 
   AND ROWNUM = 1) AS CODE_1C
...
GROUP BY 
    ...
    -- oi.EXT_ID убран из GROUP BY
```

## Преимущества подзапроса

1. **Надежность**: `ROWNUM = 1` гарантирует одну запись, даже если в `OUTER_IDS` несколько записей для одного `CUSTOMER_ID`
2. **Корректный GROUP BY**: Не нужно включать `oi.EXT_ID` в `GROUP BY`
3. **Производительность**: Подзапрос выполняется один раз для каждой строки результата

## Проверка после применения

```sql
-- Проверка количества записей с CODE_1C
SELECT 
    COUNT(*) AS TOTAL,
    COUNT(CODE_1C) AS WITH_CODE_1C,
    COUNT(*) - COUNT(CODE_1C) AS NULL_CODE_1C
FROM V_IRIDIUM_SERVICES_INFO;

-- Примеры записей с CODE_1C
SELECT 
    SERVICE_ID,
    CONTRACT_ID,
    CUSTOMER_ID,
    CUSTOMER_NAME,
    CODE_1C
FROM V_IRIDIUM_SERVICES_INFO
WHERE CODE_1C IS NOT NULL
  AND ROWNUM <= 10;
```

## После применения view

1. **Переэкспортируйте данные:**
   ```bash
   sqlplus -s $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @oracle/test/export_v_iridium_services_info.sql
   ```

2. **Переимпортируйте в PostgreSQL:**
   ```bash
   cd oracle/test
   python3 import_iridium.py --input V_IRIDIUM_SERVICES_INFO.txt \
     --dsn "host=localhost dbname=billing user=cnn password=1234" \
     --table iridium_services_info --truncate
   ```

3. **Проверьте результат:**
   ```bash
   psql -U cnn -d billing -c "SELECT COUNT(*), COUNT(code_1c) FROM iridium_services_info;"
   ```

