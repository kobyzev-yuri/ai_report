# Проверка экспорта CODE_1C из Oracle

## Проблема
После импорта в PostgreSQL все значения `code_1c` NULL.

## Причины

`CODE_1C` собирается из таблицы `OUTER_IDS` через связь:
```sql
LEFT JOIN OUTER_IDS oi ON c.CUSTOMER_ID = oi.ID 
    AND oi.TBL = 'CUSTOMERS'
-- oi.EXT_ID AS CODE_1C
```

Возможные причины:
1. **В Oracle нет данных в OUTER_IDS** для соответствующих CUSTOMER_ID
2. **View не собирает CODE_1C правильно** (проблема с JOIN)
3. **Экспорт не захватил CODE_1C** (проблема в скрипте экспорта)

## Проверка в Oracle

### Шаг 1: Проверка view V_IRIDIUM_SERVICES_INFO
```sql
sqlplus -s $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE << EOF
SELECT 
    COUNT(*) AS TOTAL,
    COUNT(CODE_1C) AS WITH_CODE_1C,
    COUNT(*) - COUNT(CODE_1C) AS NULL_CODE_1C
FROM V_IRIDIUM_SERVICES_INFO;
EOF
```

### Шаг 2: Тестовый скрипт
```bash
cd oracle/test
sqlplus -s $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @test_code_1c_export.sql
```

Скрипт покажет:
- Количество записей с/без CODE_1C в view
- Примеры записей с CODE_1C
- Связь через OUTER_IDS
- Сервисы без CODE_1C (требуют маппинга)

### Шаг 3: Проверка OUTER_IDS напрямую
```sql
SELECT 
    COUNT(*) AS TOTAL_OUTER_IDS,
    COUNT(DISTINCT ID) AS CUSTOMERS_WITH_CODE,
    COUNT(CASE WHEN TBL = 'CUSTOMERS' THEN 1 END) AS FOR_CUSTOMERS
FROM OUTER_IDS;
```

### Шаг 4: Проверка связки SERVICES -> CUSTOMERS -> OUTER_IDS
```sql
SELECT 
    s.SERVICE_ID,
    s.LOGIN AS CONTRACT_ID,
    c.CUSTOMER_ID,
    oi.EXT_ID AS CODE_1C
FROM SERVICES s
JOIN CUSTOMERS c ON s.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN OUTER_IDS oi ON c.CUSTOMER_ID = oi.ID AND oi.TBL = 'CUSTOMERS'
WHERE s.TYPE_ID = 9002
  AND s.STATUS = 1
  AND ROWNUM <= 10;
```

## Если CODE_1C есть в Oracle, но не экспортировался

1. **Проверьте экспорт скрипт:**
   ```bash
   # Экспортируйте снова
   sqlplus -s $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @export_v_iridium_services_info.sql
   
   # Проверьте, что CODE_1C присутствует в файле (колонка 17)
   awk -F'\t' 'NR<=5 {print $17}' V_IRIDIUM_SERVICES_INFO.txt
   ```

2. **Проверьте импорт:**
   ```bash
   # Импортируйте заново
   python3 import_iridium.py --input V_IRIDIUM_SERVICES_INFO.txt \
     --dsn "host=localhost dbname=billing user=cnn password=1234" \
     --table iridium_services_info --truncate
   
   # Проверьте в PostgreSQL
   psql -U cnn -d billing -c "SELECT COUNT(*), COUNT(code_1c) FROM iridium_services_info;"
   ```

## Если CODE_1C нет в Oracle (OUTER_IDS пустая)

Это означает, что в биллинге не настроены связи клиентов с 1С. Нужно:
1. Добавить записи в `OUTER_IDS` для клиентов
2. Или получить `CODE_1C` из другого источника (если есть)

## Структура OUTER_IDS

```sql
CREATE TABLE OUTER_IDS (
    ID NUMBER,           -- CUSTOMER_ID
    TBL VARCHAR2(255),   -- 'CUSTOMERS'
    EXT_ID VARCHAR2(255)  -- Код из 1С
);
```

Пример записи:
```sql
INSERT INTO OUTER_IDS (ID, TBL, EXT_ID) 
VALUES (241935, 'CUSTOMERS', '1C00123');
```

