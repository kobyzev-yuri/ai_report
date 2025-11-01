# Oracle Export / PostgreSQL Import

Рабочие скрипты для экспорта данных из Oracle и импорта в PostgreSQL.

## 📤 Экспорт из Oracle

```bash
# Вариант 1: из директории oracle/test
cd oracle/test
sqlplus -s $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @export_v_iridium_services_info.sql
# или: sqlplus -s username/password@service_name @export_v_iridium_services_info.sql

# Вариант 2: из корня проекта
sqlplus -s $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @oracle/test/export_v_iridium_services_info.sql
```

**Результат:** `V_IRIDIUM_SERVICES_INFO.txt` (TSV, 17 колонок, таб-разделитель)

**Проверка формата:**
```bash
awk -F'\t' '{print NF}' V_IRIDIUM_SERVICES_INFO.txt | head -10 | sort -u
# Должно выводить только "17"
```

**Проверка содержимого:**
```bash
# Просмотр первых строк
head -5 V_IRIDIUM_SERVICES_INFO.txt

# Проверка CODE_1C (колонка 17)
awk -F'\t' 'NR<=10 {print "CODE_1C:", $17}' V_IRIDIUM_SERVICES_INFO.txt
```

## 📥 Импорт в PostgreSQL

### Вариант 1: Использовать готовый bash скрипт

```bash
cd oracle/test

# С переменными окружения (опционально)
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=billing
export PGUSER=cnn
export PGPASSWORD=your-password-here

./import_to_postgresql.sh
```

### Вариант 2: Использовать Python скрипт напрямую

```bash
cd oracle/test

PGPASSWORD=your-password-here python3 import_iridium.py \
  --input V_IRIDIUM_SERVICES_INFO.txt \
  --dsn "host=localhost dbname=billing user=cnn password=your-password-here" \
  --table iridium_services_info \
  --truncate
```

## ✅ Проверка результата

```bash
psql -U cnn -d billing -c "SELECT COUNT(*) FROM iridium_services_info;"
psql -U cnn -d billing -c "
  SELECT service_id, contract_id, LEFT(imei,20) as imei, tariff_id, status, account_id 
  FROM iridium_services_info 
  LIMIT 5;
"
```

## 📝 Примечания

1. **IMEI источник:** Используется `VSAT` из `V_IRIDIUM_SERVICES_INFO`.
   View настроен: `s.VSAT AS IMEI`

2. **TYPE_ID:** View включает оба типа услуг:
   - `TYPE_ID = 9002` - тарификация по трафику
   - `TYPE_ID = 9014` - тарификация по сообщениям в биллинге (у Iridium только трафик)

3. **CODE_1C:** Код клиента из 1С собирается из таблицы `OUTER_IDS`:
   ```sql
   (SELECT oi.EXT_ID 
    FROM OUTER_IDS oi 
    WHERE oi.ID = c.CUSTOMER_ID 
      AND oi.TBL = 'CUSTOMERS' 
      AND ROWNUM = 1) AS CODE_1C
   ```
   Если `CODE_1C` NULL после импорта:
   - Проверьте наличие записей в `OUTER_IDS` для `CUSTOMER_ID`
   - Запустите тест: `sqlplus @test_code_1c_export.sql`
   - Проверьте view: `sqlplus @query_view_simple.sql`

4. **Формат файла:** TSV (Tab-Separated Values), 17 колонок, обработка кавычек и NULL

5. **Импорт:** Использует `import_iridium.py` с валидацией, безопасными кастами типов и логированием

## 🔄 Полный цикл

```bash
# 1. Экспорт из Oracle
export ORACLE_USER=your-username
export ORACLE_PASSWORD=your-password
export ORACLE_SERVICE=your-service-name
sqlplus -s $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @oracle/test/export_v_iridium_services_info.sql

# 2. Импорт в PostgreSQL
cd oracle/test
export PGPASSWORD=your-postgres-password
./import_to_postgresql.sh

# 3. Проверка
psql -U cnn -d billing -c "SELECT COUNT(*) FROM iridium_services_info;"
```

## 🔍 Просмотр данных view в Oracle

Если нужно просто посмотреть данные (не экспортировать):

```bash
# Простой просмотр (читаемый формат)
sqlplus -s $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @query_view_simple.sql

# Или напрямую:
sqlplus -s $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE << EOF
SET PAGESIZE 50 LINESIZE 300
SELECT SERVICE_ID, CONTRACT_ID, CUSTOMER_NAME, CODE_1C 
FROM V_IRIDIUM_SERVICES_INFO 
WHERE ROWNUM <= 10;
EXIT
EOF
```
