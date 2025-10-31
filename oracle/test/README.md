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

## 📥 Импорт в PostgreSQL

### Вариант 1: Использовать готовый bash скрипт

```bash
cd oracle/test

# С переменными окружения (опционально)
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=billing
export PGUSER=postgres
export PGPASSWORD=your-password-here

./import_to_postgresql.sh
```

### Вариант 2: Использовать Python скрипт напрямую

```bash
cd oracle/test

PGPASSWORD=your-password-here python3 import_iridium.py \
  --input V_IRIDIUM_SERVICES_INFO.txt \
  --dsn "host=localhost dbname=billing user=postgres password=your-password-here" \
  --table iridium_services_info \
  --truncate
```

## ✅ Проверка результата

```bash
psql -U postgres -d billing -c "SELECT COUNT(*) FROM iridium_services_info;"
psql -U postgres -d billing -c "
  SELECT service_id, contract_id, LEFT(imei,20) as imei, tariff_id, status, account_id 
  FROM iridium_services_info 
  LIMIT 5;
"
```

## 📝 Примечания

1. **IMEI источник:** По умолчанию используется `PASSWD` из `V_IRIDIUM_SERVICES_INFO`.
   Для использования `VSAT` обновите view:
   ```sql
   -- oracle/views/03_v_iridium_services_info.sql
   s.PASSWD AS IMEI  →  s.VSAT AS IMEI
   ```

2. **Формат файла:** TSV (Tab-Separated Values), 17 колонок, обработка кавычек и NULL

3. **Импорт:** Использует `import_iridium.py` с валидацией, безопасными кастами типов и логированием

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
psql -U postgres -d billing -c "SELECT COUNT(*) FROM iridium_services_info;"
```

