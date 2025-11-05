# Oracle Test Utilities

Утилиты для работы с Oracle VIEW и тестирования данных.

## 📋 Содержимое

- `query_view_simple.sql` - простой запрос для просмотра данных из `V_IRIDIUM_SERVICES_INFO`
- `V_IRIDIUM_SERVICES_INFO.csv` - экспортированные данные (создается автоматически при запуске `export_all.sql`)

## 🔍 Просмотр данных VIEW в Oracle

Для быстрого просмотра данных используйте:

```bash
# Простой просмотр (читаемый формат)
cd oracle/test
sqlplus -s $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @query_view_simple.sql

# Или напрямую:
sqlplus -s $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE << EOF
SET PAGESIZE 50 LINESIZE 300
SELECT SERVICE_ID, CONTRACT_ID, CUSTOMER_NAME, CODE_1C, IS_SUSPENDED
FROM V_IRIDIUM_SERVICES_INFO 
WHERE ROWNUM <= 10;
EXIT
EOF
```

## 📤 Экспорт данных

**Для экспорта используйте единый скрипт:**

```bash
cd oracle/export
sqlplus billing7/billing@bm7 @export_all.sql
```

Этот скрипт создаст:
- `billing_integration.csv` - для импорта в 1С
- `../test/V_IRIDIUM_SERVICES_INFO.csv` - для импорта в PostgreSQL
- `service_transfer_history.csv` - история переводов услуг

## 📥 Импорт в PostgreSQL

**Для импорта используйте:**

```bash
cd postgresql/scripts
python3 load_from_oracle_views.py
```

Этот скрипт напрямую загружает данные из Oracle VIEW в PostgreSQL таблицы.

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

4. **IS_SUSPENDED:** Флаг приостановления услуги (проверяет наличие активной услуги типа 9008)

5. **STATUS:** 
   - `10` - активный
   - `-10` - приостановленный/закрытый

6. **STOP_DATE:** Дата завершения предоставления услуги (stop_date)
