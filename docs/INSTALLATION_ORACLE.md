# Установка Oracle Database - Iridium M2M Reporting

Полная инструкция по установке системы отчетности Iridium M2M в Oracle Database.

## Предварительные требования

- Oracle Database 11g или выше
- Доступ к базе данных с правами на создание таблиц, представлений и функций
- SQL*Plus или SQL Developer для выполнения скриптов
- Python 3.10+ (для загрузки данных)
- Установленные Python библиотеки: `pandas`, `cx_Oracle`, `openpyxl`

## Шаг 1: Подготовка окружения

### 1.1 Настройка переменных окружения

```bash
export ORACLE_USER=your_username
export ORACLE_PASSWORD=your_password
export ORACLE_SERVICE=your_service_name
# Или используйте формат: username/password@host:port/service_name
```

### 1.2 Проверка подключения

```bash
sqlplus -s $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE << EOF
SELECT 'Connection OK' FROM DUAL;
EXIT
EOF
```

### 1.3 Установка Python зависимостей

```bash
pip install -r requirements.txt
```

## Шаг 2: Создание таблиц

Таблицы создаются в схеме, указанной в подключении (например, `billing7`).

```bash
cd oracle/tables
sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @install_all_tables.sql
```

**Создаются следующие таблицы:**
- `SPNET_TRAFFIC` - данные трафика из SPNet
- `STECCOM_EXPENSES` - данные расходов из STECCOM
- `TARIFF_PLANS` - справочник тарифных планов
- `LOAD_LOGS` - журнал загрузок данных

**Проверка:**
```sql
SELECT table_name, num_rows 
FROM user_tables 
WHERE table_name IN ('SPNET_TRAFFIC', 'STECCOM_EXPENSES', 'TARIFF_PLANS', 'LOAD_LOGS')
ORDER BY table_name;
```

## Шаг 3: Загрузка справочных данных

Загрузка тарифных планов:

```bash
cd oracle/data
sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @tariff_plans_data.sql
```

**Проверка:**
```sql
SELECT COUNT(*) as total_plans, 
       COUNT(CASE WHEN active = 'Y' THEN 1 END) as active_plans
FROM tariff_plans;
```

## Шаг 4: Создание функций

Создание PL/SQL функции для расчета превышений трафика:

```bash
cd oracle/functions
sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @calculate_overage.sql
```

**Проверка:**
```sql
-- Тест функции для SBD-1 (1 КБ включено)
SELECT calculate_overage('SBD Tiered 1250 1K', 2500) as overage_charge FROM DUAL;

-- Тест функции для SBD-10 (10 КБ включено)
SELECT calculate_overage('SBD Tiered 1250 10K', 35000) as overage_charge FROM DUAL;
```

## Шаг 5: Создание представлений (VIEW)

**ВАЖНО:** Представления создаются в строгом порядке из-за зависимостей.

```bash
cd oracle/views
sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @install_all_views.sql
```

**Порядок создания:**
1. `V_SPNET_OVERAGE_ANALYSIS` - базовый анализ превышения трафика
2. `V_CONSOLIDATED_OVERAGE_REPORT` - консолидированный отчет (SPNet + STECCOM)
3. `V_IRIDIUM_SERVICES_INFO` - информация о сервисах из биллинга (требует доступ к таблицам биллинга)
4. `V_CONSOLIDATED_REPORT_WITH_BILLING` - расширенный отчет с данными клиентов

**Проверка:**
```sql
-- Проверка количества записей в каждом представлении
SELECT 'V_SPNET_OVERAGE_ANALYSIS' as view_name, COUNT(*) as record_count
FROM V_SPNET_OVERAGE_ANALYSIS
UNION ALL
SELECT 'V_CONSOLIDATED_OVERAGE_REPORT', COUNT(*)
FROM V_CONSOLIDATED_OVERAGE_REPORT
UNION ALL
SELECT 'V_IRIDIUM_SERVICES_INFO', COUNT(*)
FROM V_IRIDIUM_SERVICES_INFO
UNION ALL
SELECT 'V_CONSOLIDATED_REPORT_WITH_BILLING', COUNT(*)
FROM V_CONSOLIDATED_REPORT_WITH_BILLING;
```

### Требования для представлений с биллингом

Если вы используете `V_IRIDIUM_SERVICES_INFO` или `V_CONSOLIDATED_REPORT_WITH_BILLING`, требуется доступ к следующим таблицам биллинга:
- `SERVICES`
- `ACCOUNTS`
- `CUSTOMERS`
- `BM_CUSTOMER_CONTACT`
- `BM_CONTACT_DICT`
- `OUTER_IDS`

Если доступа к биллингу нет, используйте только `V_CONSOLIDATED_OVERAGE_REPORT`.

## Шаг 6: Загрузка данных

### 6.1 Загрузка данных SPNet

```bash
cd python
python load_spnet_traffic.py
```

Скрипт читает Excel/CSV файлы из папки `data/SPNet reports/` и загружает их в таблицу `SPNET_TRAFFIC`.

### 6.2 Загрузка данных STECCOM

```bash
python load_steccom_expenses.py
```

Скрипт читает CSV файлы из папки `data/STECCOMLLCRussiaSBD.AccessFees_reports/` и загружает их в таблицу `STECCOM_EXPENSES`.

**Проверка загрузки:**
```sql
-- Проверка загруженных данных
SELECT 
    'SPNET_TRAFFIC' as table_name,
    COUNT(*) as records,
    MIN(bill_month) as min_month,
    MAX(bill_month) as max_month
FROM spnet_traffic
UNION ALL
SELECT 
    'STECCOM_EXPENSES',
    COUNT(*),
    TO_CHAR(MIN(invoice_date), 'YYYYMM'),
    TO_CHAR(MAX(invoice_date), 'YYYYMM')
FROM steccom_expenses;
```

## Шаг 7: Проверка установки

### 7.1 Проверка структуры

```sql
-- Проверка всех созданных объектов
SELECT 'TABLES' as object_type, COUNT(*) as count
FROM user_tables
WHERE table_name IN ('SPNET_TRAFFIC', 'STECCOM_EXPENSES', 'TARIFF_PLANS', 'LOAD_LOGS')
UNION ALL
SELECT 'VIEWS', COUNT(*)
FROM user_views
WHERE view_name LIKE 'V_%'
UNION ALL
SELECT 'FUNCTIONS', COUNT(*)
FROM user_procedures
WHERE object_name = 'CALCULATE_OVERAGE';
```

### 7.2 Тестовый запрос

```sql
-- Пример запроса к консолидированному отчету
SELECT 
    imei,
    contract_id,
    bill_month,
    plan_name,
    total_usage_kb,
    overage_kb,
    calculated_overage,
    spnet_total_amount,
    steccom_total_amount
FROM V_CONSOLIDATED_OVERAGE_REPORT
WHERE ROWNUM <= 10
ORDER BY bill_month DESC, imei;
```

## Шаг 8: Запуск Streamlit приложения

После успешной установки можно запустить веб-интерфейс:

```bash
streamlit run streamlit_report_oracle.py --server.port 8501
```

Приложение будет доступно по адресу: `http://localhost:8501`

## Устранение проблем

### Ошибка: "ORA-00942: table or view does not exist"

**Причина:** Таблицы еще не созданы или созданы в другой схеме.

**Решение:** Выполните шаг 2 (создание таблиц) и убедитесь, что используете правильную схему.

### Ошибка: "ORA-00904: invalid identifier"

**Причина:** Представление ссылается на несуществующую колонку или таблицу.

**Решение:** 
1. Проверьте, что все таблицы созданы (шаг 2)
2. Проверьте, что все функции созданы (шаг 4)
3. Создавайте представления в правильном порядке (шаг 5)

### Ошибка при создании V_IRIDIUM_SERVICES_INFO

**Причина:** Нет доступа к таблицам биллинга.

**Решение:** 
- Убедитесь, что у пользователя есть права на чтение таблиц `SERVICES`, `ACCOUNTS`, `CUSTOMERS`, `BM_CUSTOMER_CONTACT`, `BM_CONTACT_DICT`, `OUTER_IDS`
- Или используйте только `V_CONSOLIDATED_OVERAGE_REPORT` без данных биллинга

### Ошибка: "ORA-01427: single-row subquery returns more than one row"

**Причина:** В `V_IRIDIUM_SERVICES_INFO` подзапрос для `CODE_1C` возвращает несколько строк.

**Решение:** Уже исправлено в текущей версии - используется `ROWNUM = 1`. Если ошибка повторяется, проверьте данные в `OUTER_IDS`:

```sql
-- Найти дубликаты
SELECT id, COUNT(*) as cnt
FROM outer_ids
WHERE tbl = 'CUSTOMERS'
GROUP BY id
HAVING COUNT(*) > 1;
```

## Порядок установки (краткая справка)

```bash
# 1. Таблицы
cd oracle/tables && sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @install_all_tables.sql

# 2. Данные справочников
cd ../data && sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @tariff_plans_data.sql

# 3. Функции
cd ../functions && sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @calculate_overage.sql

# 4. Представления
cd ../views && sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @install_all_views.sql

# 5. Загрузка данных
cd ../../../python
python load_spnet_traffic.py
python load_steccom_expenses.py

# 6. Запуск приложения
cd ..
streamlit run streamlit_report_oracle.py --server.port 8501
```

## Дополнительная информация

- Структура таблиц: см. `oracle/tables/`
- Описание представлений: см. `oracle/README.md`
- Интеграция с биллингом: см. `docs/billing_integration.md`
- Streamlit интерфейс: см. `docs/README_STREAMLIT.md`

