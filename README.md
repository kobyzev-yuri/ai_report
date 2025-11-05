# Iridium M2M Reporting System

Система отчетности по услугам Iridium M2M с расчетом превышений трафика и интеграцией с биллингом.

## 🚀 Быстрый старт

### Установка Oracle базы данных

Полная инструкция по установке: **[docs/INSTALLATION_ORACLE.md](docs/INSTALLATION_ORACLE.md)**

**Краткая версия:**
```bash
# 1. Таблицы
cd oracle/tables
sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @install_all_tables.sql

# 2. Справочники
cd ../data
sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @tariff_plans_data.sql

# 3. Функции
cd ../functions
sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @calculate_overage.sql

# 4. Представления
cd ../views
sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @install_all_views.sql

# 5. Загрузка данных
cd ../../../python
python load_spnet_traffic.py
python load_steccom_expenses.py
```

### Запуск веб-интерфейса

```bash
# Установка зависимостей (если еще не установлены)
pip install -r requirements.txt

# Запуск Streamlit приложения для Oracle
streamlit run streamlit_report_oracle.py --server.port 8501
```

Приложение будет доступно по адресу: **http://localhost:8501**

## 📊 Основные интерфейсы

### 1. Streamlit Web Interface

**Файл:** `streamlit_report_oracle.py`

**Основные функции:**
- **Главный отчет** - сводная таблица по IMEI с расчетом превышений
- **Фильтры:**
  - По IMEI
  - По периоду (BILL_MONTH)
  - По тарифному плану
  - По клиенту (CODE_1C, CUSTOMER_NAME)
- **Экспорт данных:**
  - Excel (формат .xlsx)
  - CSV
- **Статистика:**
  - Общая сумма по периодам
  - Превышения трафика
  - Сравнение SPNet и STECCOM сумм

**Использование:**
1. Откройте браузер: `http://localhost:8501`
2. Выберите период в боковой панели
3. Примените фильтры (опционально)
4. Просмотрите данные в таблице
5. Экспортируйте при необходимости

### 2. Основные представления (VIEW) Oracle

#### V_SPNET_OVERAGE_ANALYSIS
Базовый анализ превышения трафика:
- Группировка: IMEI, CONTRACT_ID, BILL_MONTH
- Расчет: OVERAGE_KB, CALCULATED_OVERAGE_CHARGE
- Только для SBD Data Usage

**Пример запроса:**
```sql
SELECT imei, contract_id, bill_month, plan_name, 
       total_usage_kb, overage_kb, calculated_overage_charge
FROM V_SPNET_OVERAGE_ANALYSIS
WHERE bill_month = 202510;
```

#### V_CONSOLIDATED_OVERAGE_REPORT
Консолидированный отчет:
- Данные SPNet (трафик, суммы)
- Данные STECCOM (расходы)
- Расчет превышений для SBD-1 и SBD-10
- **ВАЖНО:** Каждая строка = отдельный период (BILL_MONTH)

**Пример запроса:**
```sql
SELECT imei, contract_id, bill_month, plan_name,
       spnet_total_amount, steccom_total_amount,
       calculated_overage, overage_kb
FROM V_CONSOLIDATED_OVERAGE_REPORT
WHERE bill_month = '202510'
ORDER BY imei;
```

#### V_IRIDIUM_SERVICES_INFO
Информация о сервисах из биллинга (требует доступ к таблицам биллинга):
- CUSTOMER_NAME (организация/ФИО)
- AGREEMENT_NUMBER (номер договора)
- ORDER_NUMBER (номер заказа)
- CODE_1C (код клиента из 1С)

**Пример запроса:**
```sql
SELECT contract_id, imei, customer_name, agreement_number, code_1c
FROM V_IRIDIUM_SERVICES_INFO
WHERE code_1c IS NOT NULL;
```

#### V_CONSOLIDATED_REPORT_WITH_BILLING
Расширенный отчет с данными клиентов:
- Все данные из V_CONSOLIDATED_OVERAGE_REPORT
- + данные клиентов из биллинга
- Используется для экспорта в 1С

**Пример запроса:**
```sql
SELECT customer_name, code_1c, agreement_number,
       imei, plan_name, bill_month,
       spnet_total_amount, calculated_overage, steccom_total_amount
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE bill_month = '202510'
ORDER BY customer_name;
```

### 3. Python скрипты загрузки данных

#### load_spnet_traffic.py
Загрузка данных трафика из отчетов SPNet.

**Источник:** `data/SPNet reports/*.xlsx` или `*.csv`

**Назначение:** Заполнение таблицы `SPNET_TRAFFIC`

**Использование:**
```bash
cd python
python load_spnet_traffic.py
```

#### load_steccom_expenses.py
Загрузка данных расходов из отчетов STECCOM.

**Источник:** `data/STECCOMLLCRussiaSBD.AccessFees_reports/*.csv`

**Назначение:** Заполнение таблицы `STECCOM_EXPENSES`

**Использование:**
```bash
cd python
python load_steccom_expenses.py
```

## 📁 Структура проекта

```
ai_report/
├── oracle/                      # Oracle (PRODUCTION)
│   ├── tables/                  # DDL таблиц
│   ├── views/                   # Представления для отчетов
│   ├── functions/               # PL/SQL функции
│   ├── data/                    # Справочные данные
│   └── README.md                # Документация по Oracle
│
├── python/                      # Python модули
│   ├── load_spnet_traffic.py   # Загрузка данных SPNet
│   ├── load_steccom_expenses.py # Загрузка данных STECCOM
│   └── calculate_overage.py    # Python модуль расчета превышений
│
├── docs/                        # Документация
│   ├── INSTALLATION_ORACLE.md   # Инструкция по установке Oracle
│   ├── billing_integration.md   # Интеграция с биллингом
│   ├── README_STREAMLIT.md     # Документация Streamlit
│   └── TZ.md                   # Техническое задание
│
├── streamlit_report_oracle.py  # Streamlit приложение (Oracle)
├── requirements.txt            # Python зависимости
└── README.md                   # Этот файл
```

## 🛠️ Технологии

- **Oracle 11g+** - production база данных
- **Python 3.10+** - загрузка данных и расчеты
- **Streamlit** - веб-интерфейс для отчетов
- **Pandas** - обработка данных
- **cx_Oracle** - Oracle драйвер
- **openpyxl** - работа с Excel файлами

## 📦 Установка зависимостей

```bash
pip install -r requirements.txt
```

**Зависимости:**
- pandas
- streamlit
- cx_Oracle
- openpyxl

## 🔧 Тарифные планы

Расчет превышений поддерживается для:
- **SBD-1** (1 КБ включено) - ступенчатая тарификация
- **SBD-10** (10 КБ включено) - ступенчатая тарификация

Другие тарифы отображаются без расчета превышений.

## 📚 Документация

- **[docs/INSTALLATION_ORACLE.md](docs/INSTALLATION_ORACLE.md)** - Полная инструкция по установке Oracle
- **[docs/billing_integration.md](docs/billing_integration.md)** - Интеграция с биллингом
- **[docs/README_STREAMLIT.md](docs/README_STREAMLIT.md)** - Документация Streamlit интерфейса
- **[docs/TZ.md](docs/TZ.md)** - Техническое задание
- **[docs/BILLING_EXPORT_GUIDE.md](docs/BILLING_EXPORT_GUIDE.md)** - Экспорт для 1С
- **[oracle/README.md](oracle/README.md)** - Документация по Oracle скриптам

## ⚠️ Важные примечания

1. **Views с биллингом** (`V_IRIDIUM_SERVICES_INFO`, `V_CONSOLIDATED_REPORT_WITH_BILLING`) требуют доступ к таблицам биллинга
2. Если доступа к биллингу нет, используйте только `V_CONSOLIDATED_OVERAGE_REPORT`
3. Для синхронизации данных используйте соответствующие Python скрипты
4. **ВАЖНО:** Используйте переменные окружения для паролей Oracle:
   ```bash
   export ORACLE_USER=your-username
   export ORACLE_PASSWORD=your-password
   export ORACLE_SERVICE=your-service-name
   ```

## 🔄 Основные операции

### Ежедневная загрузка данных

```bash
# 1. Загрузить новые данные SPNet
cd python
python load_spnet_traffic.py

# 2. Загрузить новые данные STECCOM
python load_steccom_expenses.py

# 3. Проверить данные в Streamlit
cd ..
streamlit run streamlit_report_oracle.py
```

### Экспорт данных для 1С

Используйте представление `V_CONSOLIDATED_REPORT_WITH_BILLING`:

```sql
-- Экспорт за период
SELECT 
    code_1c,
    customer_name,
    agreement_number,
    imei,
    plan_name,
    bill_month,
    spnet_total_amount,
    calculated_overage,
    steccom_total_amount
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE bill_month = '202510'
  AND code_1c IS NOT NULL
ORDER BY code_1c, imei;
```

## 📞 Контакты

Проект: Iridium M2M Reporting  
Дата создания: Октябрь 2025
