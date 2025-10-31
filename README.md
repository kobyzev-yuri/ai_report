# Iridium M2M Reporting System

Система отчетности по услугам Iridium M2M с расчетом превышений трафика и интеграцией с биллингом.

## 🏗️ Структура проекта

```
ai_report/
├── oracle/                      # Oracle (PRODUCTION)
│   ├── tables/                  # DDL таблиц
│   ├── views/                   # Представления для отчетов
│   ├── functions/               # PL/SQL функции
│   ├── data/                    # Справочные данные
│   └── README.md               # Документация по Oracle
│
├── postgresql/                  # PostgreSQL (TESTING)
│   ├── tables/                  # DDL таблиц (копия Oracle)
│   ├── views/                   # Представления (PostgreSQL версии)
│   ├── functions/               # PostgreSQL функции
│   ├── data/                    # Тестовые данные
│   ├── scripts/                 # Скрипты загрузки из Oracle
│   └── README.md               # Документация по PostgreSQL
│
├── python/                      # Python модули
│   ├── load_spnet_traffic.py   # Загрузка данных SPNet
│   ├── load_steccom_expenses.py # Загрузка данных STECCOM
│   ├── load_data_postgres.py   # Загрузка в PostgreSQL (для тестов)
│   └── calculate_overage.py    # Python модуль расчета превышений
│
├── docs/                        # Документация
│   ├── INDEX.md                # 📋 Навигация по документам
│   ├── TZ.md                   # Техническое задание
│   ├── billing_integration.md   # Интеграция с биллингом (база знаний)
│   ├── README_STREAMLIT.md     # Документация Streamlit
│   └── ...
│
├── streamlit_report.py          # Streamlit приложение (PostgreSQL)
├── streamlit_report_oracle.py   # Streamlit приложение (Oracle)
├── run_streamlit.sh            # Скрипт запуска
├── requirements.txt            # Python зависимости
└── README.md                   # Этот файл
```

## 🎯 Назначение баз данных

### Oracle (Production)
- **Расположение**: billing7@bm7
- **Назначение**: Основная production база данных
- **Данные**: Реальные данные из SPNet и STECCOM
- **Интеграция**: С биллингом (SERVICES, ACCOUNTS, CUSTOMERS)
- **Использование**: Production Streamlit отчеты

### PostgreSQL (Testing)
- **Расположение**: localhost:5432/billing
- **Назначение**: Локальная тестовая база данных
- **Данные**: Копия из Oracle для тестирования
- **Использование**: Разработка и отладка без доступа к Oracle

## 🚀 Быстрый старт

### Oracle (Production)

```bash
# 1. Создать таблицы
cd oracle/tables
sqlplus billing7/billing@bm7 @install_all_tables.sql

# 2. Загрузить справочники
cd ../data
sqlplus billing7/billing@bm7 @tariff_plans_data.sql

# 3. Создать функции
cd ../functions
sqlplus billing7/billing@bm7 @calculate_overage.sql

# 4. Создать представления
cd ../views
sqlplus billing7/billing@bm7 @install_all_views.sql

# 5. Загрузить данные
cd ../../../python
python load_spnet_traffic.py
python load_steccom_expenses.py

# 6. Запустить Streamlit
streamlit run ../streamlit_report_oracle.py --server.port 8501
```

### PostgreSQL (Testing)

```bash
# 1. Создать структуру
cd postgresql
psql -U postgres -d billing -f tables/install_all_tables.sql
psql -U postgres -d billing -f data/tariff_plans_data.sql
psql -U postgres -d billing -f functions/calculate_overage.sql
cd views && psql -U postgres -d billing -f install_all_views.sql

# 2. Загрузить данные (вариант A: из CSV)
cd ../../python
python load_spnet_traffic.py
python load_steccom_expenses.py

# 2. Загрузить данные (вариант B: из Oracle)
cd ../postgresql/scripts
python load_from_oracle_views.py

# 3. Запустить Streamlit
cd ../..
streamlit run streamlit_report.py --server.port 8502
```

## 📊 Основные представления

### Для обеих БД (Oracle и PostgreSQL)

#### V_SPNET_OVERAGE_ANALYSIS
Базовый анализ превышений трафика:
- Группировка по IMEI, CONTRACT_ID, BILL_MONTH
- Расчет OVERAGE_KB и CALCULATED_OVERAGE_CHARGE
- Только для SBD Data Usage

#### V_CONSOLIDATED_OVERAGE_REPORT
Консолидированный отчет:
- Данные SPNet (трафик, суммы)
- Данные STECCOM (расходы)
- Расчет превышений для SBD-1 и SBD-10

### Только для Oracle (требует billing7@bm7)

#### V_IRIDIUM_SERVICES_INFO
Информация о сервисах из биллинга:
- CUSTOMER_NAME (организация/ФИО)
- AGREEMENT_NUMBER (номер договора)
- ORDER_NUMBER (номер заказа/приложения)
- CODE_1C (код клиента из 1С)

#### V_CONSOLIDATED_REPORT_WITH_BILLING
Расширенный отчет с данными клиентов:
- Все из V_CONSOLIDATED_OVERAGE_REPORT
- + данные клиентов из биллинга

## 📚 Документация

### 🚀 Быстрый старт
- **[docs/QUICK_START.md](docs/QUICK_START.md)** - Начало работы (выбор Oracle или PostgreSQL)
- **[docs/QUICK_REFERENCE.md](docs/QUICK_REFERENCE.md)** - Шпаргалка команд

### 🔴 Production (Oracle)
- **[docs/PRODUCTION_OPERATIONS.md](docs/PRODUCTION_OPERATIONS.md)** - Ежедневные операции
- **[docs/BILLING_EXPORT_GUIDE.md](docs/BILLING_EXPORT_GUIDE.md)** - Экспорт для 1С
- **[oracle/queries/README.md](oracle/queries/README.md)** - Производственные запросы

### 🟢 Testing (PostgreSQL)  
- **[postgresql/SETUP_WITH_ORACLE_DATA.md](postgresql/SETUP_WITH_ORACLE_DATA.md)** - Настройка с данными из Oracle

### 🏗️ Архитектура
- **[docs/DUAL_CODEBASE_STRATEGY.md](docs/DUAL_CODEBASE_STRATEGY.md)** - Стратегия поддержки (Oracle + PostgreSQL)
- **[docs/SIDE_BY_SIDE_COMPARISON.md](docs/SIDE_BY_SIDE_COMPARISON.md)** - Сравнение реализаций

### 📋 Полная документация
- **[docs/INDEX.md](docs/INDEX.md)** - Навигация по всем документам
- **[docs/TZ.md](docs/TZ.md)** - Техническое задание
- **[docs/billing_integration.md](docs/billing_integration.md)** - Интеграция с биллингом
- **[docs/README_STREAMLIT.md](docs/README_STREAMLIT.md)** - Streamlit документация

## 🛠️ Технологии

- **Oracle 11g+** - production база данных (billing7@bm7)
- **PostgreSQL 12+** - testing база данных
- **Python 3.10+** - загрузка данных и расчеты
- **Streamlit** - веб-интерфейс для отчетов
- **Pandas** - обработка данных
- **cx_Oracle** - Oracle драйвер
- **psycopg2** - PostgreSQL драйвер

## 📦 Установка зависимостей

```bash
pip install -r requirements.txt
```

Содержимое `requirements.txt`:
- pandas
- streamlit
- cx_Oracle (для Oracle)
- psycopg2-binary (для PostgreSQL)
- openpyxl (для Excel export)

## 🔧 Тарифные планы

Расчет превышений поддерживается для:
- **SBD-1** (1 КБ включено) - ступенчатая тарификация
- **SBD-10** (10 КБ включено) - ступенчатая тарификация

Другие тарифы отображаются без расчета превышений.

## 🔄 Синхронизация данных

Для обновления тестовой БД PostgreSQL из production Oracle:

```bash
cd postgresql/scripts
python load_from_oracle_views.py
```

## 📁 Дополнительные файлы

- `oracle/test/import_iridium.py` - Python скрипт импорта данных из Oracle в PostgreSQL
- Структура таблиц: см. `oracle/tables/` и `postgresql/tables/`

## 🚦 Что использовать?

| Ситуация | База данных | Streamlit |
|----------|-------------|-----------|
| Production отчеты | Oracle (billing7@bm7) | streamlit_report_oracle.py |
| Разработка | PostgreSQL (localhost) | streamlit_report.py |
| Отладка без Oracle | PostgreSQL (localhost) | streamlit_report.py |
| Полные данные + клиенты | Oracle (billing7@bm7) | streamlit_report_oracle.py |

## ⚠️ Важные примечания

1. **PostgreSQL** - только для тестирования, не имеет доступа к биллингу
2. **Oracle** - production система, имеет интеграцию с биллингом
3. Views с биллингом (`V_IRIDIUM_SERVICES_INFO`, `V_CONSOLIDATED_REPORT_WITH_BILLING`) работают только в Oracle
4. Для синхронизации данных используйте `postgresql/scripts/load_from_oracle_views.py`

## 📞 Контакты

Проект: Iridium M2M Reporting  
Дата создания: Октябрь 2025  
Последнее обновление: 2025-10-29

## 📖 См. также

- [REORGANIZATION.md](REORGANIZATION.md) - история реорганизации проекта
- [docs/CHANGELOG.md](docs/CHANGELOG.md) - история изменений
- [docs/STATUS.md](docs/STATUS.md) - текущий статус проекта
