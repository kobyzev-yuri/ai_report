# Реорганизация проекта - 2025-10-29

## Цель
Навести порядок в структуре файлов, разделив SQL скрипты для Oracle и PostgreSQL по назначению.

## Проблемы до реорганизации

1. **create_tariff_plans.sql** содержал всё подряд:
   - DDL таблицы TARIFF_PLANS
   - DML (INSERT данных)
   - PL/SQL функцию CALCULATE_OVERAGE
   - Views: V_SPNET_OVERAGE_ANALYSIS, V_CONSOLIDATED_OVERAGE_REPORT

2. **create_billing_views.sql** смешивал два view вместе

3. Python, SQL и документы были вперемешку в корне

4. Не было четкого разделения Oracle vs PostgreSQL

## Новая структура

```
ai_report/
├── oracle/                      # Oracle скрипты
│   ├── tables/                  # DDL таблиц (будет добавлено)
│   ├── views/                   # Представления
│   │   ├── 01_v_spnet_overage_analysis.sql
│   │   ├── 02_v_consolidated_overage_report.sql
│   │   ├── 03_v_iridium_services_info.sql
│   │   ├── 04_v_consolidated_report_with_billing.sql
│   │   └── install_all_views.sql
│   ├── functions/               # PL/SQL функции
│   │   └── calculate_overage.sql
│   ├── data/                    # Справочные данные
│   │   └── tariff_plans_data.sql
│   └── README.md               # Документация Oracle
│
├── postgresql/                  # PostgreSQL скрипты (будет добавлено)
│   ├── tables/
│   ├── views/
│   └── functions/
│
├── python/                      # Python модули
│   ├── load_spnet_traffic.py
│   ├── load_steccom_expenses.py
│   ├── load_data_postgres.py
│   └── calculate_overage.py
│
├── docs/                        # Документация
│   ├── INDEX.md                # Навигация по документам
│   ├── TZ.md                   # Техническое задание
│   ├── billing_integration.md   # Интеграция с биллингом
│   ├── README_STREAMLIT.md     # Streamlit документация
│   ├── CHANGELOG.md
│   ├── STATUS.md
│   └── ...
│
├── streamlit_report.py          # Streamlit (PostgreSQL)
├── streamlit_report_oracle.py   # Streamlit (Oracle)
├── run_streamlit.sh
├── requirements.txt
└── README.md                   # Главный README
```

## Изменения

### Созданные файлы

#### Oracle Views (раздельно):
- `oracle/views/01_v_spnet_overage_analysis.sql` - базовый анализ превышений
- `oracle/views/02_v_consolidated_overage_report.sql` - консолидированный отчет
- `oracle/views/03_v_iridium_services_info.sql` - данные из биллинга
- `oracle/views/04_v_consolidated_report_with_billing.sql` - полный отчет с клиентами
- `oracle/views/install_all_views.sql` - мастер-скрипт установки

#### Oracle Functions:
- `oracle/functions/calculate_overage.sql` - PL/SQL функция расчета

#### Oracle Data:
- `oracle/data/tariff_plans_data.sql` - данные тарифных планов

#### Документация:
- `oracle/README.md` - руководство по Oracle скриптам
- `docs/INDEX.md` - навигация по документации
- `docs/billing_integration.md` - база знаний по биллингу
- `README.md` - обновленный главный README

### Перемещенные файлы

#### В docs/:
- TZ.md
- billing_integration.md
- README_STREAMLIT.md
- CHANGELOG.md
- SUMMARY.md
- STATUS.md
- COMPATIBILITY_SUMMARY.md
- ORACLE_COMPATIBILITY_ANALYSIS.md
- POSTGRES_TEST_DB.md

#### В python/:
- load_spnet_traffic.py
- load_steccom_expenses.py
- load_data_postgres.py
- calculate_overage.py

### Удаленные файлы

- `create_tariff_plans.sql` - разделен на views/, functions/, data/
- `create_billing_views.sql` - разделен на отдельные views
- `CURRENT_VIEWS.sql` - устаревший backup

## Views для Oracle

### 1. V_SPNET_OVERAGE_ANALYSIS
- **Источник**: SPNET_TRAFFIC + TARIFF_PLANS
- **Назначение**: Базовый анализ превышений по IMEI
- **Особенности**: 
  - Группировка по USAGE_TYPE
  - Расчет только для SBD Data Usage
  - Использует функцию CALCULATE_OVERAGE

### 2. V_CONSOLIDATED_OVERAGE_REPORT
- **Источник**: V_SPNET_OVERAGE_ANALYSIS + STECCOM_EXPENSES
- **Назначение**: Консолидированный отчет SPNet + STECCOM
- **Особенности**:
  - FULL OUTER JOIN для включения всех записей
  - Конвертация BILL_MONTH в YYYYMM
  - Суммирование по всем типам использования

### 3. V_IRIDIUM_SERVICES_INFO
- **Источник**: SERVICES + ACCOUNTS + CUSTOMERS + BM_* + OUTER_IDS
- **Назначение**: Информация о сервисах и клиентах из биллинга
- **Ключевые поля**:
  - CONTRACT_ID (связь с отчетами)
  - CUSTOMER_NAME (организация или ФИО)
  - AGREEMENT_NUMBER (договор СТЭККОМ)
  - ORDER_NUMBER (бланк заказа)
  - CODE_1C (код из 1С)

### 4. V_CONSOLIDATED_REPORT_WITH_BILLING
- **Источник**: V_CONSOLIDATED_OVERAGE_REPORT + V_IRIDIUM_SERVICES_INFO
- **Назначение**: Полный отчет с данными клиентов
- **Использование**: Для Streamlit отчетов с информацией о клиентах

## Порядок установки Oracle

```bash
# 1. Таблицы (DDL будет добавлено позже)
sqlplus billing7/billing@bm7 @oracle/tables/...

# 2. Данные
sqlplus billing7/billing@bm7 @oracle/data/tariff_plans_data.sql

# 3. Функции
sqlplus billing7/billing@bm7 @oracle/functions/calculate_overage.sql

# 4. Представления
cd oracle/views
sqlplus billing7/billing@bm7 @install_all_views.sql
```

## Преимущества новой структуры

1. ✅ **Четкое разделение по назначению**: tables, views, functions, data
2. ✅ **Разделение БД**: oracle/ vs postgresql/
3. ✅ **Модульность**: каждый view в отдельном файле
4. ✅ **Документация**: все в docs/ с навигацией через INDEX.md
5. ✅ **Python**: отдельная папка для всех скриптов
6. ✅ **Порядок установки**: мастер-скрипт install_all_views.sql
7. ✅ **Версионность**: каждый view с номером (01_, 02_, ...)

## Следующие шаги

1. Создать `oracle/tables/` с DDL для всех таблиц
2. Создать `postgresql/` структуру (аналогично oracle/)
3. Добавить README.md в python/
4. Создать integration tests

## Совместимость

Все существующие скрипты и Streamlit приложение продолжают работать:
- `streamlit_report.py` использует представления по именам
- `streamlit_report_oracle.py` адаптирован для Oracle
- Python загрузчики не изменились (только перемещены)

## Документация

- См. [README.md](README.md) - главное руководство
- См. [oracle/README.md](oracle/README.md) - детали Oracle
- См. [docs/INDEX.md](docs/INDEX.md) - навигация по документам
- См. [docs/billing_integration.md](docs/billing_integration.md) - база знаний биллинга


