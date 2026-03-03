# Iridium M2M KB Assistant

Система интеллектуального интерфейса к финансовым отчетам по услугам Iridium M2M с расчетом превышений трафика, интеграцией с биллингом и базой знаний (KB) для автоматической генерации SQL запросов.

## 🎯 Основные компоненты

- **База знаний (KB) по billing** - структурированная информация о схеме базы данных для обучения AI-агента генерации SQL запросов
- **RAG-ассистент** - интеллектуальный помощник для генерации SQL запросов на основе естественного языка с использованием LLM (GPT-4o)
- **Финансовый анализ** - готовые представления (VIEW) для анализа прибыльности, убыточности и тенденций
- **Базовые финансовые отчеты** - представления (VIEW) для анализа затрат, доходов и себестоимости
- **Oracle Database (Production)** - основная база данных для хранения данных и выполнения запросов

## 🚀 Быстрый старт

### Установка Oracle базы данных

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
# Установка зависимостей
pip install -r requirements.txt

# Запуск в фоновом режиме (Oracle)
./run_streamlit_background.sh

# Или запуск напрямую
streamlit run streamlit_report_oracle_backup.py \
  --server.port 8504 \
  --server.headless true \
  --server.baseUrlPath=/ai_report \
  --server.enableCORS false \
  --server.enableXsrfProtection false
```

**Управление:**
```bash
# Проверка статуса
./status_streamlit.sh

# Остановка
./stop_streamlit.sh
```

Приложение доступно через Nginx: **stat.steccom.ru:7776/ai_report**

## 🚀 Развертывание на сервере

Подробные инструкции: **[docs/deploy.md](docs/deploy.md)** — схема поддержки и тестирования, синхронизация, перезапуск на сервере (без Docker). Вся документация: **[docs/](docs/README.md)**.

Кратко: `./prepare_deployment.sh` → `SSH_CMD="ssh -p 1194" ./sync_deploy.sh root@82.114.2.2` → на сервере `./restart_streamlit.sh`.

### Подключение к Oracle

Для подключения к Oracle серверу настройте параметры в `config.env`:
- `ORACLE_HOST` - адрес сервера Oracle
- `ORACLE_PORT` - порт (обычно 1521)
- `ORACLE_SERVICE` - имя сервиса
- `ORACLE_USER` и `ORACLE_PASSWORD` - учетные данные

Если Oracle сервер находится в локальной сети и недоступен напрямую, используйте SSH туннель или VPN для доступа.

## 📊 Основные интерфейсы

### 1. Streamlit Web Interface (Iridium M2M KB Assistant)

**Файлы:**
- `streamlit_report_oracle_backup.py` - Oracle версия (production)

**Основные функции:**
- **🤖 Ассистент** - интеллектуальный помощник для генерации SQL запросов на естественном языке
  - Использует RAG (Retrieval-Augmented Generation) с векторной базой Qdrant
  - Интеграция с LLM (GPT-4o через proxyapi.ru) для генерации SQL
  - Автоматическая валидация SQL и исправление ошибок
  - Поиск похожих примеров из базы знаний
  - Выполнение SQL с анализом производительности (EXPLAIN PLAN, статистика)
- **📈 Финансовый анализ** - углубленный анализ прибыльности и тенденций
  - Выявление убыточных клиентов
  - Анализ динамики прибыльности по периодам
  - Выявление тенденций к ухудшению прибыльности
  - Клиенты с низкой маржой
  - Использует готовые VIEW для быстрого анализа
- **Главный отчет** - сводная таблица по IMEI с расчетом превышений
- **Data Loader** - загрузка CSV/Excel файлов (SPNet Traffic и STECCOM Access Fees)
  - Поддержка CSV и XLSX файлов
  - Автоматический контроль количества записей (в файле vs в базе)
  - Предупреждения о неполных загрузках
  - Автоматическая перезагрузка файлов с неполными данными
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
  - Количество записей в файлах и в базе

**Использование:**
1. Откройте браузер: `stat.steccom.ru:7776/ai_report`
2. Выберите период в боковой панели
3. Примените фильтры (опционально)
4. Просмотрите данные в таблице
5. Экспортируйте при необходимости
6. Используйте вкладку "Data Loader" для загрузки новых файлов

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
- **ВАЖНО:** Включает авансы за предыдущий месяц (`FEE_ADVANCE_CHARGE_PREVIOUS_MONTH`)
  - Авансы за месяц X отображаются в отчете за месяц X+1 (финансовый период)
  - Это позволяет видеть авансы даже если IMEI был выключен в следующем месяце

**Пример запроса:**
```sql
SELECT customer_name, code_1c, agreement_number,
       imei, plan_name, bill_month, financial_period,
       fee_advance_charge_previous_month,
       spnet_total_amount, calculated_overage, steccom_total_amount
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE financial_period = '2025-10'
ORDER BY customer_name;
```

#### V_PROFITABILITY_BY_PERIOD
Базовая прибыльность клиентов по периодам:
- Расходы в USD и RUB (с конверсией через курс из счетов-фактур)
- Доходы в RUB
- Прибыль, маржа и себестоимость в процентах
- Статус: UNPROFITABLE, LOW_MARGIN, PROFITABLE

**Пример запроса:**
```sql
SELECT PERIOD, CUSTOMER_NAME, CODE_1C, 
       EXPENSES_USD, EXPENSES_RUB, REVENUE_RUB, 
       PROFIT_RUB, MARGIN_PCT, STATUS
FROM V_PROFITABILITY_BY_PERIOD
WHERE PERIOD = '2025-10'
ORDER BY PROFIT_RUB DESC;
```

#### V_PROFITABILITY_TREND
Тенденции прибыльности с сравнением периодов:
- Все поля из V_PROFITABILITY_BY_PERIOD
- + сравнение с предыдущим периодом (LAG функция)
- TREND: DECREASE (ухудшение), INCREASE (улучшение)

**Пример запроса:**
```sql
SELECT PERIOD, CUSTOMER_NAME, PREV_PROFIT_RUB, PROFIT_RUB, 
       PROFIT_CHANGE, PROFIT_CHANGE_PCT, TREND
FROM V_PROFITABILITY_TREND
WHERE TREND = 'DECREASE'
ORDER BY PROFIT_CHANGE ASC;
```

#### V_UNPROFITABLE_CUSTOMERS
Убыточные клиенты и клиенты с низкой маржой:
- Фильтрует только проблемные позиции (UNPROFITABLE, LOW_MARGIN)
- ALERT_TYPE: LOSS (убыток), LOW_MARGIN (низкая маржа <10%)

**Пример запроса:**
```sql
SELECT PERIOD, CUSTOMER_NAME, CODE_1C, 
       PROFIT_RUB, MARGIN_PCT, ALERT_TYPE
FROM V_UNPROFITABLE_CUSTOMERS
WHERE PERIOD = '2025-10' AND ALERT_TYPE = 'LOSS'
ORDER BY PROFIT_RUB ASC;
```

### 3. Python скрипты загрузки данных

#### load_spnet_traffic.py
Загрузка данных трафика из отчетов SPNet.

**Источник:** `data/SPNet reports/*.xlsx` или `*.csv`

**Назначение:** Заполнение таблицы `SPNET_TRAFFIC`

**Особенности:**
- Автоматически пропускает уже загруженные файлы
- Проверяет количество записей в файле и в базе
- Перезагружает файлы с неполными данными
- Поддерживает CSV и XLSX форматы

**Использование:**
```bash
cd python
python load_spnet_traffic.py
```

#### load_steccom_expenses.py
Загрузка данных расходов из отчетов STECCOM.

**Источник:** `data/STECCOMLLCRussiaSBD.AccessFees_reports/*.csv`

**Назначение:** Заполнение таблицы `STECCOM_EXPENSES`

**Особенности:**
- Автоматически пропускает уже загруженные файлы
- Проверяет количество записей в файле и в базе
- Перезагружает файлы с неполными данными

**Использование:**
```bash
cd python
python load_steccom_expenses.py
```

#### restore_load_history.py
Восстановление истории загрузок в таблице `LOAD_LOGS` (Oracle).

**Использование:**
```bash
# Проверка отсутствующих записей
python restore_load_history.py --check-only

# Восстановление истории
python restore_load_history.py
```

## 📁 Структура проекта

```
ai_report/
├── oracle/                      # Oracle Database (PRODUCTION)
│   ├── tables/                  # DDL таблиц
│   ├── views/                   # Представления для отчетов
│   │   ├── 01_v_spnet_overage_analysis.sql
│   │   ├── 02_v_consolidated_overage_report.sql
│   │   ├── 03_v_iridium_services_info.sql
│   │   ├── 04_v_consolidated_report_with_billing.sql
│   │   ├── 05_v_cost_of_goods_sold.sql      # Финансовый отчет: себестоимость
│   │   └── 05_v_revenue_from_invoices.sql   # Финансовый отчет: доходы
│   ├── functions/               # PL/SQL функции
│   ├── queries/                 # Примеры SQL запросов
│   ├── data/                    # Справочные данные
│   └── README.md                # Документация по Oracle
│
├── kb_billing/                  # База знаний (KB) для AI-агента
│   ├── metadata.json            # Метаданные схемы billing
│   ├── tables/                  # Описания таблиц (JSON)
│   ├── views/                   # Описания представлений (JSON)
│   ├── training_data/           # Q/A примеры для обучения
│   └── README.md                # Документация по KB
│
├── python/                      # Python модули
│   ├── load_spnet_traffic.py   # Загрузка данных SPNet
│   ├── load_steccom_expenses.py # Загрузка данных STECCOM
│   ├── calculate_overage.py    # Python модуль расчета превышений
│   └── restore_load_history.py  # Восстановление истории загрузок
│
├── docs/                        # Документация (развёртывание, права, отладка)
│   ├── README.md                # Оглавление
│   ├── deploy.md                # Развёртывание и поддержка
│   ├── auth.md, tab-permissions.md, restart-streamlit.md и др.
│
├── tests/                       # Тесты (при наличии)
│
├── archive/                     # Архив (включая PostgreSQL файлы)
│   └── postgresql_migration_*/  # Архив PostgreSQL файлов
│
├── streamlit_report_oracle_backup.py      # Streamlit приложение (Oracle)
├── streamlit_data_loader.py              # Модуль загрузки данных
├── db_connection.py                      # Модуль подключения к Oracle
├── run_streamlit_background.sh           # Скрипт запуска в фоне
├── stop_streamlit.sh                     # Скрипт остановки
├── status_streamlit.sh                   # Скрипт проверки статуса
├── requirements.txt                      # Python зависимости
├── config.env.example                   # Пример конфигурации Oracle
└── README.md                             # Этот файл
```

## 🛠️ Технологии

- **Oracle 11g+** - production база данных
- **Python 3.10+** - загрузка данных и расчеты
- **Streamlit** - веб-интерфейс для отчетов
- **Pandas** - обработка данных
- **cx_Oracle** - Oracle драйвер
- **openpyxl** - работа с Excel файлами
- **KB (Knowledge Base)** - база знаний для обучения AI-агента генерации SQL запросов

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

Вся актуальная документация: **[docs/](docs/README.md)** (развёртывание, сервер, права, отладка).

## 📚 База знаний (KB) по Billing

Проект включает структурированную базу знаний (`kb_billing/`) для обучения AI-агента генерации SQL запросов:

- **Метаданные схемы** - описание структуры базы данных billing
- **Таблицы** - JSON описания всех таблиц с бизнес-логикой
- **Представления (VIEW)** - описания финансовых отчетов и аналитических представлений
- **Training Data** - примеры вопросов и SQL запросов для обучения

Подробнее: `kb_billing/README.md`

## 📊 Финансовые отчеты

Система включает базовые финансовые отчеты, которые являются основой для интеллектуального интерфейса:

### Основные отчеты:
- **V_CONSOLIDATED_REPORT_WITH_BILLING** - сводный отчет с данными клиентов и затратами
- **V_COST_OF_GOODS_SOLD** - расчет себестоимости услуг
- **V_REVENUE_FROM_INVOICES** - доходы из счетов-фактур

Эти отчеты используются как основа для:
- Анализа финансовых показателей
- Экспорта данных в 1С
- Генерации SQL запросов через AI-агента

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
5. **PostgreSQL файлы** перемещены в `archive/postgresql_migration_*/` - проект теперь использует только Oracle

## 🔄 Основные операции

### Ежедневная загрузка данных

**Через веб-интерфейс:**
1. Откройте вкладку "Data Loader" в Streamlit
2. Загрузите файлы SPNet Traffic или STECCOM Access Fees
3. Файлы автоматически обработаются и загрузятся в базу

**Через Python скрипты:**
```bash
# 1. Загрузить новые данные SPNet
cd python
python load_spnet_traffic.py

# 2. Загрузить новые данные STECCOM
python load_steccom_expenses.py
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
