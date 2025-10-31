# Oracle Database Scripts - Iridium M2M Reporting

## Структура папок

```
oracle/
├── tables/          # DDL для создания таблиц (создаются в схеме отчетности)
├── views/           # Представления для отчетов
├── functions/       # PL/SQL функции
├── data/           # Данные для справочных таблиц
└── README.md       # Этот файл
```

## Порядок установки

### 1. Таблицы (в схеме отчетности, например billing7)
```bash
# Основные таблицы данных
sqlplus billing7/billing@bm7 @tables/01_create_spnet_traffic.sql
sqlplus billing7/billing@bm7 @tables/02_create_steccom_expenses.sql
sqlplus billing7/billing@bm7 @tables/03_create_tariff_plans.sql
```

### 2. Данные справочников
```bash
# Загрузка тарифных планов
sqlplus billing7/billing@bm7 @data/tariff_plans_data.sql
```

### 3. Функции
```bash
# PL/SQL функция расчета превышения
sqlplus billing7/billing@bm7 @functions/calculate_overage.sql
```

### 4. Представления
```bash
# Установка всех представлений в правильном порядке
cd views
sqlplus billing7/billing@bm7 @install_all_views.sql
```

## Описание представлений

### V_SPNET_OVERAGE_ANALYSIS
- **Назначение**: Базовый анализ превышения трафика по IMEI
- **Источник**: SPNET_TRAFFIC + TARIFF_PLANS
- **Ключевые поля**: IMEI, CONTRACT_ID, BILL_MONTH, PLAN_NAME, OVERAGE_KB, CALCULATED_OVERAGE_CHARGE
- **Файл**: `views/01_v_spnet_overage_analysis.sql`

### V_CONSOLIDATED_OVERAGE_REPORT
- **Назначение**: Консолидированный отчет с данными SPNet и STECCOM
- **Источник**: V_SPNET_OVERAGE_ANALYSIS + STECCOM_EXPENSES
- **Ключевые поля**: все из V_SPNET_OVERAGE_ANALYSIS + STECCOM_TOTAL_AMOUNT
- **Файл**: `views/02_v_consolidated_overage_report.sql`
- **Использование**: Основной отчет для Streamlit

### V_IRIDIUM_SERVICES_INFO
- **Назначение**: Информация о сервисах из биллинга
- **Источник**: SERVICES + ACCOUNTS + CUSTOMERS + BM_CUSTOMER_CONTACT + OUTER_IDS
- **Ключевые поля**: CONTRACT_ID, IMEI, CUSTOMER_NAME, AGREEMENT_NUMBER, ORDER_NUMBER, CODE_1C
- **Файл**: `views/03_v_iridium_services_info.sql`
- **Требования**: Доступ к таблицам биллинга (SERVICES, ACCOUNTS, CUSTOMERS и др.)

### V_CONSOLIDATED_REPORT_WITH_BILLING
- **Назначение**: Расширенный отчет с данными клиентов из биллинга
- **Источник**: V_CONSOLIDATED_OVERAGE_REPORT + V_IRIDIUM_SERVICES_INFO
- **Ключевые поля**: все из V_CONSOLIDATED_OVERAGE_REPORT + CUSTOMER_NAME, AGREEMENT_NUMBER, CODE_1C
- **Файл**: `views/04_v_consolidated_report_with_billing.sql`
- **Использование**: Отчет с полной информацией о клиентах

## Зависимости представлений

```
SPNET_TRAFFIC ──┐
TARIFF_PLANS ───┼──> V_SPNET_OVERAGE_ANALYSIS ──┐
                                                   ├──> V_CONSOLIDATED_OVERAGE_REPORT ──┐
STECCOM_EXPENSES ──────────────────────────────────┘                                     │
                                                                                          ├──> V_CONSOLIDATED_REPORT_WITH_BILLING
SERVICES ────┐                                                                            │
ACCOUNTS ────┤                                                                            │
CUSTOMERS ───┼──> V_IRIDIUM_SERVICES_INFO ────────────────────────────────────────────────┘
BM_* ────────┤
OUTER_IDS ───┘
```

## Использование в Streamlit

### Для отчета без данных биллинга
```python
query = "SELECT * FROM V_CONSOLIDATED_OVERAGE_REPORT WHERE BILL_MONTH = :month"
```

### Для отчета с данными клиентов
```python
query = """
    SELECT 
        CUSTOMER_NAME, CODE_1C, AGREEMENT_NUMBER, ORDER_NUMBER,
        IMEI, PLAN_NAME, BILL_MONTH,
        SPNET_TOTAL_AMOUNT, CALCULATED_OVERAGE, STECCOM_TOTAL_AMOUNT
    FROM V_CONSOLIDATED_REPORT_WITH_BILLING
    WHERE BILL_MONTH = :month
    ORDER BY CUSTOMER_NAME
"""
```

## Примечания

1. **V_IRIDIUM_SERVICES_INFO** и **V_CONSOLIDATED_REPORT_WITH_BILLING** требуют доступ к таблицам биллинга (billing7@bm7)
2. Если доступа к биллингу нет, используйте только **V_CONSOLIDATED_OVERAGE_REPORT**
3. Все представления используют LEFT JOIN, поэтому NULL значения возможны
4. BILL_MONTH конвертируется в формат YYYYMM для совместимости
5. Расчет превышения только для SBD Data Usage с активными тарифами


