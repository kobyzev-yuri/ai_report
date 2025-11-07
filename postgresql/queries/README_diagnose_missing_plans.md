# Диагностика отсутствующих тарифных планов

## Описание

SQL-скрипт `diagnose_missing_tariff_plans.sql` предназначен для выявления причин отсутствия тарифных планов у записей в консолидированном отчете.

## Проблема

В периоде 2025-10 (202510) обнаружены записи без тарифного плана (plan_name = NULL) в представлении `V_CONSOLIDATED_OVERAGE_REPORT`.

## Источники данных для PLAN_NAME

Тарифный план может быть получен из следующих источников:

1. **SPNET_TRAFFIC.PLAN_NAME** - основной источник из данных SPNet
2. **STECCOM_EXPENSES.PLAN_DISCOUNT** - резервный источник из данных STECCOM
3. **Маппинг по CONTRACT_ID** - из других периодов SPNET_TRAFFIC
4. **Маппинг по IMEI** - из других периодов SPNET_TRAFFIC  
5. **Маппинг по TARIFF_ID** - из IRIDIUM_SERVICES_INFO через связь с консолидированным отчетом

## Использование

### Основной диагностический запрос

Запустите первый запрос в файле - он покажет:

- **spnet_records** - количество записей в SPNET_TRAFFIC для периода 202510
- **spnet_plan_names** - найденные планы в SPNET_TRAFFIC
- **spnet_status** - статус данных SPNet (есть/нет/пусто)
- **steccom_records** - количество записей в STECCOM_EXPENSES
- **steccom_plan_names** - найденные планы в STECCOM_EXPENSES
- **steccom_status** - статус данных STECCOM
- **billing_records** - данные из биллинга (IRIDIUM_SERVICES_INFO)
- **tariff_ids** - TARIFF_ID из биллинга (можно использовать для маппинга)
- **plans_in_other_periods** - есть ли планы в других периодах
- **plan_name_in_report** - что показывает консолидированный отчет
- **diagnosis** - автоматический диагноз проблемы

### Дополнительные запросы

После основного запроса можно запустить три дополнительных запроса для проверки маппингов:

1. **Маппинг по CONTRACT_ID** - показывает, какие планы есть для этих contract_id в других периодах
2. **Маппинг по IMEI** - показывает, какие планы есть для этих IMEI в других периодах
3. **Маппинг по TARIFF_ID** - показывает связь между tariff_id и планами

## Возможные причины отсутствия плана

1. **Нет данных в SPNET_TRAFFIC** - запись отсутствует в таблице SPNET_TRAFFIC для периода 202510
2. **PLAN_NAME = NULL в SPNET_TRAFFIC** - данные есть, но поле PLAN_NAME пустое
3. **Нет данных в STECCOM_EXPENSES** - запись отсутствует в таблице STECCOM_EXPENSES
4. **PLAN_DISCOUNT = NULL в STECCOM_EXPENSES** - данные есть, но поле PLAN_DISCOUNT пустое
5. **Нет маппинга** - нет данных ни в одном из источников, включая другие периоды

## Решения

### Если есть планы в других периодах

Можно использовать маппинг по CONTRACT_ID или IMEI из других периодов. Это уже реализовано в `V_CONSOLIDATED_REPORT_WITH_BILLING` через CTE:
- `contract_plan_mapping`
- `imei_plan_mapping`

### Если есть TARIFF_ID в биллинге

Можно использовать маппинг по TARIFF_ID. Это реализовано в `V_CONSOLIDATED_REPORT_WITH_BILLING` через CTE:
- `tariff_plan_mapping`

### Если нет данных вообще

Требуется:
1. Проверить, загружены ли данные за период 202510
2. Проверить исходные файлы SPNet и STECCOM
3. Проверить процесс загрузки данных

## Пример использования

```sql
-- Запустить основной диагностический запрос
\i postgresql/queries/diagnose_missing_tariff_plans.sql

-- Или в psql:
psql -d your_database -f postgresql/queries/diagnose_missing_tariff_plans.sql
```

## Обновление списка проблемных записей

Если нужно проверить другие записи, обновите CTE `problem_records` в начале файла, добавив или изменив записи:

```sql
WITH problem_records AS (
    SELECT 
        '202510' AS bill_month,
        'IMEI_VALUE' AS imei,
        'CONTRACT_ID_VALUE' AS contract_id
    UNION ALL SELECT '202510', 'IMEI2', 'CONTRACT_ID2'
    ...
)
```

