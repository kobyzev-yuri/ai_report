# Обновление V_CONSOLIDATED_OVERAGE_REPORT - Маппинг тарифных планов

## Дата: 2025-01-XX

## Проблема
В периоде 2025-10 (202510) обнаружены записи без тарифного плана (PLAN_NAME = NULL) в представлении `V_CONSOLIDATED_OVERAGE_REPORT`, хотя в других периодах для тех же IMEI/CONTRACT_ID планы есть.

## Решение
Добавлен автоматический маппинг тарифных планов из других периодов:
1. Если PLAN_NAME отсутствует в текущем периоде (SPNET_TRAFFIC или STECCOM_EXPENSES)
2. Система автоматически берет план из других периодов по приоритету:
   - Сначала по CONTRACT_ID (самый частый план для этого контракта)
   - Если нет - по IMEI (самый частый план для этого IMEI)

## Изменения в коде

### Oracle версия
Файл: `oracle/views/02_v_consolidated_overage_report.sql`

Добавлены CTE:
- `contract_plan_mapping` - маппинг по CONTRACT_ID
- `imei_plan_mapping` - маппинг по IMEI

Обновлена логика PLAN_NAME:
```sql
NVL(
    NVL(sp.PLAN_NAME, st.STECCOM_PLAN_NAME_MONTHLY),
    NVL(cpm.PLAN_NAME, ipm.PLAN_NAME)
) AS PLAN_NAME
```

### PostgreSQL версия
Файл: `postgresql/views/02_v_consolidated_overage_report.sql`

Аналогичные изменения с использованием `COALESCE` вместо `NVL`.

## Как применить

### Oracle (Production)
```sql
-- Подключиться к Oracle
sqlplus billing7/billing@bm7

-- Выполнить обновление представления
@oracle/views/02_v_consolidated_overage_report.sql
```

### PostgreSQL (Testing)
```bash
psql -U postgres -d billing -f postgresql/views/02_v_consolidated_overage_report.sql
```

## Проверка

После применения обновления проверить, что записи за октябрь 2025 теперь имеют план:

```sql
-- Oracle
SELECT imei, contract_id, bill_month, plan_name 
FROM V_CONSOLIDATED_OVERAGE_REPORT 
WHERE bill_month = '202510' 
  AND contract_id IN ('SUB-51636947303', 'SUB-51637724351')
ORDER BY contract_id;

-- PostgreSQL
SELECT imei, contract_id, bill_month, plan_name 
FROM v_consolidated_overage_report 
WHERE bill_month = '202510' 
  AND contract_id IN ('SUB-51636947303', 'SUB-51637724351')
ORDER BY contract_id;
```

Все записи должны иметь план "LBS 1" или "LBS Demo" (в зависимости от того, какой план был в других периодах).

## Обратная совместимость

Изменения полностью обратно совместимы:
- Если план есть в текущем периоде - используется он
- Если плана нет - используется маппинг из других периодов
- Если маппинга нет - PLAN_NAME остается NULL (как и раньше)

## Примечания

- Маппинг берет самый частый план для каждого CONTRACT_ID/IMEI из всех периодов
- Если для одного CONTRACT_ID/IMEI есть несколько планов с одинаковой частотой - берется первый по алфавиту
- Маппинг работает только для записей, где PLAN_NAME был NULL в исходных данных

