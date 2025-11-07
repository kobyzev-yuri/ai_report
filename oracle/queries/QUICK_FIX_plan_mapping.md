# Быстрое исправление маппинга планов в Oracle

## Проблема
После обновления view план все еще NULL для записи:
- CONTRACT_ID: SUB-38494828046
- IMEI: 300215061804050
- BILL_MONTH: 202510

## Причина
Возможные причины:
1. PLAN_NAME может быть пустой строкой '', а не NULL
2. Маппинг не находит совпадений из-за проблем с JOIN
3. View не был пересоздан правильно

## Решение

### Шаг 1: Проверка данных

Выполните диагностический запрос:
```bash
sqlplus billing7/billing@bm7 @oracle/queries/test_plan_mapping.sql
```

Это покажет:
- Есть ли планы в других периодах
- Работает ли маппинг
- Что возвращает view

### Шаг 2: Применение исправления

Исправление уже добавлено в файл - теперь обрабатываются пустые строки:

```sql
-- Обновленная логика PLAN_NAME
NVL(
    NULLIF(TRIM(sp.PLAN_NAME), ''),
    NVL(
        NULLIF(TRIM(st.STECCOM_PLAN_NAME_MONTHLY), ''),
        NVL(
            NULLIF(TRIM(cpm.PLAN_NAME), ''),
            NULLIF(TRIM(ipm.PLAN_NAME), '')
        )
    )
) AS PLAN_NAME
```

### Шаг 3: Пересоздание view

```bash
# Синхронизация исправленного файла
./sync_to_vz2.sh

# Применение на сервере
ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && sqlplus billing7/billing@bm7 @oracle/views/02_v_consolidated_overage_report.sql"
```

### Шаг 4: Проверка результата

```sql
SELECT 
    imei, 
    contract_id, 
    bill_month, 
    plan_name 
FROM V_CONSOLIDATED_OVERAGE_REPORT 
WHERE contract_id = 'SUB-38494828046'
  AND imei = '300215061804050'
  AND bill_month = '202510';
```

## Альтернативное решение (если маппинг не работает)

Если маппинг все еще не работает, можно временно обновить данные напрямую:

```sql
-- ВНИМАНИЕ: Это временное решение! Используйте только если маппинг не работает
-- Найти план из других периодов
SELECT DISTINCT plan_name 
FROM SPNET_TRAFFIC 
WHERE contract_id = 'SUB-38494828046'
  AND plan_name IS NOT NULL 
  AND plan_name != ''
ORDER BY plan_name;

-- Если нашли план (например, 'LBS Demo'), можно обновить напрямую:
-- НО это не рекомендуется, лучше исправить view!
```

## Проверка всех проблемных записей

```sql
-- Проверить все записи за октябрь 2025 без плана
SELECT 
    imei,
    contract_id,
    bill_month,
    plan_name,
    spnet_total_amount
FROM V_CONSOLIDATED_OVERAGE_REPORT
WHERE bill_month = '202510'
  AND (plan_name IS NULL OR plan_name = '')
ORDER BY contract_id;
```

