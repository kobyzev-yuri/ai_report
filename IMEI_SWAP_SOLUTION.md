# Решение проблемы со свопами IMEI и переносом авансов

## Анализ данных

### SUB-58894398397

**Авансы:**
- 2025-09: 25 на IMEI `300234069508860`
- 2025-10: 25 на IMEI `300234069508860`
- 2025-11: 12.5 на IMEI `300234069900500`

**Трафик:**
- 2025-09: 0 на IMEI `300234069508860`
- 2025-10: 0 на IMEI `300234069508860`
- 2025-11: 14688 на IMEI `300234069900500` (своп!)

**Отчет (текущее состояние):**
- 2025-09: FEE_ADVANCE_CHARGE_PREVIOUS_MONTH=12.5 на IMEI `300234069508860`
- 2025-10: FEE_ADVANCE_CHARGE_PREVIOUS_MONTH=**0** на IMEI `300234069900500` ❌ (аванс за сентябрь не перешел!)
- 2025-11: FEE_ADVANCE_CHARGE_PREVIOUS_MONTH=12.5 на IMEI `300234069508860` (аванс за октябрь перешел, но на старый IMEI)

**Проблема:** Аванс за сентябрь (12.5) не перешел в октябрь из-за свопа IMEI.

### SUB-59307360952

**Авансы:**
- 2025-10: 7 на IMEI `300234069606340`
- 2025-11: 3.5 на IMEI `300234069500050` (своп!)

**Трафик:**
- 2025-10: 0 на IMEI `300234069606340`
- 2025-11: 575 на IMEI `300234069500050` (своп!)

**Отчет (текущее состояние):**
- 2025-10: FEE_ADVANCE_CHARGE_PREVIOUS_MONTH=0 на IMEI `300234069500050` ❌
- 2025-11: FEE_ADVANCE_CHARGE_PREVIOUS_MONTH=3.5 на IMEI `300234069606340` (аванс за октябрь остался на старом IMEI)

**Проблема:** Аванс за октябрь (7) не перешел в ноябрь на новый IMEI из-за свопа.

## Решение

Убрать условие `AND cor.IMEI = sf_prev.imei` из JOIN для авансов за предыдущий месяц. Аванс должен переходить по `CONTRACT_ID`, а IMEI для отображения берется из текущего периода (из трафика).

### Изменения в VIEW

#### Изменение 1: Основной SELECT (строка 206)

**Было:**
```sql
LEFT JOIN steccom_fees sf_prev
    ON sf_prev.bill_month = CASE 
           WHEN cor.BILL_MONTH IS NOT NULL AND LENGTH(TRIM(cor.BILL_MONTH)) >= 6 THEN
               TO_CHAR(ADD_MONTHS(TO_DATE(SUBSTR(TRIM(cor.BILL_MONTH), 1, 6), 'YYYYMM'), -1), 'YYYYMM')
           ELSE
               NULL
       END
    AND RTRIM(cor.CONTRACT_ID) = RTRIM(sf_prev.CONTRACT_ID)
    AND cor.IMEI = sf_prev.imei  -- ⚠️ УБРАТЬ
```

**Станет:**
```sql
-- Advance Charge за предыдущий месяц
-- ВАЖНО: Аванс переходит по CONTRACT_ID, а не по IMEI
-- Это позволяет корректно обрабатывать свопы IMEI (замены IMEI на SUB)
-- IMEI для отображения берется из текущего периода (cor.IMEI)
LEFT JOIN steccom_fees sf_prev
    ON sf_prev.bill_month = CASE 
           WHEN cor.BILL_MONTH IS NOT NULL AND LENGTH(TRIM(cor.BILL_MONTH)) >= 6 THEN
               TO_CHAR(ADD_MONTHS(TO_DATE(SUBSTR(TRIM(cor.BILL_MONTH), 1, 6), 'YYYYMM'), -1), 'YYYYMM')
           ELSE
               NULL
       END
    AND RTRIM(cor.CONTRACT_ID) = RTRIM(sf_prev.CONTRACT_ID)
    -- УБРАНО: AND cor.IMEI = sf_prev.imei
    -- Причина: при свопе IMEI аванс должен переходить по CONTRACT_ID,
    -- а IMEI для отображения берется из текущего периода (cor.IMEI)
```

#### Изменение 2: UNION ALL часть (строка 305)

**Было:**
```sql
LEFT JOIN steccom_fees sf_next
    ON sf_next.bill_month = TO_CHAR(ADD_MONTHS(TO_DATE(sf_prev.bill_month, 'YYYYMM'), 1), 'YYYYMM')
    AND RTRIM(sf_prev.CONTRACT_ID) = RTRIM(sf_next.CONTRACT_ID)
    AND sf_prev.imei = sf_next.imei  -- ⚠️ УБРАТЬ
```

**Станет:**
```sql
-- Аванс за следующий месяц (для UNION ALL части)
-- ВАЖНО: Аванс переходит по CONTRACT_ID, а не по IMEI
LEFT JOIN steccom_fees sf_next
    ON sf_next.bill_month = TO_CHAR(ADD_MONTHS(TO_DATE(sf_prev.bill_month, 'YYYYMM'), 1), 'YYYYMM')
    AND RTRIM(sf_prev.CONTRACT_ID) = RTRIM(sf_next.CONTRACT_ID)
    -- УБРАНО: AND sf_prev.imei = sf_next.imei
    -- Причина: при свопе IMEI аванс должен переходить по CONTRACT_ID
```

#### Изменение 3: NOT EXISTS проверка (строка 319)

**Было:**
```sql
WHERE cor_check.IMEI = sf_prev.imei
    AND RTRIM(cor_check.CONTRACT_ID) = RTRIM(sf_prev.CONTRACT_ID)
```

**Станет:**
```sql
-- Проверяем по CONTRACT_ID, а не по IMEI, чтобы корректно обрабатывать свопы
WHERE RTRIM(cor_check.CONTRACT_ID) = RTRIM(sf_prev.CONTRACT_ID)
    -- УБРАНО: cor_check.IMEI = sf_prev.imei
    -- Причина: при свопе IMEI проверяем только CONTRACT_ID
```

## Ожидаемый результат после исправления

### SUB-58894398397

**После исправления:**
- 2025-10: FEE_ADVANCE_CHARGE_PREVIOUS_MONTH=**12.5** на IMEI `300234069900500` ✅ (аванс за сентябрь перешел на новый IMEI)
- 2025-11: FEE_ADVANCE_CHARGE_PREVIOUS_MONTH=12.5 на IMEI `300234069508860` (аванс за октябрь перешел на старый IMEI)

### SUB-59307360952

**После исправления:**
- 2025-11: FEE_ADVANCE_CHARGE_PREVIOUS_MONTH=**7** на IMEI `300234069500050` ✅ (аванс за октябрь перешел на новый IMEI)

## Важные замечания

### Потенциальная проблема: дублирование авансов

Если для одного CONTRACT_ID в одном периоде есть несколько IMEI (например, после свопа), аванс может дублироваться на каждую строку с IMEI.

**Решение:** Это нормально, так как каждая строка соответствует отдельному IMEI. Аванс должен быть виден на каждом IMEI, который использовался в периоде.

### Проверка после исправления

После внесения изменений проверить:

1. **SUB-58894398397:**
   - Аванс за 2025-09 должен появиться в 2025-10 на IMEI `300234069900500`
   - Аванс за 2025-10 должен появиться в 2025-11 на IMEI `300234069508860`

2. **SUB-59307360952:**
   - Аванс за 2025-10 должен появиться в 2025-11 на IMEI `300234069500050`

3. **Обычные случаи (без свопов):**
   - Убедиться, что логика не сломалась для обычных случаев

## SQL для проверки после исправления

```sql
-- Проверка: авансы должны переходить по CONTRACT_ID
SELECT 
    FINANCIAL_PERIOD,
    CONTRACT_ID,
    IMEI,
    FEE_ADVANCE_CHARGE_PREVIOUS_MONTH,
    COUNT(*) OVER (PARTITION BY CONTRACT_ID, FINANCIAL_PERIOD) AS ADVANCE_COUNT
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE CONTRACT_ID IN ('SUB-58894398397', 'SUB-59307360952')
  AND FINANCIAL_PERIOD IN ('2025-09', '2025-10', '2025-11')
  AND FEE_ADVANCE_CHARGE_PREVIOUS_MONTH > 0
ORDER BY CONTRACT_ID, FINANCIAL_PERIOD, IMEI;
```

