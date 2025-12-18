-- ============================================================================
-- check_350_1k_overage.sql
-- Проверка расчета Calc overage для плана "SBD Tiered 350 1K"
-- ============================================================================

SET PAGESIZE 200
SET FEEDBACK ON
SET HEADING ON
SET LINESIZE 2000

PROMPT === 1. Проверка наличия тарифа "SBD Tiered 350 1K" в TARIFF_PLANS ===

SELECT 
    PLAN_NAME,
    PLAN_CODE,
    INCLUDED_KB,
    TIER1_FROM_KB,
    TIER1_TO_KB,
    TIER1_PRICE_USD,
    TIER2_FROM_KB,
    TIER2_TO_KB,
    TIER2_PRICE_USD,
    TIER3_FROM_KB,
    TIER3_PRICE_USD,
    ACTIVE
FROM TARIFF_PLANS
WHERE PLAN_NAME = 'SBD Tiered 350 1K';

PROMPT
PROMPT === 2. Проверка функции CALCULATE_OVERAGE для "SBD Tiered 350 1K" ===
PROMPT Тест: 2.18 KB (2180 bytes) - должно быть превышение 1.18 KB

SELECT 
    'SBD Tiered 350 1K, 2.18 KB (2180 bytes)' AS test_case,
    CALCULATE_OVERAGE('SBD Tiered 350 1K', 2180) AS calculated_overage,
    1.77 AS expected_overage,
    ROUND(CALCULATE_OVERAGE('SBD Tiered 350 1K', 2180) - 1.77, 2) AS diff,
    'Ожидается: 1.18 KB × $1.50 = $1.77' AS explanation
FROM DUAL
UNION ALL
SELECT 
    'SBD Tiered 350 1K, 5 KB (5000 bytes)',
    CALCULATE_OVERAGE('SBD Tiered 350 1K', 5000),
    6.00,
    ROUND(CALCULATE_OVERAGE('SBD Tiered 350 1K', 5000) - 6.00, 2),
    'Ожидается: 4 KB × $1.50 = $6.00'
FROM DUAL
UNION ALL
SELECT 
    'SBD Tiered 350 1K, 0.5 KB (500 bytes) - в пределах включенного',
    CALCULATE_OVERAGE('SBD Tiered 350 1K', 500),
    0.00,
    ROUND(CALCULATE_OVERAGE('SBD Tiered 350 1K', 500) - 0.00, 2),
    'Ожидается: 0 (в пределах 1 KB включено)'
FROM DUAL;

PROMPT
PROMPT === 3. Проверка данных в V_CONSOLIDATED_REPORT_WITH_BILLING ===
PROMPT (записи с PLAN_NAME = 'SBD Tiered 350 1K' и usage > 1 KB)

SELECT 
    CONTRACT_ID,
    IMEI,
    BILL_MONTH,
    PLAN_NAME,
    TOTAL_USAGE_KB,
    INCLUDED_KB,
    OVERAGE_KB,
    CALCULATED_OVERAGE,
    SPNET_TOTAL_AMOUNT,
    CASE 
        WHEN OVERAGE_KB > 0 AND CALCULATED_OVERAGE = 0 THEN 'ПРОБЛЕМА: Есть превышение, но нет расчета!'
        WHEN OVERAGE_KB = 0 AND CALCULATED_OVERAGE > 0 THEN 'ПРОБЛЕМА: Нет превышения, но есть расчет!'
        ELSE 'OK'
    END AS STATUS
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE PLAN_NAME = 'SBD Tiered 350 1K'
  AND (OVERAGE_KB > 0 OR CALCULATED_OVERAGE > 0)
ORDER BY BILL_MONTH DESC, CALCULATED_OVERAGE DESC
FETCH FIRST 30 ROWS ONLY;

PROMPT
PROMPT === 4. Специфическая проверка для IMEI 300434069509210 ===
PROMPT (Contract: SUB-47358205519, Period: 2025-10)

SELECT 
    CONTRACT_ID,
    IMEI,
    BILL_MONTH,
    FINANCIAL_PERIOD,
    PLAN_NAME,
    TOTAL_USAGE_KB,
    INCLUDED_KB,
    OVERAGE_KB,
    CALCULATED_OVERAGE,
    SPNET_TOTAL_AMOUNT,
    CASE 
        WHEN OVERAGE_KB > 0 AND CALCULATED_OVERAGE = 0 THEN 'ПРОБЛЕМА: Есть превышение, но нет расчета!'
        WHEN OVERAGE_KB = 0 AND CALCULATED_OVERAGE > 0 THEN 'ПРОБЛЕМА: Нет превышения, но есть расчет!'
        ELSE 'OK'
    END AS STATUS
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE IMEI = '300434069509210'
  AND CONTRACT_ID = 'SUB-47358205519'
  AND BILL_MONTH = '2025-10'
  AND PLAN_NAME = 'SBD Tiered 350 1K';

PROMPT
PROMPT === 5. Сравнение: все планы с кодом SBD-1 ===

SELECT 
    PLAN_NAME,
    PLAN_CODE,
    INCLUDED_KB,
    COUNT(*) AS RECORD_COUNT,
    SUM(CASE WHEN OVERAGE_KB > 0 THEN 1 ELSE 0 END) AS RECORDS_WITH_OVERAGE,
    SUM(CASE WHEN OVERAGE_KB > 0 AND CALCULATED_OVERAGE = 0 THEN 1 ELSE 0 END) AS RECORDS_WITHOUT_CALC,
    ROUND(SUM(OVERAGE_KB), 2) AS TOTAL_OVERAGE_KB,
    ROUND(SUM(CALCULATED_OVERAGE), 2) AS TOTAL_CALCULATED_OVERAGE
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE PLAN_CODE = 'SBD-1'
GROUP BY PLAN_NAME, PLAN_CODE, INCLUDED_KB
ORDER BY PLAN_NAME;

EXIT;


