-- Финальная проверка Advance Charge за октябрь
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING ON
SET LINESIZE 2000

-- 1. Общая сумма Advance Charge за октябрь
SELECT 
    'Total Advance Charge October' AS check_type,
    ROUND(SUM(v.FEE_ADVANCE_CHARGE), 2) AS actual_amount,
    23834.5 AS expected_amount,
    ROUND(SUM(v.FEE_ADVANCE_CHARGE) - 23834.5, 2) AS difference
FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
WHERE v.FINANCIAL_PERIOD = '2025-10'
  AND v.FEE_ADVANCE_CHARGE > 0;

-- 2. Проверка: есть ли IMEI с несколькими строками для одного FINANCIAL_PERIOD
-- (разные BILL_MONTH, но одинаковый FINANCIAL_PERIOD)
SELECT 
    'IMEI with multiple BILL_MONTH' AS check_type,
    v.IMEI,
    v.CONTRACT_ID,
    v.FINANCIAL_PERIOD,
    COUNT(*) AS row_count,
    SUM(v.FEE_ADVANCE_CHARGE) AS total_advance,
    LISTAGG(v.BILL_MONTH, ', ') WITHIN GROUP (ORDER BY v.BILL_MONTH) AS bill_months
FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
WHERE v.FINANCIAL_PERIOD = '2025-10'
  AND v.FEE_ADVANCE_CHARGE > 0
GROUP BY v.IMEI, v.CONTRACT_ID, v.FINANCIAL_PERIOD
HAVING COUNT(*) > 1
ORDER BY row_count DESC
FETCH FIRST 10 ROWS ONLY;

-- 3. Проверка: сумма по уникальным IMEI (должна совпадать с общей суммой)
SELECT 
    'Sum by unique IMEI' AS check_type,
    COUNT(*) AS imei_count,
    ROUND(SUM(max_advance), 2) AS total_sum
FROM (
    SELECT 
        v.IMEI,
        MAX(v.FEE_ADVANCE_CHARGE) AS max_advance
    FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
    WHERE v.FINANCIAL_PERIOD = '2025-10'
      AND v.FEE_ADVANCE_CHARGE > 0
    GROUP BY v.IMEI
);

-- 4. Проверка: IMEI с наибольшими суммами (может быть проблема там)
SELECT 
    'Top IMEI by amount' AS check_type,
    v.IMEI,
    v.CONTRACT_ID,
    COUNT(*) AS row_count,
    ROUND(SUM(v.FEE_ADVANCE_CHARGE), 2) AS total_advance
FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
WHERE v.FINANCIAL_PERIOD = '2025-10'
  AND v.FEE_ADVANCE_CHARGE > 0
GROUP BY v.IMEI, v.CONTRACT_ID
ORDER BY total_advance DESC
FETCH FIRST 10 ROWS ONLY;

EXIT;





