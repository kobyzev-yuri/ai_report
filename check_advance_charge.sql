-- Проверка Advance Charge за октябрь 2025
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

-- 2. Количество записей с Advance Charge
SELECT 
    'Records count' AS check_type,
    COUNT(*) AS record_count,
    COUNT(DISTINCT v.IMEI) AS unique_imei_count
FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
WHERE v.FINANCIAL_PERIOD = '2025-10'
  AND v.FEE_ADVANCE_CHARGE > 0;

-- 3. Проверка конкретного IMEI
SELECT 
    'IMEI check' AS check_type,
    v.IMEI,
    v.CONTRACT_ID,
    v.FEE_ADVANCE_CHARGE,
    v.FEE_ADVANCE_CHARGE_PREVIOUS_MONTH
FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
WHERE v.FINANCIAL_PERIOD = '2025-10'
  AND v.IMEI = '300234069900210'
ORDER BY v.BILL_MONTH;

-- 4. Проверка дубликатов (IMEI с несколькими записями)
SELECT 
    'Duplicates check' AS check_type,
    v.IMEI,
    v.CONTRACT_ID,
    COUNT(*) AS row_count,
    SUM(v.FEE_ADVANCE_CHARGE) AS total_advance
FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
WHERE v.FINANCIAL_PERIOD = '2025-10'
  AND v.FEE_ADVANCE_CHARGE > 0
GROUP BY v.IMEI, v.CONTRACT_ID
HAVING COUNT(*) > 1
ORDER BY row_count DESC
FETCH FIRST 5 ROWS ONLY;

-- 5. Сумма Advance Charge Previous Month
SELECT 
    'Total Advance Charge Previous Month' AS check_type,
    ROUND(SUM(v.FEE_ADVANCE_CHARGE_PREVIOUS_MONTH), 2) AS total_prev
FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
WHERE v.FINANCIAL_PERIOD = '2025-10'
  AND v.FEE_ADVANCE_CHARGE_PREVIOUS_MONTH > 0;

EXIT;





