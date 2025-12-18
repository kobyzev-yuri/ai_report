-- Проверка дубликатов для Advance Charge
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING ON
SET LINESIZE 2000

-- 1. Проверка: есть ли IMEI с несколькими строками для одного FINANCIAL_PERIOD
SELECT 
    'IMEI with multiple rows per FINANCIAL_PERIOD' AS check_type,
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

-- 2. Проверка: сумма по IMEI (должна совпадать с общей суммой)
SELECT 
    'Sum by IMEI' AS check_type,
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

-- 3. Проверка: IMEI с наибольшими суммами (может быть проблема там)
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





