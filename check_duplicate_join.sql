-- Проверка: может ли для одного IMEI быть несколько строк в cor с разными BILL_MONTH,
-- но одинаковым transaction_month при JOIN с sf_advance
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING ON
SET LINESIZE 2000

-- Проверка: IMEI с несколькими строками в cor для разных BILL_MONTH, но одинаковым FINANCIAL_PERIOD
SELECT 
    'IMEI with multiple BILL_MONTH' AS check_type,
    v.IMEI,
    v.CONTRACT_ID,
    v.FINANCIAL_PERIOD,
    COUNT(*) AS row_count,
    SUM(v.FEE_ADVANCE_CHARGE) AS total_advance,
    LISTAGG(v.BILL_MONTH, ', ') WITHIN GROUP (ORDER BY v.BILL_MONTH) AS bill_months,
    LISTAGG(TO_CHAR(v.FEE_ADVANCE_CHARGE), ', ') WITHIN GROUP (ORDER BY v.BILL_MONTH) AS advance_amounts
FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
WHERE v.FINANCIAL_PERIOD = '2025-10'
  AND v.FEE_ADVANCE_CHARGE > 0
GROUP BY v.IMEI, v.CONTRACT_ID, v.FINANCIAL_PERIOD
HAVING COUNT(*) > 1
ORDER BY row_count DESC
FETCH FIRST 20 ROWS ONLY;

-- Проверка: сумма по уникальным комбинациям IMEI + CONTRACT_ID + FINANCIAL_PERIOD
SELECT 
    'Sum by unique IMEI+CONTRACT+PERIOD' AS check_type,
    COUNT(*) AS unique_combinations,
    ROUND(SUM(max_advance), 2) AS total_sum
FROM (
    SELECT 
        v.IMEI,
        v.CONTRACT_ID,
        v.FINANCIAL_PERIOD,
        MAX(v.FEE_ADVANCE_CHARGE) AS max_advance
    FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
    WHERE v.FINANCIAL_PERIOD = '2025-10'
      AND v.FEE_ADVANCE_CHARGE > 0
    GROUP BY v.IMEI, v.CONTRACT_ID, v.FINANCIAL_PERIOD
);

-- Проверка: детализация для IMEI с несколькими строками
SELECT 
    'Details for duplicate IMEI' AS check_type,
    v.IMEI,
    v.CONTRACT_ID,
    v.FINANCIAL_PERIOD,
    v.BILL_MONTH,
    v.FEE_ADVANCE_CHARGE,
    -- Проверка transaction_month для JOIN
    TO_CHAR(ADD_MONTHS(TO_DATE(SUBSTR(TRIM(v.BILL_MONTH), 1, 6), 'YYYYMM'), -1), 'YYYYMM') AS transaction_month_for_join
FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
WHERE v.FINANCIAL_PERIOD = '2025-10'
  AND v.FEE_ADVANCE_CHARGE > 0
  AND v.IMEI IN (
      SELECT v2.IMEI
      FROM V_CONSOLIDATED_REPORT_WITH_BILLING v2
      WHERE v2.FINANCIAL_PERIOD = '2025-10'
        AND v2.FEE_ADVANCE_CHARGE > 0
      GROUP BY v2.IMEI, v2.CONTRACT_ID, v2.FINANCIAL_PERIOD
      HAVING COUNT(*) > 1
  )
ORDER BY v.IMEI, v.BILL_MONTH
FETCH FIRST 20 ROWS ONLY;

EXIT;





