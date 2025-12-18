-- Упрощенная проверка: поиск источника лишних $34
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING ON
SET LINESIZE 2000

-- Проверка: IMEI с несколькими строками в cor для одного FINANCIAL_PERIOD
SELECT 
    'IMEI with multiple rows' AS check_type,
    COUNT(*) AS imei_count
FROM (
    SELECT 
        v.IMEI,
        v.CONTRACT_ID,
        v.FINANCIAL_PERIOD
    FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
    WHERE v.FINANCIAL_PERIOD = '2025-10'
      AND v.FEE_ADVANCE_CHARGE > 0
    GROUP BY v.IMEI, v.CONTRACT_ID, v.FINANCIAL_PERIOD
    HAVING COUNT(*) > 1
);

-- Проверка: может ли быть проблема в исходных данных STECCOM_EXPENSES
-- Проверка дубликатов для Advance Charge за октябрь
SELECT 
    'Duplicate Advance Charge in STECCOM_EXPENSES' AS check_type,
    COUNT(*) AS duplicate_count,
    ROUND(SUM(AMOUNT), 2) AS total_duplicate_amount
FROM (
    SELECT 
        se.CONTRACT_ID,
        se.ICC_ID_IMEI,
        TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') AS transaction_month,
        se.AMOUNT,
        COUNT(*) AS cnt
    FROM STECCOM_EXPENSES se
    WHERE se.CONTRACT_ID IS NOT NULL
      AND se.ICC_ID_IMEI IS NOT NULL
      AND se.INVOICE_DATE IS NOT NULL
      AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
      AND UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%'
      AND TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') = '202510'
    GROUP BY 
        se.CONTRACT_ID,
        se.ICC_ID_IMEI,
        TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM'),
        se.AMOUNT
    HAVING COUNT(*) > 1
);

-- Проверка: сумма всех Advance Charge за октябрь из STECCOM_EXPENSES (без дедупликации)
SELECT 
    'Total from STECCOM_EXPENSES (no dedup)' AS check_type,
    COUNT(*) AS record_count,
    ROUND(SUM(se.AMOUNT), 2) AS total_sum
FROM STECCOM_EXPENSES se
WHERE se.CONTRACT_ID IS NOT NULL
  AND se.ICC_ID_IMEI IS NOT NULL
  AND se.INVOICE_DATE IS NOT NULL
  AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
  AND UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%'
  AND TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') = '202510';

EXIT;





