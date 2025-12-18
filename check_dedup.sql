-- Проверка дедупликации в unique_steccom_expenses
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING ON
SET LINESIZE 2000

-- Проверка: сколько записей остается после дедупликации
WITH unique_steccom_expenses AS (
    SELECT 
        se.*,
        ROW_NUMBER() OVER (
            PARTITION BY 
                TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
                se.CONTRACT_ID,
                se.ICC_ID_IMEI,
                UPPER(TRIM(se.DESCRIPTION)),
                se.AMOUNT,
                se.TRANSACTION_DATE
            ORDER BY se.ID
        ) AS rn,
        CASE 
            WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' OR UPPER(TRIM(se.DESCRIPTION)) = 'ADVANCE CHARGE' THEN 1
            ELSE 0
        END AS is_advance_charge
    FROM STECCOM_EXPENSES se
    WHERE se.CONTRACT_ID IS NOT NULL
      AND se.ICC_ID_IMEI IS NOT NULL
      AND se.INVOICE_DATE IS NOT NULL
      AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
)
SELECT 
    'After deduplication' AS check_type,
    COUNT(*) AS record_count,
    ROUND(SUM(se.AMOUNT), 2) AS total_sum
FROM unique_steccom_expenses se
WHERE se.is_advance_charge = 1
  AND se.rn = 1
  AND TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') = '202510';

-- Проверка: может ли быть проблема в том, что для одного IMEI есть несколько записей
-- с разными INVOICE_DATE, но одинаковым TRANSACTION_DATE и AMOUNT
SELECT 
    'IMEI with multiple INVOICE_DATE' AS check_type,
    COUNT(*) AS imei_count,
    ROUND(SUM(total_amount), 2) AS total_duplicate_amount
FROM (
    SELECT 
        se.CONTRACT_ID,
        se.ICC_ID_IMEI,
        TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') AS transaction_month,
        se.AMOUNT,
        COUNT(DISTINCT TO_CHAR(se.INVOICE_DATE, 'YYYYMM')) AS invoice_months,
        SUM(se.AMOUNT) AS total_amount
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
    HAVING COUNT(DISTINCT TO_CHAR(se.INVOICE_DATE, 'YYYYMM')) > 1
);

-- Проверка: примеры таких IMEI
SELECT 
    'Examples' AS check_type,
    se.CONTRACT_ID,
    se.ICC_ID_IMEI,
    TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') AS transaction_month,
    TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS invoice_month,
    se.AMOUNT,
    COUNT(*) AS record_count
FROM STECCOM_EXPENSES se
WHERE se.CONTRACT_ID IS NOT NULL
  AND se.ICC_ID_IMEI IS NOT NULL
  AND se.INVOICE_DATE IS NOT NULL
  AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
  AND UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%'
  AND TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') = '202510'
  AND se.ICC_ID_IMEI IN (
      SELECT se2.ICC_ID_IMEI
      FROM STECCOM_EXPENSES se2
      WHERE se2.CONTRACT_ID IS NOT NULL
        AND se2.ICC_ID_IMEI IS NOT NULL
        AND se2.INVOICE_DATE IS NOT NULL
        AND (se2.SERVICE IS NULL OR UPPER(TRIM(se2.SERVICE)) != 'BROADBAND')
        AND UPPER(TRIM(se2.DESCRIPTION)) LIKE '%ADVANCE CHARGE%'
        AND TO_CHAR(se2.TRANSACTION_DATE, 'YYYYMM') = '202510'
      GROUP BY 
          se2.CONTRACT_ID,
          se2.ICC_ID_IMEI,
          TO_CHAR(se2.TRANSACTION_DATE, 'YYYYMM'),
          se2.AMOUNT
      HAVING COUNT(DISTINCT TO_CHAR(se2.INVOICE_DATE, 'YYYYMM')) > 1
      FETCH FIRST 5 ROWS ONLY
  )
GROUP BY 
    se.CONTRACT_ID,
    se.ICC_ID_IMEI,
    TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM'),
    TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
    se.AMOUNT
ORDER BY se.ICC_ID_IMEI, TO_CHAR(se.INVOICE_DATE, 'YYYYMM')
FETCH FIRST 20 ROWS ONLY;

EXIT;





