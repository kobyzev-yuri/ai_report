-- Финальная проверка: может ли быть проблема в дедупликации
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING ON
SET LINESIZE 2000

-- Проверка: для одного IMEI может быть несколько записей с разными INVOICE_DATE,
-- но одинаковым TRANSACTION_DATE и AMOUNT - они все попадают в SAME_MONTH и суммируются
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
    'IMEI with multiple INVOICE_DATE for same TRANSACTION_DATE' AS check_type,
    se.CONTRACT_ID,
    se.ICC_ID_IMEI AS imei,
    TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') AS transaction_month,
    COUNT(DISTINCT TO_CHAR(se.INVOICE_DATE, 'YYYYMM')) AS invoice_months,
    SUM(se.AMOUNT) AS total_amount,
    LISTAGG(TO_CHAR(se.INVOICE_DATE, 'YYYYMM'), ', ') WITHIN GROUP (ORDER BY TO_CHAR(se.INVOICE_DATE, 'YYYYMM')) AS invoice_months_list
FROM unique_steccom_expenses se
WHERE se.is_advance_charge = 1
  AND se.TRANSACTION_DATE IS NOT NULL
  AND se.rn = 1
  AND TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') = '202510'
  AND TO_CHAR(se.INVOICE_DATE, 'YYYYMM') = TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM')
GROUP BY 
    se.CONTRACT_ID,
    se.ICC_ID_IMEI,
    TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM')
HAVING COUNT(DISTINCT TO_CHAR(se.INVOICE_DATE, 'YYYYMM')) > 1
ORDER BY total_amount DESC
FETCH FIRST 20 ROWS ONLY;

-- Проверка: сумма по уникальным комбинациям CONTRACT_ID + IMEI + TRANSACTION_DATE
-- (без учета INVOICE_DATE)
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
    'Sum by unique IMEI+TRANSACTION_DATE (max amount)' AS check_type,
    COUNT(*) AS unique_combinations,
    ROUND(SUM(max_amount), 2) AS total_sum
FROM (
    SELECT 
        se.CONTRACT_ID,
        se.ICC_ID_IMEI AS imei,
        TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') AS transaction_month,
        MAX(se.AMOUNT) AS max_amount
    FROM unique_steccom_expenses se
    WHERE se.is_advance_charge = 1
      AND se.TRANSACTION_DATE IS NOT NULL
      AND se.rn = 1
      AND TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') = '202510'
      AND TO_CHAR(se.INVOICE_DATE, 'YYYYMM') = TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM')
    GROUP BY 
        se.CONTRACT_ID,
        se.ICC_ID_IMEI,
        TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM')
);

EXIT;

