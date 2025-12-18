-- Проверка: может ли для одного IMEI быть несколько записей с разными INVOICE_DATE,
-- которые попадают и в SAME_MONTH, и в NEXT_MONTH части UNION ALL
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING ON
SET LINESIZE 2000

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
),
same_month AS (
    SELECT 
        se.CONTRACT_ID,
        se.ICC_ID_IMEI AS imei,
        TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') AS transaction_month,
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS invoice_month,
        SUM(se.AMOUNT) AS fee_advance_charge
    FROM unique_steccom_expenses se
    WHERE se.is_advance_charge = 1
      AND se.TRANSACTION_DATE IS NOT NULL
      AND se.rn = 1
      AND TO_CHAR(se.INVOICE_DATE, 'YYYYMM') = TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM')
      AND TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') = '202510'
    GROUP BY 
        se.CONTRACT_ID, 
        se.ICC_ID_IMEI,
        TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM'),
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM')
),
next_month AS (
    SELECT 
        se.CONTRACT_ID,
        se.ICC_ID_IMEI AS imei,
        TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') AS transaction_month,
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS invoice_month,
        SUM(se.AMOUNT) AS fee_advance_charge
    FROM unique_steccom_expenses se
    WHERE se.is_advance_charge = 1
      AND se.TRANSACTION_DATE IS NOT NULL
      AND se.rn = 1
      AND TO_CHAR(se.INVOICE_DATE, 'YYYYMM') = TO_CHAR(ADD_MONTHS(se.TRANSACTION_DATE, 1), 'YYYYMM')
      AND TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') = '202510'
      AND NOT EXISTS (
          SELECT 1 
          FROM unique_steccom_expenses se2
          WHERE se2.is_advance_charge = 1
            AND se2.rn = 1
            AND se2.CONTRACT_ID = se.CONTRACT_ID
            AND se2.ICC_ID_IMEI = se.ICC_ID_IMEI
            AND TO_CHAR(se2.TRANSACTION_DATE, 'YYYYMM') = TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM')
            AND TO_CHAR(se2.INVOICE_DATE, 'YYYYMM') = TO_CHAR(se2.TRANSACTION_DATE, 'YYYYMM')
      )
    GROUP BY 
        se.CONTRACT_ID, 
        se.ICC_ID_IMEI,
        TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM'),
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM')
)
-- Проверка: IMEI, которые есть и в SAME_MONTH, и в NEXT_MONTH
-- но с разными INVOICE_DATE (это может быть проблемой)
SELECT 
    'IMEI in both sources with different INVOICE_DATE' AS check_type,
    COALESCE(sm.imei, nm.imei) AS imei,
    COALESCE(sm.CONTRACT_ID, nm.CONTRACT_ID) AS CONTRACT_ID,
    sm.invoice_month AS same_month_invoice,
    nm.invoice_month AS next_month_invoice,
    NVL(sm.fee_advance_charge, 0) AS same_month_amount,
    NVL(nm.fee_advance_charge, 0) AS next_month_amount,
    NVL(sm.fee_advance_charge, 0) + NVL(nm.fee_advance_charge, 0) AS total_amount
FROM same_month sm
FULL OUTER JOIN next_month nm
    ON sm.CONTRACT_ID = nm.CONTRACT_ID
    AND sm.imei = nm.imei
    AND sm.transaction_month = nm.transaction_month
WHERE (sm.imei IS NOT NULL AND nm.imei IS NOT NULL)
  AND sm.invoice_month != nm.invoice_month
ORDER BY total_amount DESC
FETCH FIRST 20 ROWS ONLY;

-- Проверка: может ли быть для одного IMEI несколько записей в SAME_MONTH с разными INVOICE_DATE
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
),
same_month AS (
    SELECT 
        se.CONTRACT_ID,
        se.ICC_ID_IMEI AS imei,
        TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') AS transaction_month,
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS invoice_month,
        SUM(se.AMOUNT) AS fee_advance_charge
    FROM unique_steccom_expenses se
    WHERE se.is_advance_charge = 1
      AND se.TRANSACTION_DATE IS NOT NULL
      AND se.rn = 1
      AND TO_CHAR(se.INVOICE_DATE, 'YYYYMM') = TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM')
      AND TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') = '202510'
    GROUP BY 
        se.CONTRACT_ID, 
        se.ICC_ID_IMEI,
        TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM'),
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM')
)
SELECT 
    'IMEI with multiple INVOICE_DATE in SAME_MONTH' AS check_type,
    imei,
    CONTRACT_ID,
    transaction_month,
    COUNT(*) AS invoice_count,
    SUM(fee_advance_charge) AS total_amount
FROM same_month
GROUP BY imei, CONTRACT_ID, transaction_month
HAVING COUNT(*) > 1
ORDER BY total_amount DESC
FETCH FIRST 10 ROWS ONLY;

EXIT;

