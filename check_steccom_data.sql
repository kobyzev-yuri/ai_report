-- Проверка данных в steccom_advance_charge_by_period для октября
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING ON
SET LINESIZE 2000

-- Проверка: может ли быть для одного IMEI несколько записей в steccom_advance_charge_by_period
-- для одного transaction_month (из-за UNION ALL)
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
steccom_advance_charge_by_period_raw AS (
    SELECT 
        se.CONTRACT_ID,
        se.ICC_ID_IMEI AS imei,
        TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') AS transaction_month,
        SUM(se.AMOUNT) AS fee_advance_charge,
        'SAME_MONTH' AS source_type
    FROM unique_steccom_expenses se
    WHERE se.is_advance_charge = 1
      AND se.TRANSACTION_DATE IS NOT NULL
      AND se.rn = 1
      AND TO_CHAR(se.INVOICE_DATE, 'YYYYMM') = TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM')
      AND TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') = '202510'
    GROUP BY 
        se.CONTRACT_ID, 
        se.ICC_ID_IMEI,
        TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM')
    UNION ALL
    SELECT 
        se.CONTRACT_ID,
        se.ICC_ID_IMEI AS imei,
        TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') AS transaction_month,
        SUM(se.AMOUNT) AS fee_advance_charge,
        'NEXT_MONTH' AS source_type
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
        TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM')
),
steccom_advance_charge_by_period AS (
    SELECT 
        CONTRACT_ID,
        imei,
        transaction_month,
        SUM(fee_advance_charge) AS fee_advance_charge,
        COUNT(*) AS source_count,
        LISTAGG(source_type, ', ') WITHIN GROUP (ORDER BY source_type) AS source_types
    FROM steccom_advance_charge_by_period_raw
    GROUP BY 
        CONTRACT_ID,
        imei,
        transaction_month
)
-- Проверка: есть ли IMEI с несколькими источниками (SAME_MONTH и NEXT_MONTH)
SELECT 
    'IMEI with multiple sources' AS check_type,
    imei,
    CONTRACT_ID,
    transaction_month,
    source_count,
    fee_advance_charge,
    source_types
FROM steccom_advance_charge_by_period
WHERE transaction_month = '202510'
  AND source_count > 1
ORDER BY source_count DESC
FETCH FIRST 10 ROWS ONLY;

-- Общая сумма из steccom_advance_charge_by_period
SELECT 
    'Total from steccom_advance_charge_by_period' AS check_type,
    COUNT(*) AS record_count,
    ROUND(SUM(fee_advance_charge), 2) AS total_sum
FROM (
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
    steccom_advance_charge_by_period_raw AS (
        SELECT 
            se.CONTRACT_ID,
            se.ICC_ID_IMEI AS imei,
            TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') AS transaction_month,
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
            TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM')
        UNION ALL
        SELECT 
            se.CONTRACT_ID,
            se.ICC_ID_IMEI AS imei,
            TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') AS transaction_month,
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
            TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM')
    ),
    steccom_advance_charge_by_period AS (
        SELECT 
            CONTRACT_ID,
            imei,
            transaction_month,
            SUM(fee_advance_charge) AS fee_advance_charge
        FROM steccom_advance_charge_by_period_raw
        GROUP BY 
            CONTRACT_ID,
            imei,
            transaction_month
    )
    SELECT * FROM steccom_advance_charge_by_period
    WHERE transaction_month = '202510'
);

EXIT;





