-- Проверка: почему 2 записи не JOIN'ятся с cor
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING ON
SET LINESIZE 2000

-- Проверка: IMEI из steccom_advance_charge_by_period, которых нет в cor для октября
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
-- Проверка: IMEI из steccom_advance_charge_by_period, которых нет в cor для BILL_MONTH = 202511 (FINANCIAL_PERIOD = 2025-10)
SELECT 
    'IMEI not in cor' AS check_type,
    sf.imei,
    sf.CONTRACT_ID,
    sf.fee_advance_charge,
    sf.transaction_month
FROM steccom_advance_charge_by_period sf
WHERE sf.transaction_month = '202510'
  AND NOT EXISTS (
      SELECT 1
      FROM V_CONSOLIDATED_OVERAGE_REPORT cor
      WHERE RTRIM(cor.CONTRACT_ID) = RTRIM(sf.CONTRACT_ID)
        AND cor.IMEI = sf.imei
        AND CASE 
                WHEN cor.BILL_MONTH IS NOT NULL AND LENGTH(TRIM(cor.BILL_MONTH)) >= 6 THEN
                    SUBSTR(TRIM(cor.BILL_MONTH), 1, 6)
                ELSE cor.BILL_MONTH
            END = '202511'  -- BILL_MONTH для FINANCIAL_PERIOD = 2025-10
  )
ORDER BY sf.fee_advance_charge DESC
FETCH FIRST 10 ROWS ONLY;

-- Проверка: IMEI, которые есть и в SAME_MONTH, и в NEXT_MONTH
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
),
next_month AS (
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
)
SELECT 
    'IMEI in both sources' AS check_type,
    COALESCE(sm.imei, nm.imei) AS imei,
    COALESCE(sm.CONTRACT_ID, nm.CONTRACT_ID) AS CONTRACT_ID,
    NVL(sm.fee_advance_charge, 0) AS same_month_amount,
    NVL(nm.fee_advance_charge, 0) AS next_month_amount,
    NVL(sm.fee_advance_charge, 0) + NVL(nm.fee_advance_charge, 0) AS total_amount
FROM same_month sm
FULL OUTER JOIN next_month nm
    ON sm.CONTRACT_ID = nm.CONTRACT_ID
    AND sm.imei = nm.imei
WHERE (sm.imei IS NOT NULL AND nm.imei IS NOT NULL)
ORDER BY total_amount DESC
FETCH FIRST 10 ROWS ONLY;

EXIT;





