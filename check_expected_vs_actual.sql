-- Сравнение ожидаемой суммы с фактической
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING ON
SET LINESIZE 2000

-- Ожидаемая сумма: $23,834.50
-- Фактическая сумма в представлении: $23,868.50
-- Разница: $34

-- Проверка: может ли быть проблема в том, что для некоторых IMEI
-- есть записи, которые не должны учитываться
SELECT 
    'Expected vs Actual' AS check_type,
    23834.5 AS expected_amount,
    ROUND(SUM(v.FEE_ADVANCE_CHARGE), 2) AS actual_amount,
    ROUND(SUM(v.FEE_ADVANCE_CHARGE) - 23834.5, 2) AS difference
FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
WHERE v.FINANCIAL_PERIOD = '2025-10'
  AND v.FEE_ADVANCE_CHARGE > 0;

-- Проверка: может ли быть проблема в том, что ожидаемая сумма $23,834.50
-- не учитывает какие-то записи или учитывает лишние
-- Сравним с суммой из steccom_advance_charge_by_period (исключая не JOIN'ящиеся)
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
SELECT 
    'In CTE (excluding not joined)' AS check_type,
    COUNT(*) AS record_count,
    ROUND(SUM(sf.fee_advance_charge), 2) AS total_sum
FROM steccom_advance_charge_by_period sf
WHERE sf.transaction_month = '202510'
  AND EXISTS (
      SELECT 1
      FROM V_CONSOLIDATED_OVERAGE_REPORT cor
      WHERE RTRIM(cor.CONTRACT_ID) = RTRIM(sf.CONTRACT_ID)
        AND cor.IMEI = sf.imei
        AND CASE 
                WHEN cor.BILL_MONTH IS NOT NULL AND LENGTH(TRIM(cor.BILL_MONTH)) >= 6 THEN
                    SUBSTR(TRIM(cor.BILL_MONTH), 1, 6)
                ELSE cor.BILL_MONTH
            END = '202511'
  );

EXIT;





