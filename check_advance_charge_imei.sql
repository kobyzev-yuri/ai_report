-- Проверка проблемы с авансом для IMEI 300234069209690
-- Проблема: в детализации Иридиум 2 аванса по 12.5$, а в отчете только один

-- 1. Проверка всех записей в STECCOM_EXPENSES для этого IMEI
SELECT 
    se.ID,
    se.INVOICE_DATE,
    TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS bill_month,
    se.CONTRACT_ID,
    se.ICC_ID_IMEI AS IMEI,
    se.TRANSACTION_DATE,
    se.DESCRIPTION,
    se.AMOUNT,
    se.SOURCE_FILE
FROM STECCOM_EXPENSES se
WHERE se.ICC_ID_IMEI = '300234069209690'
  AND UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%'
ORDER BY se.INVOICE_DATE, se.TRANSACTION_DATE, se.ID;

-- 2. Проверка как они группируются в unique_steccom_expenses
-- (симуляция логики из представления)
WITH unique_steccom_expenses AS (
    SELECT 
        se.*,
        CASE 
            WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' OR UPPER(TRIM(se.DESCRIPTION)) = 'ADVANCE CHARGE' THEN
                ROW_NUMBER() OVER (
                    PARTITION BY 
                        TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
                        se.CONTRACT_ID,
                        se.ICC_ID_IMEI,
                        UPPER(TRIM(se.DESCRIPTION)),
                        se.AMOUNT
                    ORDER BY se.ID
                )
            ELSE
                ROW_NUMBER() OVER (
                    PARTITION BY 
                        TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
                        se.CONTRACT_ID,
                        se.ICC_ID_IMEI,
                        UPPER(TRIM(se.DESCRIPTION)),
                        se.AMOUNT,
                        se.TRANSACTION_DATE
                    ORDER BY se.ID
                )
        END AS rn
    FROM STECCOM_EXPENSES se
    WHERE se.CONTRACT_ID IS NOT NULL
      AND se.ICC_ID_IMEI IS NOT NULL
      AND se.INVOICE_DATE IS NOT NULL
      AND se.ICC_ID_IMEI = '300234069209690'
      AND (UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' OR UPPER(TRIM(se.DESCRIPTION)) = 'ADVANCE CHARGE')
)
SELECT 
    se.ID,
    TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS bill_month,
    se.CONTRACT_ID,
    se.ICC_ID_IMEI AS IMEI,
    se.TRANSACTION_DATE,
    se.DESCRIPTION,
    se.AMOUNT,
    se.rn,
    CASE WHEN se.rn = 1 THEN 'ВКЛЮЧЕНА' ELSE 'ОТФИЛЬТРОВАНА' END AS status
FROM unique_steccom_expenses se
ORDER BY se.INVOICE_DATE, se.ID;

-- 3. Проверка агрегации в steccom_fees
WITH unique_steccom_expenses AS (
    SELECT 
        se.*,
        CASE 
            WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' OR UPPER(TRIM(se.DESCRIPTION)) = 'ADVANCE CHARGE' THEN
                ROW_NUMBER() OVER (
                    PARTITION BY 
                        TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
                        se.CONTRACT_ID,
                        se.ICC_ID_IMEI,
                        UPPER(TRIM(se.DESCRIPTION)),
                        se.AMOUNT
                    ORDER BY se.ID
                )
            ELSE
                ROW_NUMBER() OVER (
                    PARTITION BY 
                        TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
                        se.CONTRACT_ID,
                        se.ICC_ID_IMEI,
                        UPPER(TRIM(se.DESCRIPTION)),
                        se.AMOUNT,
                        se.TRANSACTION_DATE
                    ORDER BY se.ID
                )
        END AS rn
    FROM STECCOM_EXPENSES se
    WHERE se.CONTRACT_ID IS NOT NULL
      AND se.ICC_ID_IMEI IS NOT NULL
      AND se.INVOICE_DATE IS NOT NULL
      AND se.ICC_ID_IMEI = '300234069209690'
      AND (UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' OR UPPER(TRIM(se.DESCRIPTION)) = 'ADVANCE CHARGE')
),
steccom_fees AS (
    SELECT 
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS bill_month,
        se.CONTRACT_ID,
        se.ICC_ID_IMEI AS imei,
        SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' OR UPPER(TRIM(se.DESCRIPTION)) = 'ADVANCE CHARGE' THEN se.AMOUNT ELSE 0 END) AS fee_advance_charge,
        COUNT(*) AS record_count
    FROM unique_steccom_expenses se
    WHERE se.rn = 1
    GROUP BY 
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
        se.CONTRACT_ID, 
        se.ICC_ID_IMEI
)
SELECT 
    sf.bill_month,
    sf.CONTRACT_ID,
    sf.imei,
    sf.fee_advance_charge,
    sf.record_count,
    CASE 
        WHEN sf.fee_advance_charge = 12.5 THEN '❌ ПРОБЛЕМА: только один аванс'
        WHEN sf.fee_advance_charge = 25 THEN '✅ ПРАВИЛЬНО: два аванса'
        ELSE '⚠️ Неожиданное значение: ' || sf.fee_advance_charge
    END AS status
FROM steccom_fees sf
ORDER BY sf.bill_month;

-- 4. Проверка в финальном представлении
SELECT 
    v.FINANCIAL_PERIOD,
    v.BILL_MONTH,
    v.IMEI,
    v.CONTRACT_ID,
    v.FEE_ADVANCE_CHARGE,
    v.FEE_ADVANCE_CHARGE_PREVIOUS_MONTH
FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
WHERE v.IMEI = '300234069209690'
  AND v.FINANCIAL_PERIOD = '2025-10'
ORDER BY v.BILL_MONTH;





