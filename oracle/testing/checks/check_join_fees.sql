-- Проверка JOIN с fees для IMEI 301434061220930 с INVOICE_DATE = 2025-11-02

-- 1. Проверка данных в steccom_fees
SELECT 
    'steccom_fees' AS source,
    sf.bill_month,
    sf.CONTRACT_ID,
    sf.imei,
    sf.fee_advance_charge
FROM (
    SELECT 
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS bill_month,
        se.CONTRACT_ID,
        se.ICC_ID_IMEI AS imei,
        SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' THEN se.AMOUNT ELSE 0 END) AS fee_advance_charge
    FROM STECCOM_EXPENSES se
    WHERE se.ICC_ID_IMEI IS NOT NULL
      AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
    GROUP BY TO_CHAR(se.INVOICE_DATE, 'YYYYMM'), se.CONTRACT_ID, se.ICC_ID_IMEI
) sf
WHERE sf.imei = '301434061220930'
  AND sf.bill_month = '202511';

-- 2. Проверка данных в cor (V_CONSOLIDATED_OVERAGE_REPORT)
SELECT 
    'cor' AS source,
    cor.BILL_MONTH,
    cor.CONTRACT_ID,
    cor.IMEI
FROM V_CONSOLIDATED_OVERAGE_REPORT cor
WHERE cor.IMEI = '301434061220930'
  AND cor.BILL_MONTH = '202511';

-- 3. Проверка JOIN вручную
SELECT 
    'Manual JOIN' AS source,
    cor.BILL_MONTH AS cor_bill_month,
    cor.BILL_MONTH_YYYMM AS cor_bill_month_yyymm,
    sf.bill_month AS sf_bill_month,
    cor.CONTRACT_ID AS cor_contract_id,
    sf.CONTRACT_ID AS sf_contract_id,
    sf.fee_advance_charge
FROM (
    SELECT 
        IMEI,
        CONTRACT_ID,
        BILL_MONTH,
        BILL_MONTH AS BILL_MONTH_YYYMM
    FROM V_CONSOLIDATED_OVERAGE_REPORT
    WHERE IMEI = '301434061220930'
      AND BILL_MONTH = '202511'
) cor
LEFT JOIN (
    SELECT 
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS bill_month,
        se.CONTRACT_ID,
        se.ICC_ID_IMEI AS imei,
        SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' THEN se.AMOUNT ELSE 0 END) AS fee_advance_charge
    FROM STECCOM_EXPENSES se
    WHERE se.ICC_ID_IMEI IS NOT NULL
      AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
    GROUP BY TO_CHAR(se.INVOICE_DATE, 'YYYYMM'), se.CONTRACT_ID, se.ICC_ID_IMEI
) sf ON cor.BILL_MONTH_YYYMM = sf.bill_month
    AND RTRIM(cor.CONTRACT_ID) = RTRIM(sf.CONTRACT_ID)
    AND cor.IMEI = sf.imei;

