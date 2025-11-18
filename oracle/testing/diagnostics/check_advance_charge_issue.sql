-- Диагностика проблемы с авансом за предыдущий период
-- IMEI: 300234069508860 (12.5) и 300234069606340 (3.5)
-- Проблема: не сходится на 16$ (12.5 + 3.5)

-- 1. Проверка данных в STECCOM_EXPENSES для этих IMEI
SELECT 
    'STECCOM_EXPENSES' AS source,
    se.ICC_ID_IMEI AS IMEI,
    se.CONTRACT_ID,
    se.INVOICE_DATE,
    TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS bill_month,
    se.DESCRIPTION,
    se.AMOUNT
FROM STECCOM_EXPENSES se
WHERE se.ICC_ID_IMEI IN ('300234069508860', '300234069606340')
  AND UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%'
ORDER BY se.ICC_ID_IMEI, se.INVOICE_DATE DESC;

-- 2. Проверка данных в steccom_fees CTE (как они агрегируются)
-- Нужно проверить, правильно ли группируются данные
SELECT 
    'steccom_fees (calculated)' AS source,
    TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS bill_month,
    se.CONTRACT_ID,
    se.ICC_ID_IMEI AS imei,
    SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' THEN se.AMOUNT ELSE 0 END) AS fee_advance_charge
FROM STECCOM_EXPENSES se
WHERE se.CONTRACT_ID IS NOT NULL
  AND se.ICC_ID_IMEI IS NOT NULL
  AND se.INVOICE_DATE IS NOT NULL
  AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
  AND se.ICC_ID_IMEI IN ('300234069508860', '300234069606340')
GROUP BY 
    TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
    se.CONTRACT_ID, 
    se.ICC_ID_IMEI
ORDER BY se.ICC_ID_IMEI, bill_month DESC;

-- 3. Проверка данных в представлении для текущего периода
SELECT 
    'V_CONSOLIDATED_REPORT_WITH_BILLING (current)' AS source,
    v.IMEI,
    v.CONTRACT_ID,
    v.BILL_MONTH,
    v.BILL_MONTH_YYYMM,
    v.FINANCIAL_PERIOD,
    v.FEE_ADVANCE_CHARGE AS current_advance,
    v.FEE_ADVANCE_CHARGE_PREVIOUS_MONTH AS prev_advance
FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
WHERE v.IMEI IN ('300234069508860', '300234069606340')
ORDER BY v.IMEI, v.BILL_MONTH DESC;

-- 4. Проверка JOIN для предыдущего месяца (вручную)
-- Для каждого IMEI найдем предыдущий месяц и проверим, есть ли данные
SELECT 
    'Manual JOIN check' AS source,
    cor.IMEI,
    cor.CONTRACT_ID,
    cor.BILL_MONTH,
    cor.BILL_MONTH_YYYMM,
    CASE 
        WHEN cor.BILL_MONTH IS NOT NULL AND LENGTH(TRIM(cor.BILL_MONTH)) >= 6 THEN
            TO_CHAR(ADD_MONTHS(TO_DATE(SUBSTR(TRIM(cor.BILL_MONTH), 1, 6), 'YYYYMM'), -1), 'YYYYMM')
        ELSE NULL
    END AS prev_bill_month_yyymm,
    sf_prev.fee_advance_charge AS prev_advance_from_steccom_fees
FROM (
    SELECT DISTINCT
        IMEI,
        CONTRACT_ID,
        BILL_MONTH,
        CASE 
            WHEN BILL_MONTH IS NOT NULL AND LENGTH(TRIM(BILL_MONTH)) >= 6 THEN
                SUBSTR(TRIM(BILL_MONTH), 1, 6)
            ELSE BILL_MONTH
        END AS BILL_MONTH_YYYMM
    FROM V_CONSOLIDATED_OVERAGE_REPORT
    WHERE IMEI IN ('300234069508860', '300234069606340')
) cor
LEFT JOIN (
    SELECT 
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS bill_month,
        se.CONTRACT_ID,
        se.ICC_ID_IMEI AS imei,
        SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' THEN se.AMOUNT ELSE 0 END) AS fee_advance_charge
    FROM STECCOM_EXPENSES se
    WHERE se.CONTRACT_ID IS NOT NULL
      AND se.ICC_ID_IMEI IS NOT NULL
      AND se.INVOICE_DATE IS NOT NULL
      AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
    GROUP BY 
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
        se.CONTRACT_ID, 
        se.ICC_ID_IMEI
) sf_prev ON sf_prev.bill_month = CASE 
        WHEN cor.BILL_MONTH IS NOT NULL AND LENGTH(TRIM(cor.BILL_MONTH)) >= 6 THEN
            TO_CHAR(ADD_MONTHS(TO_DATE(SUBSTR(TRIM(cor.BILL_MONTH), 1, 6), 'YYYYMM'), -1), 'YYYYMM')
        ELSE NULL
    END
    AND RTRIM(cor.CONTRACT_ID) = RTRIM(sf_prev.CONTRACT_ID)
    AND cor.IMEI = sf_prev.imei
WHERE cor.IMEI IN ('300234069508860', '300234069606340')
ORDER BY cor.IMEI, cor.BILL_MONTH DESC;

