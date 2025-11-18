-- Проверка проблемы с авансом за предыдущий период
-- IMEI: 300234069508860 (12.5) и 300234069606340 (3.5)
-- Проблема: не сходится на 16$ (12.5 + 3.5)

-- 1. Проверка данных в STECCOM_EXPENSES для этих IMEI (все периоды)
SELECT 
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

-- 2. Проверка данных в представлении для текущего периода
SELECT 
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

-- 3. Проверка JOIN для предыдущего месяца (детальная диагностика)
-- Для каждого IMEI найдем текущий период и проверим JOIN с предыдущим месяцем
SELECT 
    cor.IMEI,
    cor.CONTRACT_ID,
    cor.BILL_MONTH,
    cor.BILL_MONTH_YYYMM,
    CASE 
        WHEN cor.BILL_MONTH IS NOT NULL AND LENGTH(TRIM(cor.BILL_MONTH)) >= 6 THEN
            TO_CHAR(ADD_MONTHS(TO_DATE(SUBSTR(TRIM(cor.BILL_MONTH), 1, 6), 'YYYYMM'), -1), 'YYYYMM')
        ELSE NULL
    END AS calculated_prev_month,
    sf_prev.bill_month AS sf_prev_bill_month,
    sf_prev.fee_advance_charge AS prev_advance_from_join,
    CASE 
        WHEN RTRIM(cor.CONTRACT_ID) = RTRIM(sf_prev.CONTRACT_ID) THEN 'MATCH' 
        ELSE 'MISMATCH: ' || cor.CONTRACT_ID || ' vs ' || sf_prev.CONTRACT_ID 
    END AS contract_match,
    CASE 
        WHEN cor.IMEI = sf_prev.imei THEN 'MATCH' 
        ELSE 'MISMATCH: ' || cor.IMEI || ' vs ' || sf_prev.imei 
    END AS imei_match
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

-- 4. Проверка всех доступных периодов для этих IMEI в steccom_fees
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
  AND se.ICC_ID_IMEI IN ('300234069508860', '300234069606340')
GROUP BY 
    TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
    se.CONTRACT_ID, 
    se.ICC_ID_IMEI
ORDER BY se.ICC_ID_IMEI, bill_month DESC;

