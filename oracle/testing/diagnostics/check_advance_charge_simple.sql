-- Простая проверка проблемы с авансом за предыдущий период
-- IMEI: 300234069508860 (12.5) и 300234069606340 (3.5)
-- Запустить через туннель для диагностики

-- 1. Проверка данных в представлении (что видит финансист)
SELECT 
    v.IMEI,
    v.CONTRACT_ID,
    v.BILL_MONTH,
    v.FINANCIAL_PERIOD,
    v.FEE_ADVANCE_CHARGE AS current_advance,
    v.FEE_ADVANCE_CHARGE_PREVIOUS_MONTH AS prev_advance
FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
WHERE v.IMEI IN ('300234069508860', '300234069606340')
ORDER BY v.IMEI, v.BILL_MONTH DESC;

-- 2. Проверка исходных данных в STECCOM_EXPENSES (все периоды для этих IMEI)
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

