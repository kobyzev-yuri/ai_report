-- Проверка наличия колонок Fees в представлении
SET PAGESIZE 1000
SET LINESIZE 200

PROMPT ========================================
PROMPT Проверка колонок Fees в представлении
PROMPT ========================================
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    DATA_LENGTH,
    NULLABLE
FROM USER_TAB_COLUMNS 
WHERE TABLE_NAME = 'V_CONSOLIDATED_REPORT_WITH_BILLING'
  AND UPPER(COLUMN_NAME) LIKE 'FEE%'
ORDER BY COLUMN_ID;

PROMPT ========================================
PROMPT Пример данных с Fees (первые 5 строк)
PROMPT ========================================
SELECT 
    BILL_MONTH,
    FINANCIAL_PERIOD,
    IMEI,
    CONTRACT_ID,
    FEE_ACTIVATION_FEE,
    FEE_ADVANCE_CHARGE,
    FEE_ADVANCE_CHARGE_PREVIOUS_MONTH,
    FEE_CREDIT,
    FEE_CREDITED,
    FEE_PRORATED,
    FEES_TOTAL
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE (FEE_ACTIVATION_FEE != 0 
   OR FEE_ADVANCE_CHARGE != 0 
   OR FEE_CREDIT != 0 
   OR FEE_CREDITED != 0 
   OR FEE_PRORATED != 0)
  AND ROWNUM <= 5
ORDER BY BILL_MONTH DESC;

PROMPT ========================================
PROMPT Статистика по Fees
PROMPT ========================================
SELECT 
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN FEE_ACTIVATION_FEE != 0 THEN 1 END) AS rows_with_activation_fee,
    COUNT(CASE WHEN FEE_ADVANCE_CHARGE != 0 THEN 1 END) AS rows_with_advance_charge,
    COUNT(CASE WHEN FEE_CREDIT != 0 THEN 1 END) AS rows_with_credit,
    COUNT(CASE WHEN FEE_CREDITED != 0 THEN 1 END) AS rows_with_credited,
    COUNT(CASE WHEN FEE_PRORATED != 0 THEN 1 END) AS rows_with_prorated,
    SUM(FEE_ACTIVATION_FEE) AS total_activation_fee,
    SUM(FEE_ADVANCE_CHARGE) AS total_advance_charge,
    SUM(FEE_CREDIT) AS total_credit,
    SUM(FEE_CREDITED) AS total_credited,
    SUM(FEE_PRORATED) AS total_prorated,
    SUM(FEES_TOTAL) AS total_fees_total
FROM V_CONSOLIDATED_REPORT_WITH_BILLING;

PROMPT ========================================
PROMPT Проверка завершена
PROMPT ========================================

