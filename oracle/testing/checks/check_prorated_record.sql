-- Проверка записи с Prorated
SET PAGESIZE 1000
SET LINESIZE 200

PROMPT ========================================
PROMPT Проверка записи в представлении
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
    FEE_PRORATED
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE IMEI = '300234069907470'
  AND CONTRACT_ID = 'SUB-61941702540'
  AND BILL_MONTH = '2025-09';

PROMPT ========================================
PROMPT Проверка исходных данных в STECCOM_EXPENSES
PROMPT ========================================
SELECT 
    SOURCE_FILE,
    CONTRACT_ID,
    ICC_ID_IMEI,
    DESCRIPTION,
    AMOUNT,
    INVOICE_DATE
FROM STECCOM_EXPENSES
WHERE CONTRACT_ID = 'SUB-61941702540'
  AND ICC_ID_IMEI = '300234069907470'
  AND SOURCE_FILE LIKE '%20251002%'
ORDER BY DESCRIPTION;

PROMPT ========================================
PROMPT Проверка завершена
PROMPT ========================================

