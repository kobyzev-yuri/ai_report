-- Проверка несоответствия CONTRACT_ID
SET PAGESIZE 1000
SET LINESIZE 200

PROMPT ========================================
PROMPT Проверка CONTRACT_ID в V_CONSOLIDATED_OVERAGE_REPORT
PROMPT ========================================
SELECT 
    BILL_MONTH,
    IMEI,
    CONTRACT_ID,
    LENGTH(CONTRACT_ID) AS contract_id_len
FROM V_CONSOLIDATED_OVERAGE_REPORT
WHERE IMEI = '300234069308010'
  AND (CONTRACT_ID LIKE 'SUB-26089990228%')
  AND BILL_MONTH = '202509';

PROMPT ========================================
PROMPT Проверка CONTRACT_ID в V_IRIDIUM_SERVICES_INFO
PROMPT ========================================
SELECT 
    CONTRACT_ID,
    IMEI,
    LENGTH(CONTRACT_ID) AS contract_id_len,
    SERVICE_ID
FROM V_IRIDIUM_SERVICES_INFO
WHERE IMEI = '300234069308010'
  AND (CONTRACT_ID LIKE 'SUB-26089990228%')
ORDER BY SERVICE_ID DESC;

PROMPT ========================================
PROMPT Проверка CONTRACT_ID в STECCOM_EXPENSES
PROMPT ========================================
SELECT DISTINCT
    CONTRACT_ID,
    ICC_ID_IMEI,
    LENGTH(CONTRACT_ID) AS contract_id_len
FROM STECCOM_EXPENSES
WHERE ICC_ID_IMEI = '300234069308010'
  AND (CONTRACT_ID LIKE 'SUB-26089990228%');

PROMPT ========================================
PROMPT Проверка CONTRACT_ID в V_CONSOLIDATED_REPORT_WITH_BILLING
PROMPT ========================================
SELECT 
    BILL_MONTH,
    IMEI,
    CONTRACT_ID,
    LENGTH(CONTRACT_ID) AS contract_id_len,
    FEE_ACTIVATION_FEE,
    FEE_ADVANCE_CHARGE,
    FEES_TOTAL
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE IMEI = '300234069308010'
  AND (CONTRACT_ID LIKE 'SUB-26089990228%')
  AND BILL_MONTH = '2025-09';

PROMPT ========================================
PROMPT Проверка завершена
PROMPT ========================================

