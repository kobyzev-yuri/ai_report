-- Поиск источника лишнего нуля в CONTRACT_ID
SET PAGESIZE 1000
SET LINESIZE 200

PROMPT ========================================
PROMPT Проверка CONTRACT_ID в V_CONSOLIDATED_OVERAGE_REPORT
PROMPT ========================================
SELECT 
    BILL_MONTH,
    IMEI,
    CONTRACT_ID,
    LENGTH(CONTRACT_ID) AS contract_id_len,
    SUBSTR(CONTRACT_ID, -1) AS last_char
FROM V_CONSOLIDATED_OVERAGE_REPORT
WHERE IMEI = '300234069308010'
  AND BILL_MONTH = '202511';

PROMPT ========================================
PROMPT Проверка CONTRACT_ID в V_SPNET_OVERAGE_ANALYSIS
PROMPT ========================================
SELECT DISTINCT
    IMEI,
    CONTRACT_ID,
    LENGTH(CONTRACT_ID) AS contract_id_len,
    SUBSTR(CONTRACT_ID, -1) AS last_char
FROM V_SPNET_OVERAGE_ANALYSIS
WHERE IMEI = '300234069308010'
  AND (CONTRACT_ID LIKE 'SUB-26089990228%')
ORDER BY CONTRACT_ID;

PROMPT ========================================
PROMPT Проверка CONTRACT_ID в V_IRIDIUM_SERVICES_INFO
PROMPT ========================================
SELECT DISTINCT
    IMEI,
    CONTRACT_ID,
    LENGTH(CONTRACT_ID) AS contract_id_len,
    SUBSTR(CONTRACT_ID, -1) AS last_char,
    SERVICE_ID
FROM V_IRIDIUM_SERVICES_INFO
WHERE IMEI = '300234069308010'
  AND (CONTRACT_ID LIKE 'SUB-26089990228%')
ORDER BY SERVICE_ID DESC;

PROMPT ========================================
PROMPT Проверка CONTRACT_ID в исходных таблицах SPNET_TRAFFIC
PROMPT ========================================
SELECT DISTINCT
    IMEI,
    CONTRACT_ID,
    LENGTH(CONTRACT_ID) AS contract_id_len,
    SUBSTR(CONTRACT_ID, -1) AS last_char
FROM SPNET_TRAFFIC
WHERE IMEI = '300234069308010'
  AND (CONTRACT_ID LIKE 'SUB-26089990228%')
ORDER BY CONTRACT_ID;

PROMPT ========================================
PROMPT Проверка завершена
PROMPT ========================================

