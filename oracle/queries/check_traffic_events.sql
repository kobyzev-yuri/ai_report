-- ============================================================================
-- Проверка трафика и событий для одного периода
-- BILL_MONTH формат: MMYYYY (месяц*10000 + год), например 102025 = октябрь 2025
-- Использование: измените BILL_MONTH на нужный период (например, 102025)
-- ============================================================================

-- 1. Проверка данных в SPNET_TRAFFIC для периода 102025 (октябрь 2025)
SELECT 
    BILL_MONTH,
    TRUNC(BILL_MONTH / 10000) AS month,
    MOD(BILL_MONTH, 10000) AS year,
    COUNT(*) AS total_records,
    COUNT(DISTINCT CONTRACT_ID) AS unique_contracts,
    COUNT(DISTINCT IMEI) AS unique_imeis,
    SUM(USAGE_BYTES) AS total_traffic_bytes,
    ROUND(SUM(USAGE_BYTES) / 1000, 2) AS total_traffic_kb,
    SUM(CASE WHEN USAGE_TYPE = 'SBD Data Usage' THEN USAGE_BYTES ELSE 0 END) AS sbd_data_bytes,
    ROUND(SUM(CASE WHEN USAGE_TYPE = 'SBD Data Usage' THEN USAGE_BYTES ELSE 0 END) / 1000, 2) AS sbd_data_kb
FROM SPNET_TRAFFIC
WHERE BILL_MONTH = 102025
GROUP BY BILL_MONTH;

-- 2. Проверка событий по типам для периода 102025
SELECT 
    BILL_MONTH,
    USAGE_TYPE,
    COUNT(*) AS events_count,
    SUM(USAGE_BYTES) AS total_bytes,
    ROUND(SUM(USAGE_BYTES) / 1000, 2) AS total_kb
FROM SPNET_TRAFFIC
WHERE BILL_MONTH = 102025
GROUP BY BILL_MONTH, USAGE_TYPE
ORDER BY USAGE_TYPE;

-- 3. Проверка данных в представлении V_CONSOLIDATED_OVERAGE_REPORT для периода 102025
-- В представлении BILL_MONTH преобразован в строку YYYYMM
SELECT 
    BILL_MONTH,
    COUNT(*) AS total_records,
    SUM(TRAFFIC_USAGE_BYTES) AS total_traffic_bytes,
    ROUND(SUM(TRAFFIC_USAGE_BYTES) / 1000, 2) AS total_traffic_kb,
    SUM(EVENTS_COUNT) AS total_events,
    SUM(DATA_USAGE_EVENTS) AS data_events,
    SUM(MAILBOX_EVENTS) AS mailbox_events,
    SUM(REGISTRATION_EVENTS) AS registration_events
FROM V_CONSOLIDATED_OVERAGE_REPORT
WHERE BILL_MONTH = '2025-10'  -- В представлении формат YYYY-MM
GROUP BY BILL_MONTH;

-- 4. Проверка для конкретного контракта (пример из запроса пользователя)
SELECT 
    BILL_MONTH,
    CONTRACT_ID,
    IMEI,
    ROUND(TRAFFIC_USAGE_BYTES / 1000, 2) AS traffic_kb,
    EVENTS_COUNT,
    DATA_USAGE_EVENTS,
    MAILBOX_EVENTS,
    REGISTRATION_EVENTS
FROM V_CONSOLIDATED_OVERAGE_REPORT
WHERE CONTRACT_ID = 'SUB-61922000117'
  AND IMEI = '300234069804210'
  AND BILL_MONTH = '2025-10';  -- В представлении формат YYYY-MM

-- 5. Проверка исходных данных в SPNET_TRAFFIC для конкретного контракта
SELECT 
    BILL_MONTH,
    CONTRACT_ID,
    IMEI,
    USAGE_TYPE,
    COUNT(*) AS records_count,
    SUM(USAGE_BYTES) AS total_bytes,
    ROUND(SUM(USAGE_BYTES) / 1000, 2) AS total_kb
FROM SPNET_TRAFFIC
WHERE CONTRACT_ID = 'SUB-61922000117'
  AND IMEI = '300234069804210'
  AND BILL_MONTH = 102025  -- Формат MMYYYY
GROUP BY BILL_MONTH, CONTRACT_ID, IMEI, USAGE_TYPE
ORDER BY USAGE_TYPE;

