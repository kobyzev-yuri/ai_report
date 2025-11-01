-- ============================================================================
-- V_CONSOLIDATED_OVERAGE_REPORT
-- Сводный отчет по превышению с данными из SPNet и STECCOM
-- База данных: PostgreSQL (testing)
-- ВАЖНО: Периоды НЕ суммируются - каждая строка = отдельный период (BILL_MONTH)
-- ============================================================================

DROP VIEW IF EXISTS V_CONSOLIDATED_OVERAGE_REPORT CASCADE;

CREATE OR REPLACE VIEW V_CONSOLIDATED_OVERAGE_REPORT AS
WITH spnet_data AS (
    SELECT 
        ov.IMEI,
        ov.CONTRACT_ID,
        -- BILL_MONTH в формате YYYYMM (не суммируем!)
        LPAD(CAST(ov.BILL_MONTH % 10000 AS TEXT), 4, '0') || LPAD(CAST(ov.BILL_MONTH / 10000 AS TEXT), 2, '0') AS BILL_MONTH,
        ov.PLAN_NAME,
        
        -- Разделение трафика и событий
        SUM(ov.TRAFFIC_USAGE_BYTES) AS TRAFFIC_USAGE_BYTES,
        SUM(ov.EVENTS_COUNT) AS EVENTS_COUNT,
        SUM(ov.DATA_USAGE_EVENTS) AS DATA_USAGE_EVENTS,
        SUM(ov.MAILBOX_EVENTS) AS MAILBOX_EVENTS,
        SUM(ov.REGISTRATION_EVENTS) AS REGISTRATION_EVENTS,
        
        -- SPNet суммы (по каждому периоду отдельно)
        SUM(ov.SPNET_TOTAL_AMOUNT) AS SPNET_TOTAL_AMOUNT,
        MAX(ov.INCLUDED_KB) AS INCLUDED_KB,
        SUM(ov.TOTAL_USAGE_KB) AS TOTAL_USAGE_KB,
        SUM(ov.OVERAGE_KB) AS OVERAGE_KB,
        SUM(ov.CALCULATED_OVERAGE_CHARGE) AS CALCULATED_OVERAGE
    FROM V_SPNET_OVERAGE_ANALYSIS ov
    GROUP BY 
        ov.IMEI,
        ov.CONTRACT_ID,
        LPAD(CAST(ov.BILL_MONTH % 10000 AS TEXT), 4, '0') || LPAD(CAST(ov.BILL_MONTH / 10000 AS TEXT), 2, '0'),
        ov.PLAN_NAME
),
steccom_data AS (
    SELECT 
        se.ICC_ID_IMEI AS IMEI,
        se.CONTRACT_ID,
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS BILL_MONTH,
        SUM(se.AMOUNT) AS STECCOM_TOTAL_AMOUNT
    FROM STECCOM_EXPENSES se
    WHERE se.ICC_ID_IMEI IS NOT NULL
    GROUP BY 
        se.ICC_ID_IMEI,
        se.CONTRACT_ID,
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM')
)
SELECT 
    COALESCE(sp.IMEI, st.IMEI) AS IMEI,
    COALESCE(sp.CONTRACT_ID, st.CONTRACT_ID) AS CONTRACT_ID,
    COALESCE(sp.BILL_MONTH, st.BILL_MONTH) AS BILL_MONTH,
    sp.PLAN_NAME,
    
    -- Разделение трафика и событий (по каждому периоду)
    COALESCE(sp.TRAFFIC_USAGE_BYTES, 0) AS TRAFFIC_USAGE_BYTES,
    COALESCE(sp.EVENTS_COUNT, 0) AS EVENTS_COUNT,
    COALESCE(sp.DATA_USAGE_EVENTS, 0) AS DATA_USAGE_EVENTS,
    COALESCE(sp.MAILBOX_EVENTS, 0) AS MAILBOX_EVENTS,
    COALESCE(sp.REGISTRATION_EVENTS, 0) AS REGISTRATION_EVENTS,
    
    -- SPNet данные (по каждому периоду отдельно, НЕ суммируются!)
    COALESCE(sp.SPNET_TOTAL_AMOUNT, 0) AS SPNET_TOTAL_AMOUNT,
    COALESCE(sp.INCLUDED_KB, 0) AS INCLUDED_KB,
    COALESCE(sp.TOTAL_USAGE_KB, 0) AS TOTAL_USAGE_KB,
    COALESCE(sp.OVERAGE_KB, 0) AS OVERAGE_KB,
    COALESCE(sp.CALCULATED_OVERAGE, 0) AS CALCULATED_OVERAGE,
    
    -- STECCOM данные (по каждому периоду отдельно)
    COALESCE(st.STECCOM_TOTAL_AMOUNT, 0) AS STECCOM_TOTAL_AMOUNT
    
FROM spnet_data sp
FULL OUTER JOIN steccom_data st 
    ON sp.IMEI = st.IMEI 
    AND sp.CONTRACT_ID = st.CONTRACT_ID
    AND sp.BILL_MONTH = st.BILL_MONTH
ORDER BY 
    COALESCE(sp.IMEI, st.IMEI),
    COALESCE(sp.BILL_MONTH, st.BILL_MONTH) DESC;

COMMENT ON VIEW V_CONSOLIDATED_OVERAGE_REPORT IS 'Сводный отчет по превышению с данными из SPNet и STECCOM. КАЖДАЯ СТРОКА = ОТДЕЛЬНЫЙ ПЕРИОД (BILL_MONTH). Периоды НЕ суммируются!';

\echo 'View V_CONSOLIDATED_OVERAGE_REPORT создан успешно!'
