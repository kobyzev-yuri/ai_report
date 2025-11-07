-- ============================================================================
-- V_CONSOLIDATED_OVERAGE_REPORT
-- Сводный отчет по превышению с данными из SPNet и STECCOM
-- База данных: PostgreSQL (testing)
-- ВАЖНО: Периоды НЕ суммируются - каждая строка = отдельный период (BILL_MONTH)
-- ============================================================================

DROP VIEW IF EXISTS V_CONSOLIDATED_OVERAGE_REPORT CASCADE;

CREATE OR REPLACE VIEW V_CONSOLIDATED_OVERAGE_REPORT AS
WITH -- Маппинг plan_name по contract_id из других периодов SPNET_TRAFFIC (берем самый частый план)
contract_plan_mapping AS (
    SELECT DISTINCT ON (contract_id)
        contract_id,
        plan_name
    FROM (
        SELECT 
            st.contract_id,
            st.plan_name,
            COUNT(*) AS plan_count
        FROM spnet_traffic st
        WHERE st.contract_id IS NOT NULL
          AND st.plan_name IS NOT NULL
          AND st.plan_name != ''
        GROUP BY st.contract_id, st.plan_name
    ) grouped_plans
    ORDER BY contract_id, plan_count DESC, plan_name
),
-- Маппинг plan_name по IMEI из других периодов SPNET_TRAFFIC (берем самый частый план)
imei_plan_mapping AS (
    SELECT DISTINCT ON (imei)
        imei,
        plan_name
    FROM (
        SELECT 
            st.imei,
            st.plan_name,
            COUNT(*) AS plan_count
        FROM spnet_traffic st
        WHERE st.imei IS NOT NULL
          AND st.plan_name IS NOT NULL
          AND st.plan_name != ''
        GROUP BY st.imei, st.plan_name
    ) grouped_plans
    ORDER BY imei, plan_count DESC, plan_name
),
spnet_data AS (
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
        -- Основной тариф (Monthly Fee, не Suspended) - сумма и план
        SUM(CASE 
            WHEN se.RATE_TYPE IS NOT NULL 
             AND UPPER(TRIM(se.RATE_TYPE)) NOT LIKE '%SUSPEND%'
            THEN se.AMOUNT 
            ELSE 0 
        END) AS STECCOM_MONTHLY_AMOUNT,
        -- Suspended тариф - сумма
        SUM(CASE 
            WHEN se.RATE_TYPE IS NOT NULL 
             AND UPPER(TRIM(se.RATE_TYPE)) LIKE '%SUSPEND%'
            THEN se.AMOUNT 
            ELSE 0 
        END) AS STECCOM_SUSPENDED_AMOUNT,
        -- Общая сумма для обратной совместимости
        SUM(se.AMOUNT) AS STECCOM_TOTAL_AMOUNT,
        -- Две отдельные колонки для планов: основной и suspended
        -- Основной план тарифа (из plan_discount, где rate_type не Suspend)
        MAX(CASE 
            WHEN se.RATE_TYPE IS NOT NULL 
             AND UPPER(TRIM(se.RATE_TYPE)) NOT LIKE '%SUSPEND%'
             AND se.PLAN_DISCOUNT IS NOT NULL
            THEN se.PLAN_DISCOUNT 
            ELSE NULL 
        END) AS STECCOM_PLAN_NAME_MONTHLY,
        -- Suspended план тарифа (из plan_discount, где rate_type содержит Suspend)
        MAX(CASE 
            WHEN se.RATE_TYPE IS NOT NULL 
             AND UPPER(TRIM(se.RATE_TYPE)) LIKE '%SUSPEND%'
             AND se.PLAN_DISCOUNT IS NOT NULL
            THEN se.PLAN_DISCOUNT 
            ELSE NULL 
        END) AS STECCOM_PLAN_NAME_SUSPENDED
    FROM STECCOM_EXPENSES se
    WHERE se.ICC_ID_IMEI IS NOT NULL
      AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
    GROUP BY 
        se.ICC_ID_IMEI,
        se.CONTRACT_ID,
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM')
)
SELECT 
    COALESCE(sp.IMEI, st.IMEI) AS IMEI,
    COALESCE(sp.CONTRACT_ID, st.CONTRACT_ID) AS CONTRACT_ID,
    COALESCE(sp.BILL_MONTH, st.BILL_MONTH) AS BILL_MONTH,
    -- PLAN_NAME: сначала из текущего периода, если нет - из STECCOM, если нет - из маппинга по contract_id, если нет - из маппинга по IMEI
    -- Используем NULLIF для обработки пустых строк как NULL
    COALESCE(
        NULLIF(TRIM(sp.PLAN_NAME), ''),
        NULLIF(TRIM(st.STECCOM_PLAN_NAME_MONTHLY), ''),
        NULLIF(TRIM(cpm.plan_name), ''),
        NULLIF(TRIM(ipm.plan_name), '')
    ) AS PLAN_NAME,
    
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
    -- Разделение на основной тариф и suspended
    COALESCE(st.STECCOM_MONTHLY_AMOUNT, 0) AS STECCOM_MONTHLY_AMOUNT,
    COALESCE(st.STECCOM_SUSPENDED_AMOUNT, 0) AS STECCOM_SUSPENDED_AMOUNT,
    -- Общая сумма для обратной совместимости
    COALESCE(st.STECCOM_TOTAL_AMOUNT, 0) AS STECCOM_TOTAL_AMOUNT,
    -- Две отдельные колонки для планов: основной и suspended
    st.STECCOM_PLAN_NAME_MONTHLY AS STECCOM_PLAN_NAME_MONTHLY,
    st.STECCOM_PLAN_NAME_SUSPENDED AS STECCOM_PLAN_NAME_SUSPENDED
    
FROM spnet_data sp
FULL OUTER JOIN steccom_data st 
    ON sp.IMEI = st.IMEI 
    AND sp.CONTRACT_ID = st.CONTRACT_ID
    AND sp.BILL_MONTH = st.BILL_MONTH
LEFT JOIN contract_plan_mapping cpm
    ON COALESCE(sp.CONTRACT_ID, st.CONTRACT_ID) = cpm.contract_id
LEFT JOIN imei_plan_mapping ipm
    ON COALESCE(sp.IMEI, st.IMEI) = ipm.imei
ORDER BY 
    COALESCE(sp.IMEI, st.IMEI),
    COALESCE(sp.BILL_MONTH, st.BILL_MONTH) DESC;

COMMENT ON VIEW V_CONSOLIDATED_OVERAGE_REPORT IS 'Сводный отчет по превышению с данными из SPNet и STECCOM. КАЖДАЯ СТРОКА = ОТДЕЛЬНЫЙ ПЕРИОД (BILL_MONTH). Периоды НЕ суммируются! STECCOM данные разделены: STECCOM_MONTHLY_AMOUNT/SUSPENDED_AMOUNT (суммы) и STECCOM_PLAN_NAME_MONTHLY/SUSPENDED (планы). Группировка: IMEI + CONTRACT_ID + BILL_MONTH - одна строка на период. PLAN_NAME заполняется из текущего периода, если отсутствует - из маппинга по contract_id или IMEI из других периодов.';

\echo 'View V_CONSOLIDATED_OVERAGE_REPORT создан успешно!'
