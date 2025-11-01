-- ============================================================================
-- V_SPNET_OVERAGE_ANALYSIS
-- Анализ превышения трафика по IMEI с расчетом стоимости
-- База данных: PostgreSQL (testing)
-- ============================================================================

DROP VIEW IF EXISTS V_SPNET_OVERAGE_ANALYSIS CASCADE;

CREATE OR REPLACE VIEW V_SPNET_OVERAGE_ANALYSIS AS
SELECT 
    st.IMEI,
    st.CONTRACT_ID,
    st.BILL_MONTH,
    st.PLAN_NAME,
    tp.PLAN_CODE,
    tp.INCLUDED_KB,
    
    -- Разделение на трафик (bytes) и события (count)
    -- Трафик: только для SBD Data Usage в байтах
    SUM(CASE WHEN st.USAGE_TYPE = 'SBD Data Usage' THEN st.USAGE_BYTES ELSE 0 END) AS TRAFFIC_USAGE_BYTES,
    
    -- События: количество сессий/вызовов (CALL_SESSION_COUNT)
    -- Для всех типов использования суммируем события
    SUM(COALESCE(st.CALL_SESSION_COUNT, 0)) AS EVENTS_COUNT,
    
    -- Также можно отдельно считать события по типам
    SUM(CASE WHEN st.USAGE_TYPE = 'SBD Data Usage' THEN COALESCE(st.CALL_SESSION_COUNT, 0) ELSE 0 END) AS DATA_USAGE_EVENTS,
    SUM(CASE WHEN st.USAGE_TYPE = 'SBD Mailbox Checks' THEN COALESCE(st.CALL_SESSION_COUNT, 0) ELSE 0 END) AS MAILBOX_EVENTS,
    SUM(CASE WHEN st.USAGE_TYPE = 'SBD Registrations' THEN COALESCE(st.CALL_SESSION_COUNT, 0) ELSE 0 END) AS REGISTRATION_EVENTS,
    
    -- Количество записей
    COUNT(*) AS RECORD_COUNT,
    
    -- Трафик в KB (только SBD Data Usage)
    ROUND(CAST(SUM(CASE WHEN st.USAGE_TYPE = 'SBD Data Usage' THEN st.USAGE_BYTES ELSE 0 END) AS NUMERIC) / 1000, 2) AS TOTAL_USAGE_KB,
    
    -- Превышение (только для SBD Data Usage)
    CASE 
        WHEN SUM(CASE WHEN st.USAGE_TYPE = 'SBD Data Usage' THEN st.USAGE_BYTES ELSE 0 END) > 0 
             AND tp.ACTIVE = TRUE THEN
            GREATEST(0, ROUND(
                CAST(SUM(CASE WHEN st.USAGE_TYPE = 'SBD Data Usage' THEN st.USAGE_BYTES ELSE 0 END) AS NUMERIC) / 1000 - tp.INCLUDED_KB, 
                2
            ))
        ELSE 0
    END AS OVERAGE_KB,
    
    -- Расчет стоимости превышения через функцию (только для SBD Data Usage)
    CASE 
        WHEN SUM(CASE WHEN st.USAGE_TYPE = 'SBD Data Usage' THEN st.USAGE_BYTES ELSE 0 END) > 0 
             AND tp.ACTIVE = TRUE THEN
            calculate_overage(
                st.PLAN_NAME, 
                SUM(CASE WHEN st.USAGE_TYPE = 'SBD Data Usage' THEN st.USAGE_BYTES ELSE 0 END)
            )
        ELSE 0
    END AS CALCULATED_OVERAGE_CHARGE,
    
    -- Сумма из отчета SPNet (все типы использования)
    SUM(st.TOTAL_AMOUNT) AS SPNET_TOTAL_AMOUNT
    
FROM SPNET_TRAFFIC st
LEFT JOIN TARIFF_PLANS tp ON st.PLAN_NAME = tp.PLAN_NAME AND tp.ACTIVE = TRUE
GROUP BY 
    st.IMEI,
    st.CONTRACT_ID,
    st.BILL_MONTH,
    st.PLAN_NAME,
    tp.PLAN_CODE,
    tp.INCLUDED_KB,
    tp.ACTIVE;

COMMENT ON VIEW V_SPNET_OVERAGE_ANALYSIS IS 'Анализ превышения трафика по IMEI с расчетом стоимости. Разделение на трафик (bytes) и события (count)';

\echo 'View V_SPNET_OVERAGE_ANALYSIS создан успешно!'
