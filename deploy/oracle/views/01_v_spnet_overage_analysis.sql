-- ============================================================================
-- V_SPNET_OVERAGE_ANALYSIS
-- Анализ превышения трафика по IMEI с расчетом стоимости
-- База данных: Oracle (production)
-- ============================================================================

SET SQLBLANKLINES ON
SET DEFINE OFF

CREATE OR REPLACE VIEW V_SPNET_OVERAGE_ANALYSIS AS
SELECT 
    st.IMEI,
    st.CONTRACT_ID,
    st.BILL_MONTH,
    st.PLAN_NAME,
    MAX(tp.PLAN_CODE) AS PLAN_CODE,
    MAX(tp.INCLUDED_KB) AS INCLUDED_KB,
    
    -- Разделение на трафик (bytes) и события (count)
    -- Трафик: только для SBD Data Usage в байтах
    SUM(CASE WHEN st.USAGE_TYPE = 'SBD Data Usage' THEN st.USAGE_BYTES ELSE 0 END) AS TRAFFIC_USAGE_BYTES,
    
    -- События: только MAILBOX и REGISTRATION (USAGE_UNIT = 'EVENT')
    -- SBD Data Usage с USAGE_UNIT = 'BYTES' - это трафик, а не события!
    -- MAILBOX_EVENTS: только для SBD Mailbox Checks с USAGE_UNIT = 'EVENT'
    SUM(
        CASE 
            WHEN st.USAGE_TYPE = 'SBD Mailbox Checks' 
                 AND UPPER(TRIM(st.USAGE_UNIT)) = 'EVENT' THEN 
                COALESCE(st.USAGE_BYTES, st.ACTUAL_USAGE, 0)
            ELSE 0
        END
    ) AS MAILBOX_EVENTS,
    -- REGISTRATION_EVENTS: только для SBD Registrations с USAGE_UNIT = 'EVENT'
    SUM(
        CASE 
            WHEN st.USAGE_TYPE = 'SBD Registrations' 
                 AND UPPER(TRIM(st.USAGE_UNIT)) = 'EVENT' THEN 
                COALESCE(st.USAGE_BYTES, st.ACTUAL_USAGE, 0)
            ELSE 0
        END
    ) AS REGISTRATION_EVENTS,
    -- EVENTS_COUNT: сумма MAILBOX_EVENTS + REGISTRATION_EVENTS
    SUM(
        CASE 
            WHEN (st.USAGE_TYPE = 'SBD Mailbox Checks' OR st.USAGE_TYPE = 'SBD Registrations')
                 AND UPPER(TRIM(st.USAGE_UNIT)) = 'EVENT' THEN 
                COALESCE(st.USAGE_BYTES, st.ACTUAL_USAGE, 0)
            ELSE 0
        END
    ) AS EVENTS_COUNT,
    
    -- Количество записей
    COUNT(*) AS RECORD_COUNT,
    
    -- Трафик в KB (только SBD Data Usage)
    ROUND(SUM(CASE WHEN st.USAGE_TYPE = 'SBD Data Usage' THEN st.USAGE_BYTES ELSE 0 END) / 1000, 2) AS TOTAL_USAGE_KB,
    
    -- Превышение (только для SBD Data Usage)
    CASE 
        WHEN SUM(CASE WHEN st.USAGE_TYPE = 'SBD Data Usage' THEN st.USAGE_BYTES ELSE 0 END) > 0 
             AND MAX(tp.ACTIVE) = 'Y' THEN
            GREATEST(0, ROUND(
                SUM(CASE WHEN st.USAGE_TYPE = 'SBD Data Usage' THEN st.USAGE_BYTES ELSE 0 END) / 1000 - MAX(tp.INCLUDED_KB), 
                2
            ))
        ELSE 0
    END AS OVERAGE_KB,
    
    -- Расчет стоимости превышения через функцию (только для SBD Data Usage)
    -- Используем MAX(st.PLAN_NAME) для корректной работы в агрегированном контексте
    CASE 
        WHEN SUM(CASE WHEN st.USAGE_TYPE = 'SBD Data Usage' THEN st.USAGE_BYTES ELSE 0 END) > 0 
             AND MAX(tp.ACTIVE) = 'Y' THEN
            CALCULATE_OVERAGE(
                MAX(st.PLAN_NAME), 
                SUM(CASE WHEN st.USAGE_TYPE = 'SBD Data Usage' THEN st.USAGE_BYTES ELSE 0 END)
            )
        ELSE 0
    END AS CALCULATED_OVERAGE_CHARGE,
    
    -- Сумма из отчета SPNet (все типы использования)
    SUM(st.TOTAL_AMOUNT) AS SPNET_TOTAL_AMOUNT
    
FROM SPNET_TRAFFIC st
LEFT JOIN TARIFF_PLANS tp ON st.PLAN_NAME = tp.PLAN_NAME AND tp.ACTIVE = 'Y'
GROUP BY 
    st.IMEI,
    st.CONTRACT_ID,
    st.BILL_MONTH,
    st.PLAN_NAME
/

COMMENT ON TABLE V_SPNET_OVERAGE_ANALYSIS IS 'Анализ превышения трафика по IMEI с расчетом стоимости. Разделение на трафик (bytes) и события (count). Событиями считаются только MAILBOX и REGISTRATION с USAGE_UNIT=EVENT. SBD Data Usage с USAGE_UNIT=BYTES - это трафик, а не события. EVENTS_COUNT = MAILBOX_EVENTS + REGISTRATION_EVENTS.'
/

SET DEFINE ON
