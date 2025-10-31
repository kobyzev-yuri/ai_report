-- ============================================================================
-- V_SPNET_OVERAGE_ANALYSIS
-- Анализ превышения трафика по IMEI с расчетом стоимости
-- База данных: Oracle
-- ============================================================================

CREATE OR REPLACE VIEW V_SPNET_OVERAGE_ANALYSIS AS
SELECT 
    st.IMEI,
    st.CONTRACT_ID,
    st.BILL_MONTH,
    st.PLAN_NAME,
    st.USAGE_TYPE,
    tp.PLAN_CODE,
    tp.INCLUDED_KB,
    
    -- Трафик
    COUNT(*) AS RECORD_COUNT,
    SUM(st.USAGE_BYTES) AS TOTAL_USAGE_BYTES,
    ROUND(SUM(st.USAGE_BYTES) / 1000, 2) AS TOTAL_USAGE_KB,
    
    -- Превышение (только для SBD Data Usage)
    CASE 
        WHEN st.USAGE_TYPE = 'SBD Data Usage' AND tp.ACTIVE = 'Y' THEN
            GREATEST(0, ROUND(SUM(st.USAGE_BYTES) / 1000 - tp.INCLUDED_KB, 2))
        ELSE 0
    END AS OVERAGE_KB,
    
    -- Расчет стоимости превышения через функцию (только для SBD Data Usage)
    CASE 
        WHEN st.USAGE_TYPE = 'SBD Data Usage' AND tp.ACTIVE = 'Y' THEN
            CALCULATE_OVERAGE(st.PLAN_NAME, SUM(st.USAGE_BYTES))
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
    st.PLAN_NAME,
    st.USAGE_TYPE,
    tp.PLAN_CODE,
    tp.INCLUDED_KB,
    tp.ACTIVE;

COMMENT ON TABLE V_SPNET_OVERAGE_ANALYSIS IS 'Анализ превышения трафика по IMEI с расчетом стоимости';


