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
    
    -- События: для событий (USAGE_UNIT = 'EVENT') используем значение USAGE_BYTES,
    -- для остальных типов используем CALL_SESSION_COUNT или COUNT записей
    SUM(
        CASE 
            WHEN UPPER(TRIM(st.USAGE_UNIT)) = 'EVENT' THEN 
                COALESCE(st.USAGE_BYTES, st.ACTUAL_USAGE, 0)
            WHEN st.CALL_SESSION_COUNT IS NOT NULL THEN 
                st.CALL_SESSION_COUNT
            ELSE 1
        END
    ) AS EVENTS_COUNT,
    
    -- Отдельно считаем события по типам
    -- Для событий (USAGE_UNIT = 'EVENT') суммируем USAGE_BYTES, иначе используем CALL_SESSION_COUNT или COUNT
    SUM(
        CASE 
            WHEN st.USAGE_TYPE = 'SBD Data Usage' THEN
                CASE 
                    WHEN UPPER(TRIM(st.USAGE_UNIT)) = 'EVENT' THEN 
                        COALESCE(st.USAGE_BYTES, st.ACTUAL_USAGE, 0)
                    WHEN st.CALL_SESSION_COUNT IS NOT NULL THEN 
                        st.CALL_SESSION_COUNT
                    ELSE 1
                END
            ELSE 0
        END
    ) AS DATA_USAGE_EVENTS,
    SUM(
        CASE 
            WHEN st.USAGE_TYPE = 'SBD Mailbox Checks' THEN
                CASE 
                    WHEN UPPER(TRIM(st.USAGE_UNIT)) = 'EVENT' THEN 
                        COALESCE(st.USAGE_BYTES, st.ACTUAL_USAGE, 0)
                    WHEN st.CALL_SESSION_COUNT IS NOT NULL THEN 
                        st.CALL_SESSION_COUNT
                    ELSE 1
                END
            ELSE 0
        END
    ) AS MAILBOX_EVENTS,
    SUM(
        CASE 
            WHEN st.USAGE_TYPE = 'SBD Registrations' THEN
                CASE 
                    WHEN UPPER(TRIM(st.USAGE_UNIT)) = 'EVENT' THEN 
                        COALESCE(st.USAGE_BYTES, st.ACTUAL_USAGE, 0)
                    WHEN st.CALL_SESSION_COUNT IS NOT NULL THEN 
                        st.CALL_SESSION_COUNT
                    ELSE 1
                END
            ELSE 0
        END
    ) AS REGISTRATION_EVENTS,
    
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

COMMENT ON VIEW V_SPNET_OVERAGE_ANALYSIS IS 'Анализ превышения трафика по IMEI с расчетом стоимости. Разделение на трафик (bytes) и события (count). Для событий (USAGE_UNIT=EVENT) используется значение USAGE_BYTES, для остальных - CALL_SESSION_COUNT или COUNT записей';

\echo 'View V_SPNET_OVERAGE_ANALYSIS создан успешно!'
