-- ============================================================================
-- V_CONSOLIDATED_OVERAGE_REPORT
-- Сводный отчет по превышению с данными из SPNet и STECCOM
-- База данных: Oracle (production)
-- ВАЖНО: Периоды НЕ суммируются - каждая строка = отдельный период (BILL_MONTH)
-- ============================================================================

SET SQLBLANKLINES ON
SET DEFINE OFF

CREATE OR REPLACE VIEW V_CONSOLIDATED_OVERAGE_REPORT AS
WITH -- Маппинг plan_name по contract_id из других периодов SPNET_TRAFFIC (берем самый частый план)
contract_plan_mapping AS (
    SELECT 
        CONTRACT_ID,
        PLAN_NAME
    FROM (
        SELECT 
            st.CONTRACT_ID,
            st.PLAN_NAME,
            COUNT(*) AS plan_count,
            ROW_NUMBER() OVER (PARTITION BY st.CONTRACT_ID ORDER BY COUNT(*) DESC, st.PLAN_NAME) AS rn
        FROM SPNET_TRAFFIC st
        WHERE st.CONTRACT_ID IS NOT NULL
          AND st.PLAN_NAME IS NOT NULL
          AND st.PLAN_NAME != ''
        GROUP BY st.CONTRACT_ID, st.PLAN_NAME
    )
    WHERE rn = 1
),
-- Маппинг plan_name по IMEI из других периодов SPNET_TRAFFIC (берем самый частый план)
imei_plan_mapping AS (
    SELECT 
        IMEI,
        PLAN_NAME
    FROM (
        SELECT 
            st.IMEI,
            st.PLAN_NAME,
            COUNT(*) AS plan_count,
            ROW_NUMBER() OVER (PARTITION BY st.IMEI ORDER BY COUNT(*) DESC, st.PLAN_NAME) AS rn
        FROM SPNET_TRAFFIC st
        WHERE st.IMEI IS NOT NULL
          AND st.PLAN_NAME IS NOT NULL
          AND st.PLAN_NAME != ''
        GROUP BY st.IMEI, st.PLAN_NAME
    )
    WHERE rn = 1
),
spnet_data AS (
    SELECT 
        ov.IMEI,
        ov.CONTRACT_ID,
        -- BILL_MONTH в формате YYYYMM (не суммируем!)
        -- BILL_MONTH в SPNET_TRAFFIC: новые данные в формате YYYYMM (202509), старые в MMYYYY (92025)
        -- Преобразуем в строку YYYYMM с обратной совместимостью
        CASE 
            WHEN ov.BILL_MONTH >= 200000 THEN
                -- Уже в формате YYYYMM (например, 202509)
                TO_CHAR(ov.BILL_MONTH)
            ELSE
                -- Старый формат MMYYYY (например, 92025) -> преобразуем в YYYYMM
                LPAD(TO_CHAR(MOD(ov.BILL_MONTH, 10000)), 4, '0') || LPAD(TO_CHAR(TRUNC(ov.BILL_MONTH / 10000)), 2, '0')
        END AS BILL_MONTH,
        ov.PLAN_NAME,
        
        -- Разделение трафика и событий
        SUM(ov.TRAFFIC_USAGE_BYTES) AS TRAFFIC_USAGE_BYTES,
        SUM(ov.EVENTS_COUNT) AS EVENTS_COUNT,
        SUM(ov.DATA_USAGE_EVENTS) AS DATA_USAGE_EVENTS,
        SUM(ov.MAILBOX_EVENTS) AS MAILBOX_EVENTS,
        SUM(ov.REGISTRATION_EVENTS) AS REGISTRATION_EVENTS,
        
        -- SPNet суммы (по каждому периоду отдельно)
        MAX(ov.INCLUDED_KB) AS INCLUDED_KB,
        SUM(ov.TOTAL_USAGE_KB) AS TOTAL_USAGE_KB,
        SUM(ov.OVERAGE_KB) AS OVERAGE_KB,
        SUM(ov.CALCULATED_OVERAGE_CHARGE) AS CALCULATED_OVERAGE,
        -- Сумма из отчета SPNet (стоимость трафика из детализации)
        SUM(ov.SPNET_TOTAL_AMOUNT) AS SPNET_TOTAL_AMOUNT
    FROM V_SPNET_OVERAGE_ANALYSIS ov
    GROUP BY 
        ov.IMEI,
        ov.CONTRACT_ID,
        CASE 
            WHEN ov.BILL_MONTH >= 200000 THEN
                TO_CHAR(ov.BILL_MONTH)
            ELSE
                LPAD(TO_CHAR(MOD(ov.BILL_MONTH, 10000)), 4, '0') || LPAD(TO_CHAR(TRUNC(ov.BILL_MONTH / 10000)), 2, '0')
        END,
        ov.PLAN_NAME
),
steccom_data AS (
    SELECT 
        se.ICC_ID_IMEI AS IMEI,
        se.CONTRACT_ID,
        -- BILL_MONTH - это месяц из INVOICE_DATE (без вычитания)
        -- INVOICE_DATE = 2025-11-02 (ноябрь) → BILL_MONTH = 202511 (ноябрь)
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS BILL_MONTH,
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
        -- Группируем по вычисленному BILL_MONTH (используем то же выражение, что и в SELECT)
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM')
)
SELECT 
    NVL(sp.IMEI, st.IMEI) AS IMEI,
    NVL(sp.CONTRACT_ID, st.CONTRACT_ID) AS CONTRACT_ID,
    NVL(sp.BILL_MONTH, st.BILL_MONTH) AS BILL_MONTH,
    -- PLAN_NAME: сначала из текущего периода, если нет - из STECCOM, если нет - из маппинга по contract_id, если нет - из маппинга по IMEI
    -- Используем NULLIF для обработки пустых строк как NULL
    NVL(
        NULLIF(TRIM(sp.PLAN_NAME), ''),
        NVL(
            NULLIF(TRIM(st.STECCOM_PLAN_NAME_MONTHLY), ''),
            NVL(
                NULLIF(TRIM(cpm.PLAN_NAME), ''),
                NULLIF(TRIM(ipm.PLAN_NAME), '')
            )
        )
    ) AS PLAN_NAME,
    
    -- Разделение трафика и событий (по каждому периоду)
    NVL(sp.TRAFFIC_USAGE_BYTES, 0) AS TRAFFIC_USAGE_BYTES,
    NVL(sp.EVENTS_COUNT, 0) AS EVENTS_COUNT,
    NVL(sp.DATA_USAGE_EVENTS, 0) AS DATA_USAGE_EVENTS,
    NVL(sp.MAILBOX_EVENTS, 0) AS MAILBOX_EVENTS,
    NVL(sp.REGISTRATION_EVENTS, 0) AS REGISTRATION_EVENTS,
    
        -- SPNet данные (по каждому периоду отдельно, НЕ суммируются!)
        NVL(sp.INCLUDED_KB, 0) AS INCLUDED_KB,
    NVL(sp.TOTAL_USAGE_KB, 0) AS TOTAL_USAGE_KB,
    NVL(sp.OVERAGE_KB, 0) AS OVERAGE_KB,
    NVL(sp.CALCULATED_OVERAGE, 0) AS CALCULATED_OVERAGE,
    NVL(sp.SPNET_TOTAL_AMOUNT, 0) AS SPNET_TOTAL_AMOUNT,
    
    -- STECCOM данные (по каждому периоду отдельно)
    -- Две отдельные колонки для планов: основной и suspended
    st.STECCOM_PLAN_NAME_MONTHLY AS STECCOM_PLAN_NAME_MONTHLY,
    st.STECCOM_PLAN_NAME_SUSPENDED AS STECCOM_PLAN_NAME_SUSPENDED
    
FROM spnet_data sp
FULL OUTER JOIN steccom_data st 
    ON sp.IMEI = st.IMEI 
    AND sp.CONTRACT_ID = st.CONTRACT_ID
    AND sp.BILL_MONTH = st.BILL_MONTH
LEFT JOIN contract_plan_mapping cpm
    ON NVL(sp.CONTRACT_ID, st.CONTRACT_ID) = cpm.CONTRACT_ID
LEFT JOIN imei_plan_mapping ipm
    ON NVL(sp.IMEI, st.IMEI) = ipm.IMEI
ORDER BY 
    NVL(sp.IMEI, st.IMEI),
    NVL(sp.BILL_MONTH, st.BILL_MONTH) DESC
/

COMMENT ON TABLE V_CONSOLIDATED_OVERAGE_REPORT IS 'Сводный отчет по превышению с данными из SPNet и STECCOM. КАЖДАЯ СТРОКА = ОТДЕЛЬНЫЙ ПЕРИОД (BILL_MONTH). Периоды НЕ суммируются! ВАЖНО: BILL_MONTH - это месяц из INVOICE_DATE (без вычитания). Для STECCOM: INVOICE_DATE = 2025-11-02 (ноябрь) → BILL_MONTH = 202511 (ноябрь). STECCOM данные содержат только планы: STECCOM_PLAN_NAME_MONTHLY/SUSPENDED. Группировка: IMEI + CONTRACT_ID + BILL_MONTH - одна строка на период. PLAN_NAME заполняется из текущего периода, если отсутствует - из маппинга по contract_id или IMEI из других периодов.'
/

SET DEFINE ON
