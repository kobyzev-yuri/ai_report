-- ============================================================================
-- V_CONSOLIDATED_OVERAGE_REPORT
-- Сводный отчет по превышению с данными из SPNet и STECCOM
-- База данных: PostgreSQL (testing)
-- ============================================================================

DROP VIEW IF EXISTS V_CONSOLIDATED_OVERAGE_REPORT CASCADE;

CREATE OR REPLACE VIEW V_CONSOLIDATED_OVERAGE_REPORT AS
SELECT 
    COALESCE(ov.IMEI, se.ICC_ID_IMEI) AS IMEI,
    COALESCE(ov.CONTRACT_ID, se.CONTRACT_ID) AS CONTRACT_ID,
    -- BILL_MONTH в формате YYYYMM
    COALESCE(
        LPAD(CAST(ov.BILL_MONTH % 10000 AS TEXT), 4, '0') || LPAD(CAST(ov.BILL_MONTH / 10000 AS TEXT), 2, '0'),
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM')
    ) AS BILL_MONTH,
    ov.PLAN_NAME,
    se.ACTIVATION_DATE,
    
    -- SPNet данные (суммируем по всем типам использования)
    SUM(ov.SPNET_TOTAL_AMOUNT) AS SPNET_TOTAL_AMOUNT,
    MAX(ov.INCLUDED_KB) AS INCLUDED_KB,
    SUM(ov.TOTAL_USAGE_KB) AS TOTAL_USAGE_KB,
    SUM(ov.OVERAGE_KB) AS OVERAGE_KB,
    SUM(ov.CALCULATED_OVERAGE_CHARGE) AS CALCULATED_OVERAGE,
    
    -- STECCOM данные
    SUM(se.AMOUNT) AS STECCOM_TOTAL_AMOUNT
    
FROM V_SPNET_OVERAGE_ANALYSIS ov
FULL OUTER JOIN STECCOM_EXPENSES se 
    ON ov.IMEI = se.ICC_ID_IMEI 
    AND ov.CONTRACT_ID = se.CONTRACT_ID
    AND TO_CHAR(se.INVOICE_DATE, 'YYYYMM') = 
        LPAD(CAST(ov.BILL_MONTH % 10000 AS TEXT), 4, '0') || LPAD(CAST(ov.BILL_MONTH / 10000 AS TEXT), 2, '0')
GROUP BY 
    COALESCE(ov.IMEI, se.ICC_ID_IMEI),
    COALESCE(ov.CONTRACT_ID, se.CONTRACT_ID),
    COALESCE(
        LPAD(CAST(ov.BILL_MONTH % 10000 AS TEXT), 4, '0') || LPAD(CAST(ov.BILL_MONTH / 10000 AS TEXT), 2, '0'),
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM')
    ),
    ov.PLAN_NAME,
    se.ACTIVATION_DATE;

COMMENT ON VIEW V_CONSOLIDATED_OVERAGE_REPORT IS 'Сводный отчет по превышению с данными из SPNet и STECCOM';

\echo 'View V_CONSOLIDATED_OVERAGE_REPORT создан успешно!'




