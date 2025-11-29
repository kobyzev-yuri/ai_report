-- ============================================================================
-- V_UNPROFITABLE_CUSTOMERS
-- Представление для выявления убыточных клиентов и клиентов с низкой маржой
-- База данных: Oracle (billing7@bm7)
-- ============================================================================

SET SQLBLANKLINES ON
SET DEFINE OFF

CREATE OR REPLACE VIEW V_UNPROFITABLE_CUSTOMERS AS
SELECT 
    PERIOD,
    CUSTOMER_NAME,
    CODE_1C,
    EXPENSES_USD,
    EXPENSES_RUB,
    REVENUE_RUB,
    PROFIT_RUB,
    MARGIN_PCT,
    COST_PCT,
    STATUS,
    CASE 
        WHEN PROFIT_RUB < 0 THEN 'LOSS'
        WHEN MARGIN_PCT < 10 THEN 'LOW_MARGIN'
        ELSE 'PROFITABLE'
    END AS ALERT_TYPE
FROM V_PROFITABILITY_BY_PERIOD
WHERE STATUS IN ('UNPROFITABLE', 'LOW_MARGIN')
/

COMMENT ON TABLE V_UNPROFITABLE_CUSTOMERS IS 'Убыточные клиенты и клиенты с низкой маржой (<10%). Используется для выявления проблемных позиций.'
/

COMMENT ON COLUMN V_UNPROFITABLE_CUSTOMERS.ALERT_TYPE IS 'Тип алерта: УБЫТОК (прибыль < 0), НИЗКАЯ МАРЖА (маржа < 10%), ПРИБЫЛЬНО'
/

SET DEFINE ON

