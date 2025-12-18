-- ============================================================================
-- V_PROFITABILITY_TREND
-- Представление для анализа тенденций прибыльности с сравнением периодов
-- Использует LAG функцию для сравнения текущего периода с предыдущим
-- База данных: Oracle (billing7@bm7)
-- ============================================================================

SET SQLBLANKLINES ON
SET DEFINE OFF

CREATE OR REPLACE VIEW V_PROFITABILITY_TREND AS
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
    LAG(PROFIT_RUB) OVER (PARTITION BY CUSTOMER_NAME, CODE_1C ORDER BY PERIOD) AS PREV_PROFIT_RUB,
    PROFIT_RUB - LAG(PROFIT_RUB) OVER (PARTITION BY CUSTOMER_NAME, CODE_1C ORDER BY PERIOD) AS PROFIT_CHANGE,
    ROUND(((PROFIT_RUB - LAG(PROFIT_RUB) OVER (PARTITION BY CUSTOMER_NAME, CODE_1C ORDER BY PERIOD)) / 
           NULLIF(LAG(PROFIT_RUB) OVER (PARTITION BY CUSTOMER_NAME, CODE_1C ORDER BY PERIOD), 0) * 100), 2) AS PROFIT_CHANGE_PCT,
    CASE 
        WHEN LAG(PROFIT_RUB) OVER (PARTITION BY CUSTOMER_NAME, CODE_1C ORDER BY PERIOD) IS NOT NULL 
         AND PROFIT_RUB < LAG(PROFIT_RUB) OVER (PARTITION BY CUSTOMER_NAME, CODE_1C ORDER BY PERIOD) 
        THEN 'DECREASE'
        WHEN LAG(PROFIT_RUB) OVER (PARTITION BY CUSTOMER_NAME, CODE_1C ORDER BY PERIOD) IS NOT NULL 
         AND PROFIT_RUB > LAG(PROFIT_RUB) OVER (PARTITION BY CUSTOMER_NAME, CODE_1C ORDER BY PERIOD) 
        THEN 'INCREASE'
        ELSE NULL
    END AS TREND
FROM V_PROFITABILITY_BY_PERIOD
/

COMMENT ON TABLE V_PROFITABILITY_TREND IS 'Анализ тенденций прибыльности с сравнением текущего периода с предыдущим. Использует LAG функцию для выявления ухудшения/улучшения прибыльности.'
/

COMMENT ON COLUMN V_PROFITABILITY_TREND.PREV_PROFIT_RUB IS 'Прибыль в предыдущем периоде (через LAG)'
/
COMMENT ON COLUMN V_PROFITABILITY_TREND.PROFIT_CHANGE IS 'Изменение прибыли: текущий период - предыдущий период'
/
COMMENT ON COLUMN V_PROFITABILITY_TREND.PROFIT_CHANGE_PCT IS 'Процент изменения прибыли относительно предыдущего периода'
/
COMMENT ON COLUMN V_PROFITABILITY_TREND.TREND IS 'Тенденция: DECREASE (ухудшение), INCREASE (улучшение) или NULL (если нет предыдущего периода)'
/

SET DEFINE ON

