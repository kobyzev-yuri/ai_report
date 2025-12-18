-- ============================================================================
-- V_COST_OF_GOODS_SOLD
-- Расчет себестоимости на основе выверенного отчета по затратам
-- Использует V_CONSOLIDATED_REPORT_WITH_BILLING как основу для затрат
-- Добавляет доходы из ANALYTICS и BM_INVOICE_ITEM
-- База данных: Oracle (billing7@bm7)
-- ============================================================================
-- ВАЖНО:
-- - Финансистов волнуют ВСЕ SUB-XXXXX, даже тестовые
-- - Иридиум не всегда выставляет счета, особенно по тестовым услугам
-- - Есть SUB- которых может не быть в счетах Иридиум (тесты, еще не активированы)
-- - Есть SUB- в счетах Иридиум, но нет в доходах (не попали в счета-фактуры)
-- ============================================================================

SET SQLBLANKLINES ON
SET DEFINE OFF

CREATE OR REPLACE VIEW V_COST_OF_GOODS_SOLD AS
WITH -- Все SBD услуги с SUB-XXXXX (включая тестовые)
all_services AS (
    SELECT DISTINCT
        s.SERVICE_ID,
        s.LOGIN AS CONTRACT_ID,
        s.VSAT AS IMEI,
        s.STATUS,
        s.CREATE_DATE,
        s.START_DATE,
        s.STOP_DATE,
        CASE 
            WHEN s.LOGIN LIKE '%-clone-%' THEN 'TEST_CLONE'
            WHEN s.STATUS != 10 THEN 'INACTIVE'
            ELSE 'ACTIVE'
        END AS SERVICE_STATUS_TYPE
    FROM SERVICES s
    WHERE s.TYPE_ID = 9002  -- SBD услуги
      AND s.LOGIN LIKE 'SUB-%'
),
-- Доходы из ANALYTICS (ВСЕ начисления, включая тесты)
revenues_analytics AS (
    SELECT 
        s.LOGIN AS CONTRACT_ID,
        a.PERIOD_ID,
        SUM(a.MONEY) AS REVENUE_ANALYTICS,
        COUNT(*) AS ANALYTICS_COUNT,
        COUNT(CASE WHEN a.INVOICE_ITEM_ID IS NOT NULL THEN 1 END) AS INVOICE_ITEMS_COUNT,
        COUNT(CASE WHEN a.INVOICE_ITEM_ID IS NULL THEN 1 END) AS TEST_ITEMS_COUNT
    FROM ANALYTICS a
    JOIN SERVICES s ON a.SERVICE_ID = s.SERVICE_ID
    WHERE s.TYPE_ID = 9002
      AND s.LOGIN LIKE 'SUB-%'
    GROUP BY s.LOGIN, a.PERIOD_ID
),
-- Доходы из BM_INVOICE_ITEM (только попавшие в счета-фактуры)
revenues_invoices AS (
    SELECT 
        s.LOGIN AS CONTRACT_ID,
        ii.PERIOD_ID,
        SUM(ii.MONEY - NVL(ii.MONEY_REVERSED, 0)) AS REVENUE_INVOICES,
        COUNT(*) AS INVOICE_ITEMS_COUNT
    FROM BM_INVOICE_ITEM ii
    JOIN SERVICES s ON ii.SERVICE_ID = s.SERVICE_ID
    WHERE s.TYPE_ID = 9002
      AND s.LOGIN LIKE 'SUB-%'
    GROUP BY s.LOGIN, ii.PERIOD_ID
),
-- Маппинг PERIOD_ID на BILL_MONTH через BM_PERIOD
periods_mapping AS (
    SELECT 
        PERIOD_ID,
        TO_NUMBER(TO_CHAR(START_DATE, 'YYYYMM')) AS BILL_MONTH_START,
        TO_NUMBER(TO_CHAR(STOP_DATE, 'YYYYMM')) AS BILL_MONTH_END,
        MONTH AS PERIOD_NAME,
        START_DATE,
        STOP_DATE
    FROM BM_PERIOD
)
-- Основной запрос: используем V_CONSOLIDATED_REPORT_WITH_BILLING как основу для затрат
SELECT 
    -- Данные из выверенного отчета по затратам
    costs.BILL_MONTH,
    costs.BILL_MONTH_YYYMM,
    costs.FINANCIAL_PERIOD,
    costs.CONTRACT_ID,
    costs.IMEI,
    costs.SERVICE_ID,
    costs.SERVICE_STATUS,
    costs.CUSTOMER_NAME,
    costs.CODE_1C,
    costs.AGREEMENT_NUMBER,
    costs.ORDER_NUMBER,
    costs.PLAN_NAME,
    
    -- Затраты из выверенного отчета (НЕ ТРОГАЕМ!)
    costs.CALCULATED_OVERAGE,
    costs.SPNET_TOTAL_AMOUNT,
    costs.FEE_ACTIVATION_FEE,
    costs.FEE_ADVANCE_CHARGE,
    costs.FEE_ADVANCE_CHARGE_PREVIOUS_MONTH,
    costs.FEE_CREDIT,
    costs.FEE_CREDITED,
    costs.FEE_PRORATED,
    costs.FEES_TOTAL,
    -- Итого затрат Иридиум
    NVL(costs.CALCULATED_OVERAGE, 0) + 
    NVL(costs.SPNET_TOTAL_AMOUNT, 0) + 
    NVL(costs.FEES_TOTAL, 0) AS TOTAL_COST_IRIDIUM,
    
    -- Трафик
    costs.TOTAL_USAGE_KB,
    costs.OVERAGE_KB,
    costs.TRAFFIC_USAGE_BYTES,
    
    -- Доходы (добавляем к выверенному отчету)
    NVL(ra.REVENUE_ANALYTICS, 0) AS REVENUE_ANALYTICS,
    NVL(ri.REVENUE_INVOICES, 0) AS REVENUE_INVOICES,
    NVL(ra.ANALYTICS_COUNT, 0) AS ANALYTICS_COUNT,
    NVL(ra.INVOICE_ITEMS_COUNT, 0) AS INVOICE_ITEMS_COUNT,
    NVL(ra.TEST_ITEMS_COUNT, 0) AS TEST_ITEMS_COUNT,
    
    -- Период из доходов
    ra.PERIOD_ID AS PERIOD_ID_REVENUE,
    pm.PERIOD_NAME AS PERIOD_NAME_REVENUE,
    
    -- Флаги наличия данных
    CASE WHEN ra.CONTRACT_ID IS NOT NULL THEN 'Y' ELSE 'N' END AS HAS_REVENUE_ANALYTICS,
    CASE WHEN ri.CONTRACT_ID IS NOT NULL THEN 'Y' ELSE 'N' END AS HAS_REVENUE_INVOICES,
    CASE WHEN costs.CONTRACT_ID IS NOT NULL THEN 'Y' ELSE 'N' END AS HAS_COST,
    
    -- Проблемные случаи
    CASE 
        WHEN ra.CONTRACT_ID IS NOT NULL AND costs.CONTRACT_ID IS NULL THEN 'REVENUE_NO_COST'
        WHEN ra.CONTRACT_ID IS NULL AND ri.CONTRACT_ID IS NULL AND costs.CONTRACT_ID IS NOT NULL THEN 'COST_NO_REVENUE'
        WHEN ra.TEST_ITEMS_COUNT > 0 THEN 'HAS_TEST_ITEMS'
        WHEN svc.SERVICE_STATUS_TYPE = 'TEST_CLONE' THEN 'TEST_CLONE'
        ELSE 'OK'
    END AS ISSUE_FLAG,
    
    -- Себестоимость (если есть и доходы и затраты)
    CASE 
        WHEN NVL(ra.REVENUE_ANALYTICS, 0) > 0 
             AND (NVL(costs.CALCULATED_OVERAGE, 0) + NVL(costs.SPNET_TOTAL_AMOUNT, 0) + NVL(costs.FEES_TOTAL, 0)) > 0 
        THEN ROUND(
            (NVL(costs.CALCULATED_OVERAGE, 0) + NVL(costs.SPNET_TOTAL_AMOUNT, 0) + NVL(costs.FEES_TOTAL, 0)) 
            / ra.REVENUE_ANALYTICS * 100, 
            2
        )
        ELSE NULL
    END AS COST_PERCENTAGE,
    
    -- Маржинальность (если есть и доходы и затраты)
    CASE 
        WHEN NVL(ra.REVENUE_ANALYTICS, 0) > 0 
             AND (NVL(costs.CALCULATED_OVERAGE, 0) + NVL(costs.SPNET_TOTAL_AMOUNT, 0) + NVL(costs.FEES_TOTAL, 0)) > 0 
        THEN ROUND(
            (ra.REVENUE_ANALYTICS - (NVL(costs.CALCULATED_OVERAGE, 0) + NVL(costs.SPNET_TOTAL_AMOUNT, 0) + NVL(costs.FEES_TOTAL, 0))) 
            / ra.REVENUE_ANALYTICS * 100, 
            2
        )
        ELSE NULL
    END AS MARGIN_PERCENTAGE
    
FROM all_services svc
-- Используем выверенный отчет по затратам как основу
LEFT JOIN V_CONSOLIDATED_REPORT_WITH_BILLING costs 
    ON svc.CONTRACT_ID = costs.CONTRACT_ID
    AND svc.IMEI = costs.IMEI
-- Добавляем доходы из ANALYTICS
LEFT JOIN revenues_analytics ra 
    ON svc.CONTRACT_ID = ra.CONTRACT_ID
-- Добавляем доходы из BM_INVOICE_ITEM
LEFT JOIN revenues_invoices ri 
    ON svc.CONTRACT_ID = ri.CONTRACT_ID 
    AND NVL(ra.PERIOD_ID, -1) = NVL(ri.PERIOD_ID, -1)
-- Маппинг периодов
LEFT JOIN periods_mapping pm 
    ON NVL(ra.PERIOD_ID, ri.PERIOD_ID) = pm.PERIOD_ID
-- Сопоставление периодов: BILL_MONTH из затрат должен попадать в период доходов
WHERE (
    -- Включаем все SUB- из затрат
    costs.CONTRACT_ID IS NOT NULL
    -- Или все SUB- с доходами (даже если нет затрат)
    OR ra.CONTRACT_ID IS NOT NULL
    -- Или все SUB- из services (для полного покрытия)
    OR svc.CONTRACT_ID IS NOT NULL
)
ORDER BY 
    svc.CONTRACT_ID,
    costs.BILL_MONTH_YYYMM NULLS LAST,
    NVL(ra.PERIOD_ID, ri.PERIOD_ID) NULLS LAST;

-- Комментарии
COMMENT ON TABLE V_COST_OF_GOODS_SOLD IS 'Расчет себестоимости на основе выверенного отчета по затратам V_CONSOLIDATED_REPORT_WITH_BILLING. Добавляет доходы из ANALYTICS и BM_INVOICE_ITEM. Включает ВСЕ SUB-XXXXX, даже тестовые. Выявляет проблемные случаи: REVENUE_NO_COST, COST_NO_REVENUE, HAS_TEST_ITEMS, TEST_CLONE.';
COMMENT ON COLUMN V_COST_OF_GOODS_SOLD.CONTRACT_ID IS 'ID контракта (SUB-XXXXX) - ключевая связь между доходами и затратами';
COMMENT ON COLUMN V_COST_OF_GOODS_SOLD.TOTAL_COST_IRIDIUM IS 'Итого затрат Иридиум: CALCULATED_OVERAGE + SPNET_TOTAL_AMOUNT + FEES_TOTAL (из выверенного отчета)';
COMMENT ON COLUMN V_COST_OF_GOODS_SOLD.REVENUE_ANALYTICS IS 'Доходы из ANALYTICS (ВСЕ начисления, включая тесты)';
COMMENT ON COLUMN V_COST_OF_GOODS_SOLD.REVENUE_INVOICES IS 'Доходы из BM_INVOICE_ITEM (только попавшие в счета-фактуры)';
COMMENT ON COLUMN V_COST_OF_GOODS_SOLD.TEST_ITEMS_COUNT IS 'Количество тестовых начислений (INVOICE_ITEM_ID IS NULL в ANALYTICS)';
COMMENT ON COLUMN V_COST_OF_GOODS_SOLD.ISSUE_FLAG IS 'Флаг проблемных случаев: REVENUE_NO_COST, COST_NO_REVENUE, HAS_TEST_ITEMS, TEST_CLONE, OK';
COMMENT ON COLUMN V_COST_OF_GOODS_SOLD.COST_PERCENTAGE IS 'Себестоимость в процентах: (Затраты / Доходы) * 100';
COMMENT ON COLUMN V_COST_OF_GOODS_SOLD.MARGIN_PERCENTAGE IS 'Маржинальность в процентах: ((Доходы - Затраты) / Доходы) * 100';














