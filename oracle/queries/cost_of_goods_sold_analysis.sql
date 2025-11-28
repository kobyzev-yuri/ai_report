-- ============================================================================
-- Анализ себестоимости по SUB- (CONTRACT_ID)
-- Показывает все SUB-XXXXX с доходами и затратами Иридиум
-- Включает тестовые услуги и выявляет расхождения
-- ============================================================================
-- ВАЖНО:
-- - Финансистов волнуют ВСЕ SUB-XXXXX, даже тестовые
-- - Иридиум не всегда выставляет счета, особенно по тестовым услугам
-- - Есть SUB- которых может не быть в счетах Иридиум (тесты, еще не активированы)
-- - Есть SUB- в счетах Иридиум, но нет в доходах (не попали в счета-фактуры)
-- ============================================================================

SET LINESIZE 250
SET PAGESIZE 100

PROMPT ============================================================================
PROMPT АНАЛИЗ СЕБЕСТОИМОСТИ ПО SUB- (CONTRACT_ID)
PROMPT ============================================================================
PROMPT Показывает:
PROMPT   - Все SUB-XXXXX из SERVICES (включая тестовые)
PROMPT   - Доходы из ANALYTICS (все начисления) и BM_INVOICE_ITEM (только в счетах)
PROMPT   - Затраты Иридиум из STECCOM_EXPENSES и SPNET_TRAFFIC
PROMPT   - Флаги наличия данных и проблемные случаи
PROMPT ============================================================================
PROMPT

-- Все SBD услуги с SUB-XXXXX
WITH all_services AS (
    SELECT DISTINCT
        s.SERVICE_ID,
        s.LOGIN AS CONTRACT_ID,
        s.VSAT AS IMEI,
        s.STATUS,
        s.CREATE_DATE,
        s.START_DATE,
        s.STOP_DATE,
        s.TYPE_ID,
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
-- Затраты Иридиум из STECCOM_EXPENSES
costs_steccom AS (
    SELECT 
        CONTRACT_ID,
        TO_NUMBER(TO_CHAR(INVOICE_DATE, 'YYYYMM')) AS BILL_MONTH,
        SUM(AMOUNT) AS COST_STECCOM,
        COUNT(*) AS STECCOM_RECORDS_COUNT,
        -- Типы fees
        SUM(CASE WHEN UPPER(TRIM(DESCRIPTION)) LIKE '%ACTIVATION%' THEN AMOUNT ELSE 0 END) AS FEE_ACTIVATION,
        SUM(CASE WHEN UPPER(TRIM(DESCRIPTION)) LIKE '%ADVANCE%' THEN AMOUNT ELSE 0 END) AS FEE_ADVANCE,
        SUM(CASE WHEN UPPER(TRIM(DESCRIPTION)) LIKE '%CREDIT%' THEN AMOUNT ELSE 0 END) AS FEE_CREDIT,
        -- Планы
        MAX(CASE WHEN RATE_TYPE IS NULL OR UPPER(RATE_TYPE) NOT LIKE '%SUSPEND%' THEN PLAN_DISCOUNT END) AS PLAN_MONTHLY,
        MAX(CASE WHEN UPPER(RATE_TYPE) LIKE '%SUSPEND%' THEN PLAN_DISCOUNT END) AS PLAN_SUSPENDED
    FROM STECCOM_EXPENSES
    WHERE (SERVICE IS NULL OR UPPER(TRIM(SERVICE)) != 'BROADBAND')
      AND CONTRACT_ID LIKE 'SUB-%'
    GROUP BY CONTRACT_ID, TO_NUMBER(TO_CHAR(INVOICE_DATE, 'YYYYMM'))
),
-- Затраты из SPNET_TRAFFIC (overage)
costs_spnet AS (
    SELECT 
        CONTRACT_ID,
        BILL_MONTH,
        SUM(TOTAL_AMOUNT) AS COST_SPNET,
        SUM(CASE WHEN USAGE_TYPE = 'SBD Data Usage' THEN USAGE_BYTES ELSE 0 END) AS TRAFFIC_BYTES
    FROM SPNET_TRAFFIC
    WHERE CONTRACT_ID LIKE 'SUB-%'
      AND USAGE_TYPE = 'SBD Data Usage'
    GROUP BY CONTRACT_ID, BILL_MONTH
),
-- Маппинг PERIOD_ID на BILL_MONTH через BM_PERIODS
periods_mapping AS (
    SELECT 
        PERIOD_ID,
        TO_NUMBER(TO_CHAR(DATE_START, 'YYYYMM')) AS BILL_MONTH_START,
        TO_NUMBER(TO_CHAR(DATE_END, 'YYYYMM')) AS BILL_MONTH_END,
        NAME AS PERIOD_NAME,
        DATE_START,
        DATE_END
    FROM BM_PERIODS
)
-- Основной запрос: все SUB- с доходами и затратами
SELECT 
    -- Идентификация услуги
    svc.CONTRACT_ID,
    svc.IMEI,
    svc.SERVICE_STATUS_TYPE,
    svc.STATUS AS SERVICE_STATUS,
    svc.CREATE_DATE,
    svc.START_DATE,
    svc.STOP_DATE,
    
    -- Период
    NVL(ra.PERIOD_ID, ri.PERIOD_ID) AS PERIOD_ID,
    pm.PERIOD_NAME,
    NVL(cs.BILL_MONTH, csp.BILL_MONTH) AS BILL_MONTH,
    
    -- Доходы
    NVL(ra.REVENUE_ANALYTICS, 0) AS REVENUE_ANALYTICS,
    NVL(ri.REVENUE_INVOICES, 0) AS REVENUE_INVOICES,
    NVL(ra.ANALYTICS_COUNT, 0) AS ANALYTICS_COUNT,
    NVL(ra.INVOICE_ITEMS_COUNT, 0) AS INVOICE_ITEMS_COUNT,
    NVL(ra.TEST_ITEMS_COUNT, 0) AS TEST_ITEMS_COUNT,
    
    -- Затраты Иридиум
    NVL(cs.COST_STECCOM, 0) AS COST_STECCOM,
    NVL(csp.COST_SPNET, 0) AS COST_SPNET,
    NVL(cs.COST_STECCOM, 0) + NVL(csp.COST_SPNET, 0) AS TOTAL_COST_IRIDIUM,
    NVL(cs.STECCOM_RECORDS_COUNT, 0) AS STECCOM_RECORDS_COUNT,
    
    -- Детализация fees
    NVL(cs.FEE_ACTIVATION, 0) AS FEE_ACTIVATION,
    NVL(cs.FEE_ADVANCE, 0) AS FEE_ADVANCE,
    NVL(cs.FEE_CREDIT, 0) AS FEE_CREDIT,
    cs.PLAN_MONTHLY,
    cs.PLAN_SUSPENDED,
    
    -- Трафик
    NVL(csp.TRAFFIC_BYTES, 0) AS TRAFFIC_BYTES,
    ROUND(NVL(csp.TRAFFIC_BYTES, 0) / 1000.0, 2) AS TRAFFIC_KB,
    
    -- Флаги наличия данных
    CASE WHEN ra.CONTRACT_ID IS NOT NULL THEN 'Y' ELSE 'N' END AS HAS_REVENUE_ANALYTICS,
    CASE WHEN ri.CONTRACT_ID IS NOT NULL THEN 'Y' ELSE 'N' END AS HAS_REVENUE_INVOICES,
    CASE WHEN cs.CONTRACT_ID IS NOT NULL THEN 'Y' ELSE 'N' END AS HAS_COST_STECCOM,
    CASE WHEN csp.CONTRACT_ID IS NOT NULL THEN 'Y' ELSE 'N' END AS HAS_COST_SPNET,
    
    -- Проблемные случаи
    CASE 
        WHEN ra.CONTRACT_ID IS NOT NULL AND cs.CONTRACT_ID IS NULL AND csp.CONTRACT_ID IS NULL THEN 'REVENUE_NO_COST'
        WHEN ra.CONTRACT_ID IS NULL AND ri.CONTRACT_ID IS NULL AND (cs.CONTRACT_ID IS NOT NULL OR csp.CONTRACT_ID IS NOT NULL) THEN 'COST_NO_REVENUE'
        WHEN ra.TEST_ITEMS_COUNT > 0 THEN 'HAS_TEST_ITEMS'
        WHEN svc.SERVICE_STATUS_TYPE = 'TEST_CLONE' THEN 'TEST_CLONE'
        ELSE 'OK'
    END AS ISSUE_FLAG,
    
    -- Себестоимость (если есть и доходы и затраты)
    CASE 
        WHEN NVL(ra.REVENUE_ANALYTICS, 0) > 0 AND (NVL(cs.COST_STECCOM, 0) + NVL(csp.COST_SPNET, 0)) > 0 
        THEN ROUND((NVL(cs.COST_STECCOM, 0) + NVL(csp.COST_SPNET, 0)) / ra.REVENUE_ANALYTICS * 100, 2)
        ELSE NULL
    END AS COST_PERCENTAGE
    
FROM all_services svc
LEFT JOIN revenues_analytics ra ON svc.CONTRACT_ID = ra.CONTRACT_ID
LEFT JOIN revenues_invoices ri ON svc.CONTRACT_ID = ri.CONTRACT_ID 
    AND NVL(ra.PERIOD_ID, -1) = NVL(ri.PERIOD_ID, -1)
LEFT JOIN periods_mapping pm ON NVL(ra.PERIOD_ID, ri.PERIOD_ID) = pm.PERIOD_ID
LEFT JOIN costs_steccom cs ON svc.CONTRACT_ID = cs.CONTRACT_ID
    AND (
        pm.BILL_MONTH_START IS NULL 
        OR (cs.BILL_MONTH >= pm.BILL_MONTH_START AND cs.BILL_MONTH <= pm.BILL_MONTH_END)
    )
LEFT JOIN costs_spnet csp ON svc.CONTRACT_ID = csp.CONTRACT_ID
    AND (
        pm.BILL_MONTH_START IS NULL 
        OR (csp.BILL_MONTH >= pm.BILL_MONTH_START AND csp.BILL_MONTH <= pm.BILL_MONTH_END)
    )
ORDER BY 
    svc.CONTRACT_ID,
    NVL(ra.PERIOD_ID, ri.PERIOD_ID) NULLS LAST,
    NVL(cs.BILL_MONTH, csp.BILL_MONTH) NULLS LAST;

PROMPT
PROMPT ============================================================================
PROMPT СВОДКА ПО ПРОБЛЕМНЫМ СЛУЧАЯМ
PROMPT ============================================================================

-- SUB- с доходами, но без затрат Иридиум
WITH revenues_analytics AS (
    SELECT DISTINCT
        s.LOGIN AS CONTRACT_ID,
        a.PERIOD_ID
    FROM ANALYTICS a
    JOIN SERVICES s ON a.SERVICE_ID = s.SERVICE_ID
    WHERE s.TYPE_ID = 9002
      AND s.LOGIN LIKE 'SUB-%'
),
costs_steccom AS (
    SELECT DISTINCT CONTRACT_ID
    FROM STECCOM_EXPENSES
    WHERE CONTRACT_ID LIKE 'SUB-%'
),
costs_spnet AS (
    SELECT DISTINCT CONTRACT_ID
    FROM SPNET_TRAFFIC
    WHERE CONTRACT_ID LIKE 'SUB-%'
)
SELECT 
    'SUB- с доходами, но без затрат Иридиум' AS ISSUE_TYPE,
    COUNT(DISTINCT ra.CONTRACT_ID) AS CONTRACT_COUNT
FROM revenues_analytics ra
WHERE NOT EXISTS (
    SELECT 1 FROM costs_steccom cs WHERE cs.CONTRACT_ID = ra.CONTRACT_ID
)
AND NOT EXISTS (
    SELECT 1 FROM costs_spnet csp WHERE csp.CONTRACT_ID = ra.CONTRACT_ID
);

-- SUB- с затратами Иридиум, но без доходов
WITH costs_steccom AS (
    SELECT DISTINCT CONTRACT_ID
    FROM STECCOM_EXPENSES
    WHERE CONTRACT_ID LIKE 'SUB-%'
),
costs_spnet AS (
    SELECT DISTINCT CONTRACT_ID
    FROM SPNET_TRAFFIC
    WHERE CONTRACT_ID LIKE 'SUB-%'
),
revenues_analytics AS (
    SELECT DISTINCT s.LOGIN AS CONTRACT_ID
    FROM ANALYTICS a
    JOIN SERVICES s ON a.SERVICE_ID = s.SERVICE_ID
    WHERE s.TYPE_ID = 9002
      AND s.LOGIN LIKE 'SUB-%'
),
revenues_invoices AS (
    SELECT DISTINCT s.LOGIN AS CONTRACT_ID
    FROM BM_INVOICE_ITEM ii
    JOIN SERVICES s ON ii.SERVICE_ID = s.SERVICE_ID
    WHERE s.TYPE_ID = 9002
      AND s.LOGIN LIKE 'SUB-%'
)
SELECT 
    'SUB- с затратами Иридиум, но без доходов' AS ISSUE_TYPE,
    COUNT(DISTINCT costs.CONTRACT_ID) AS CONTRACT_COUNT
FROM (
    SELECT CONTRACT_ID FROM costs_steccom
    UNION
    SELECT CONTRACT_ID FROM costs_spnet
) costs
WHERE NOT EXISTS (
    SELECT 1 FROM revenues_analytics ra WHERE ra.CONTRACT_ID = costs.CONTRACT_ID
)
AND NOT EXISTS (
    SELECT 1 FROM revenues_invoices ri WHERE ri.CONTRACT_ID = costs.CONTRACT_ID
);

PROMPT
PROMPT ============================================================================
PROMPT ИТОГО: Все SUB- с тестовыми начислениями
PROMPT ============================================================================

SELECT 
    COUNT(DISTINCT svc.CONTRACT_ID) AS TOTAL_SUB_COUNT,
    COUNT(DISTINCT CASE WHEN ra.CONTRACT_ID IS NOT NULL THEN svc.CONTRACT_ID END) AS SUB_WITH_REVENUE_ANALYTICS,
    COUNT(DISTINCT CASE WHEN ri.CONTRACT_ID IS NOT NULL THEN svc.CONTRACT_ID END) AS SUB_WITH_REVENUE_INVOICES,
    COUNT(DISTINCT CASE WHEN cs.CONTRACT_ID IS NOT NULL OR csp.CONTRACT_ID IS NOT NULL THEN svc.CONTRACT_ID END) AS SUB_WITH_COST,
    COUNT(DISTINCT CASE WHEN ra.CONTRACT_ID IS NOT NULL AND (cs.CONTRACT_ID IS NOT NULL OR csp.CONTRACT_ID IS NOT NULL) THEN svc.CONTRACT_ID END) AS SUB_WITH_BOTH,
    SUM(NVL(ra.REVENUE_ANALYTICS, 0)) AS TOTAL_REVENUE_ANALYTICS,
    SUM(NVL(ri.REVENUE_INVOICES, 0)) AS TOTAL_REVENUE_INVOICES,
    SUM(NVL(cs.COST_STECCOM, 0) + NVL(csp.COST_SPNET, 0)) AS TOTAL_COST_IRIDIUM
FROM (
    SELECT DISTINCT s.LOGIN AS CONTRACT_ID
    FROM SERVICES s
    WHERE s.TYPE_ID = 9002
      AND s.LOGIN LIKE 'SUB-%'
) svc
LEFT JOIN (
    SELECT CONTRACT_ID, SUM(REVENUE_ANALYTICS) AS REVENUE_ANALYTICS
    FROM (
        SELECT s.LOGIN AS CONTRACT_ID, SUM(a.MONEY) AS REVENUE_ANALYTICS
        FROM ANALYTICS a
        JOIN SERVICES s ON a.SERVICE_ID = s.SERVICE_ID
        WHERE s.TYPE_ID = 9002 AND s.LOGIN LIKE 'SUB-%'
        GROUP BY s.LOGIN
    )
    GROUP BY CONTRACT_ID
) ra ON svc.CONTRACT_ID = ra.CONTRACT_ID
LEFT JOIN (
    SELECT CONTRACT_ID, SUM(REVENUE_INVOICES) AS REVENUE_INVOICES
    FROM (
        SELECT s.LOGIN AS CONTRACT_ID, SUM(ii.MONEY - NVL(ii.MONEY_REVERSED, 0)) AS REVENUE_INVOICES
        FROM BM_INVOICE_ITEM ii
        JOIN SERVICES s ON ii.SERVICE_ID = s.SERVICE_ID
        WHERE s.TYPE_ID = 9002 AND s.LOGIN LIKE 'SUB-%'
        GROUP BY s.LOGIN
    )
    GROUP BY CONTRACT_ID
) ri ON svc.CONTRACT_ID = ri.CONTRACT_ID
LEFT JOIN (
    SELECT DISTINCT CONTRACT_ID
    FROM STECCOM_EXPENSES
    WHERE CONTRACT_ID LIKE 'SUB-%'
) cs ON svc.CONTRACT_ID = cs.CONTRACT_ID
LEFT JOIN (
    SELECT DISTINCT CONTRACT_ID
    FROM SPNET_TRAFFIC
    WHERE CONTRACT_ID LIKE 'SUB-%'
) csp ON svc.CONTRACT_ID = csp.CONTRACT_ID;
