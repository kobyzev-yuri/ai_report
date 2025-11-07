-- ============================================================================
-- Диагностика отсутствующих тарифных планов
-- Проверяет все возможные источники данных для выявления причин
-- ============================================================================

-- Список проблемных записей (из пользовательского запроса)
WITH problem_records AS (
    SELECT 
        '202510' AS bill_month,
        '300215061445250' AS imei,
        'SUB-51636947303' AS contract_id
    UNION ALL SELECT '202510', '300215061168130', 'SUB-51637724351'
    UNION ALL SELECT '202510', '300215060295740', 'SUB-37896123618'
    UNION ALL SELECT '202510', '300215061247520', 'SUB-51637720091'
    UNION ALL SELECT '202510', '300215061246820', 'SUB-51637754714'
    UNION ALL SELECT '202510', '300215061049670', 'SUB-51635724320'
    UNION ALL SELECT '202510', '300215061809720', 'SUB-37694028083'
    UNION ALL SELECT '202510', '300215010050900', 'SUB-7050679980'
    UNION ALL SELECT '202510', '300215061147710', 'SUB-51638261918'
    UNION ALL SELECT '202510', '300215061804050', 'SUB-38494828046'
    UNION ALL SELECT '202510', '300215061803050', 'SUB-37694441195'
    UNION ALL SELECT '202510', '300215010051900', 'SUB-7051012633'
    UNION ALL SELECT '202510', '300215010642880', 'SUB-5654062275'
    UNION ALL SELECT '202510', '300215061242820', 'SUB-55804657342'
    UNION ALL SELECT '202510', '300215061149410', 'SUB-55805465170'
    UNION ALL SELECT '202510', '300215061041600', 'SUB-51637720395'
    UNION ALL SELECT '202510', '300215061248910', 'SUB-55803919691'
    UNION ALL SELECT '202510', '300215060061540', 'SUB-21048971980'
),
-- Проверка в SPNET_TRAFFIC
spnet_check AS (
    SELECT 
        pr.imei,
        pr.contract_id,
        pr.bill_month,
        COUNT(*) AS spnet_records_count,
        COUNT(DISTINCT st.plan_name) AS distinct_plans_count,
        STRING_AGG(DISTINCT st.plan_name, ', ') AS plan_names_in_spnet,
        COUNT(CASE WHEN st.plan_name IS NULL OR st.plan_name = '' THEN 1 END) AS null_plan_count,
        SUM(st.total_amount) AS spnet_total_amount,
        SUM(CASE WHEN st.usage_type = 'SBD Data Usage' THEN st.usage_bytes ELSE 0 END) AS traffic_bytes
    FROM problem_records pr
    LEFT JOIN spnet_traffic st 
        ON pr.imei = st.imei 
        AND pr.contract_id = st.contract_id
        AND CAST(pr.bill_month AS INTEGER) = st.bill_month
    GROUP BY pr.imei, pr.contract_id, pr.bill_month
),
-- Проверка в STECCOM_EXPENSES
steccom_check AS (
    SELECT 
        pr.imei,
        pr.contract_id,
        pr.bill_month,
        COUNT(*) AS steccom_records_count,
        COUNT(DISTINCT se.plan_discount) AS distinct_plans_count,
        STRING_AGG(DISTINCT se.plan_discount, ', ') AS plan_names_in_steccom,
        COUNT(CASE WHEN se.plan_discount IS NULL OR se.plan_discount = '' THEN 1 END) AS null_plan_count,
        SUM(se.amount) AS steccom_total_amount,
        STRING_AGG(DISTINCT se.rate_type, ', ') AS rate_types
    FROM problem_records pr
    LEFT JOIN steccom_expenses se 
        ON pr.imei = se.icc_id_imei 
        AND pr.contract_id = se.contract_id
        AND TO_CHAR(se.invoice_date, 'YYYYMM') = pr.bill_month
        AND (se.service IS NULL OR UPPER(TRIM(se.service)) != 'BROADBAND')
    GROUP BY pr.imei, pr.contract_id, pr.bill_month
),
-- Проверка в IRIDIUM_SERVICES_INFO
billing_check AS (
    SELECT 
        pr.imei,
        pr.contract_id,
        pr.bill_month,
        COUNT(*) AS billing_records_count,
        STRING_AGG(DISTINCT CAST(isi.tariff_id AS TEXT), ', ') AS tariff_ids,
        STRING_AGG(DISTINCT isi.organization_name, '; ') AS organization_names,
        STRING_AGG(DISTINCT isi.code_1c, ', ') AS codes_1c,
        STRING_AGG(DISTINCT isi.agreement_number, '; ') AS agreement_numbers
    FROM problem_records pr
    LEFT JOIN iridium_services_info isi 
        ON pr.contract_id = isi.contract_id
    GROUP BY pr.imei, pr.contract_id, pr.bill_month
),
-- Проверка планов в других периодах для этих IMEI/CONTRACT_ID
other_periods_plans AS (
    SELECT 
        pr.imei,
        pr.contract_id,
        COUNT(DISTINCT st.plan_name) AS plans_in_other_periods_count,
        STRING_AGG(DISTINCT st.plan_name, ', ') AS plan_names_other_periods,
        STRING_AGG(DISTINCT CAST(st.bill_month AS TEXT), ', ') AS periods_with_plans
    FROM problem_records pr
    LEFT JOIN spnet_traffic st 
        ON pr.imei = st.imei 
        AND pr.contract_id = st.contract_id
        AND CAST(pr.bill_month AS INTEGER) != st.bill_month
        AND st.plan_name IS NOT NULL 
        AND st.plan_name != ''
    GROUP BY pr.imei, pr.contract_id
),
-- Проверка в консолидированном отчете
consolidated_check AS (
    SELECT 
        pr.imei,
        pr.contract_id,
        pr.bill_month,
        cor.plan_name AS plan_name_in_report,
        cor.spnet_total_amount,
        cor.steccom_total_amount,
        cor.steccom_plan_name_monthly,
        cor.steccom_plan_name_suspended
    FROM problem_records pr
    LEFT JOIN v_consolidated_overage_report cor
        ON pr.imei = cor.imei
        AND pr.contract_id = cor.contract_id
        AND pr.bill_month = cor.bill_month
)
-- Итоговый отчет
SELECT 
    pr.imei,
    pr.contract_id,
    pr.bill_month,
    -- SPNet данные
    COALESCE(sc.spnet_records_count, 0) AS spnet_records,
    sc.plan_names_in_spnet AS spnet_plan_names,
    CASE 
        WHEN sc.spnet_records_count = 0 THEN 'Нет записей в SPNET_TRAFFIC'
        WHEN sc.null_plan_count > 0 THEN 'PLAN_NAME = NULL в SPNET_TRAFFIC'
        WHEN sc.plan_names_in_spnet IS NULL THEN 'PLAN_NAME отсутствует'
        ELSE 'Есть PLAN_NAME: ' || sc.plan_names_in_spnet
    END AS spnet_status,
    -- STECCOM данные
    COALESCE(stc.steccom_records_count, 0) AS steccom_records,
    stc.plan_names_in_steccom AS steccom_plan_names,
    stc.rate_types AS steccom_rate_types,
    CASE 
        WHEN stc.steccom_records_count = 0 THEN 'Нет записей в STECCOM_EXPENSES'
        WHEN stc.null_plan_count > 0 THEN 'PLAN_DISCOUNT = NULL в STECCOM_EXPENSES'
        WHEN stc.plan_names_in_steccom IS NULL THEN 'PLAN_DISCOUNT отсутствует'
        ELSE 'Есть PLAN_DISCOUNT: ' || stc.plan_names_in_steccom
    END AS steccom_status,
    -- Биллинг данные
    COALESCE(bc.billing_records_count, 0) AS billing_records,
    bc.tariff_ids,
    bc.organization_names,
    bc.codes_1c,
    bc.agreement_numbers,
    -- Планы в других периодах
    COALESCE(opp.plans_in_other_periods_count, 0) AS plans_in_other_periods,
    opp.plan_names_other_periods,
    opp.periods_with_plans,
    -- Консолидированный отчет
    cc.plan_name_in_report,
    cc.steccom_plan_name_monthly,
    cc.steccom_plan_name_suspended,
    -- Диагноз
    CASE 
        WHEN sc.spnet_records_count = 0 AND stc.steccom_records_count = 0 THEN 
            'КРИТИЧНО: Нет данных ни в SPNET_TRAFFIC, ни в STECCOM_EXPENSES'
        WHEN sc.spnet_records_count > 0 AND sc.plan_names_in_spnet IS NULL THEN 
            'ПРОБЛЕМА: Есть данные в SPNET_TRAFFIC, но PLAN_NAME = NULL'
        WHEN stc.steccom_records_count > 0 AND stc.plan_names_in_steccom IS NULL THEN 
            'ПРОБЛЕМА: Есть данные в STECCOM_EXPENSES, но PLAN_DISCOUNT = NULL'
        WHEN opp.plans_in_other_periods_count > 0 THEN 
            'РЕШЕНИЕ: Есть планы в других периодах - можно использовать маппинг'
        WHEN bc.tariff_ids IS NOT NULL THEN 
            'РЕШЕНИЕ: Есть TARIFF_ID в биллинге - можно использовать маппинг по tariff_id'
        ELSE 
            'ТРЕБУЕТСЯ РУЧНОЙ АНАЛИЗ'
    END AS diagnosis
FROM problem_records pr
LEFT JOIN spnet_check sc ON pr.imei = sc.imei AND pr.contract_id = sc.contract_id AND pr.bill_month = sc.bill_month
LEFT JOIN steccom_check stc ON pr.imei = stc.imei AND pr.contract_id = stc.contract_id AND pr.bill_month = stc.bill_month
LEFT JOIN billing_check bc ON pr.imei = bc.imei AND pr.contract_id = bc.contract_id AND pr.bill_month = bc.bill_month
LEFT JOIN other_periods_plans opp ON pr.imei = opp.imei AND pr.contract_id = opp.contract_id
LEFT JOIN consolidated_check cc ON pr.imei = cc.imei AND pr.contract_id = cc.contract_id AND pr.bill_month = cc.bill_month
ORDER BY pr.imei, pr.contract_id;

-- ============================================================================
-- Дополнительные запросы: проверка маппингов
-- Запускать отдельно после основного запроса
-- ============================================================================

-- Маппинг по contract_id из SPNET_TRAFFIC
WITH problem_records AS (
    SELECT 
        '202510' AS bill_month,
        '300215061445250' AS imei,
        'SUB-51636947303' AS contract_id
    UNION ALL SELECT '202510', '300215061168130', 'SUB-51637724351'
    UNION ALL SELECT '202510', '300215060295740', 'SUB-37896123618'
    UNION ALL SELECT '202510', '300215061247520', 'SUB-51637720091'
    UNION ALL SELECT '202510', '300215061246820', 'SUB-51637754714'
    UNION ALL SELECT '202510', '300215061049670', 'SUB-51635724320'
    UNION ALL SELECT '202510', '300215061809720', 'SUB-37694028083'
    UNION ALL SELECT '202510', '300215010050900', 'SUB-7050679980'
    UNION ALL SELECT '202510', '300215061147710', 'SUB-51638261918'
    UNION ALL SELECT '202510', '300215061804050', 'SUB-38494828046'
    UNION ALL SELECT '202510', '300215061803050', 'SUB-37694441195'
    UNION ALL SELECT '202510', '300215010051900', 'SUB-7051012633'
    UNION ALL SELECT '202510', '300215010642880', 'SUB-5654062275'
    UNION ALL SELECT '202510', '300215061242820', 'SUB-55804657342'
    UNION ALL SELECT '202510', '300215061149410', 'SUB-55805465170'
    UNION ALL SELECT '202510', '300215061041600', 'SUB-51637720395'
    UNION ALL SELECT '202510', '300215061248910', 'SUB-55803919691'
    UNION ALL SELECT '202510', '300215060061540', 'SUB-21048971980'
)
SELECT 
    'Маппинг по CONTRACT_ID из SPNET_TRAFFIC' AS mapping_type,
    contract_id,
    COUNT(DISTINCT plan_name) AS distinct_plans,
    STRING_AGG(DISTINCT plan_name, ', ') AS plan_names,
    COUNT(*) AS total_records
FROM spnet_traffic
WHERE contract_id IN (
    SELECT DISTINCT contract_id FROM problem_records
)
AND plan_name IS NOT NULL 
AND plan_name != ''
GROUP BY contract_id
ORDER BY contract_id;

-- Маппинг по IMEI из SPNET_TRAFFIC
WITH problem_records AS (
    SELECT 
        '202510' AS bill_month,
        '300215061445250' AS imei,
        'SUB-51636947303' AS contract_id
    UNION ALL SELECT '202510', '300215061168130', 'SUB-51637724351'
    UNION ALL SELECT '202510', '300215060295740', 'SUB-37896123618'
    UNION ALL SELECT '202510', '300215061247520', 'SUB-51637720091'
    UNION ALL SELECT '202510', '300215061246820', 'SUB-51637754714'
    UNION ALL SELECT '202510', '300215061049670', 'SUB-51635724320'
    UNION ALL SELECT '202510', '300215061809720', 'SUB-37694028083'
    UNION ALL SELECT '202510', '300215010050900', 'SUB-7050679980'
    UNION ALL SELECT '202510', '300215061147710', 'SUB-51638261918'
    UNION ALL SELECT '202510', '300215061804050', 'SUB-38494828046'
    UNION ALL SELECT '202510', '300215061803050', 'SUB-37694441195'
    UNION ALL SELECT '202510', '300215010051900', 'SUB-7051012633'
    UNION ALL SELECT '202510', '300215010642880', 'SUB-5654062275'
    UNION ALL SELECT '202510', '300215061242820', 'SUB-55804657342'
    UNION ALL SELECT '202510', '300215061149410', 'SUB-55805465170'
    UNION ALL SELECT '202510', '300215061041600', 'SUB-51637720395'
    UNION ALL SELECT '202510', '300215061248910', 'SUB-55803919691'
    UNION ALL SELECT '202510', '300215060061540', 'SUB-21048971980'
)
SELECT 
    'Маппинг по IMEI из SPNET_TRAFFIC' AS mapping_type,
    imei,
    COUNT(DISTINCT plan_name) AS distinct_plans,
    STRING_AGG(DISTINCT plan_name, ', ') AS plan_names,
    COUNT(*) AS total_records
FROM spnet_traffic
WHERE imei IN (
    SELECT DISTINCT imei FROM problem_records
)
AND plan_name IS NOT NULL 
AND plan_name != ''
GROUP BY imei
ORDER BY imei;

-- Маппинг по tariff_id (если есть в биллинге)
WITH problem_records AS (
    SELECT 
        '202510' AS bill_month,
        '300215061445250' AS imei,
        'SUB-51636947303' AS contract_id
    UNION ALL SELECT '202510', '300215061168130', 'SUB-51637724351'
    UNION ALL SELECT '202510', '300215060295740', 'SUB-37896123618'
    UNION ALL SELECT '202510', '300215061247520', 'SUB-51637720091'
    UNION ALL SELECT '202510', '300215061246820', 'SUB-51637754714'
    UNION ALL SELECT '202510', '300215061049670', 'SUB-51635724320'
    UNION ALL SELECT '202510', '300215061809720', 'SUB-37694028083'
    UNION ALL SELECT '202510', '300215010050900', 'SUB-7050679980'
    UNION ALL SELECT '202510', '300215061147710', 'SUB-51638261918'
    UNION ALL SELECT '202510', '300215061804050', 'SUB-38494828046'
    UNION ALL SELECT '202510', '300215061803050', 'SUB-37694441195'
    UNION ALL SELECT '202510', '300215010051900', 'SUB-7051012633'
    UNION ALL SELECT '202510', '300215010642880', 'SUB-5654062275'
    UNION ALL SELECT '202510', '300215061242820', 'SUB-55804657342'
    UNION ALL SELECT '202510', '300215061149410', 'SUB-55805465170'
    UNION ALL SELECT '202510', '300215061041600', 'SUB-51637720395'
    UNION ALL SELECT '202510', '300215061248910', 'SUB-55803919691'
    UNION ALL SELECT '202510', '300215060061540', 'SUB-21048971980'
)
SELECT 
    'Маппинг по TARIFF_ID из биллинга' AS mapping_type,
    isi.tariff_id,
    COUNT(DISTINCT cor.plan_name) AS distinct_plans,
    STRING_AGG(DISTINCT cor.plan_name, ', ') AS plan_names,
    COUNT(*) AS total_records
FROM iridium_services_info isi
JOIN v_consolidated_overage_report cor ON isi.contract_id = cor.contract_id
WHERE isi.contract_id IN (
    SELECT DISTINCT contract_id FROM problem_records
)
AND isi.tariff_id IS NOT NULL
AND cor.plan_name IS NOT NULL
AND cor.plan_name != ''
GROUP BY isi.tariff_id
ORDER BY isi.tariff_id;

