-- ============================================================================
-- V_CONSOLIDATED_REPORT_WITH_BILLING
-- Расширенный отчет с данными из биллинга (название клиента, договор, код 1С)
-- База данных: PostgreSQL (testing)
-- ============================================================================

DROP VIEW IF EXISTS V_CONSOLIDATED_REPORT_WITH_BILLING CASCADE;

CREATE OR REPLACE VIEW V_CONSOLIDATED_REPORT_WITH_BILLING AS
WITH -- Получаем маппинг tariff_id -> plan_name из существующих данных
tariff_plan_mapping AS (
    SELECT DISTINCT ON (v.tariff_id)
        v.tariff_id,
        cor.plan_name
    FROM v_iridium_services_info v
    JOIN v_consolidated_overage_report cor ON v.contract_id = cor.contract_id
    WHERE v.tariff_id IS NOT NULL 
      AND cor.plan_name IS NOT NULL
      AND cor.plan_name != ''
    GROUP BY v.tariff_id, cor.plan_name
    ORDER BY v.tariff_id, COUNT(*) DESC, cor.plan_name
),
-- Получаем маппинг IMEI -> plan_name (для IMEI, которые используются в других контрактах с планом)
imei_plan_mapping AS (
    SELECT DISTINCT ON (imei)
        imei,
        plan_name
    FROM (
        -- Планы из v_consolidated_overage_report
        SELECT cor.imei, cor.plan_name
        FROM v_consolidated_overage_report cor
        WHERE cor.imei IS NOT NULL
          AND cor.plan_name IS NOT NULL
          AND cor.plan_name != ''
        UNION ALL
        -- Планы напрямую из SPNET_TRAFFIC (даже если запись не попала в consolidated report)
        SELECT DISTINCT st.imei, st.plan_name
        FROM spnet_traffic st
        WHERE st.imei IS NOT NULL
          AND st.plan_name IS NOT NULL
          AND st.plan_name != ''
    ) all_imei_plans
    GROUP BY imei, plan_name
    ORDER BY imei, COUNT(*) DESC, plan_name
),
-- Получаем маппинг contract_id -> plan_name из SPNET_TRAFFIC напрямую
contract_plan_mapping AS (
    SELECT DISTINCT ON (contract_id)
        contract_id,
        plan_name
    FROM spnet_traffic
    WHERE contract_id IS NOT NULL
      AND plan_name IS NOT NULL
      AND plan_name != ''
    GROUP BY contract_id, plan_name
    ORDER BY contract_id, COUNT(*) DESC, plan_name
)
SELECT 
    -- Все поля из основного отчета (по каждому периоду отдельно!)
    cor.bill_month,
    cor.imei,
    cor.contract_id,
    -- Используем plan_name из cor, если пустой - берем из маппинга по contract_id из SPNET_TRAFFIC,
    -- если пустой - из маппинга по tariff_id, если пустой - из маппинга по IMEI
    COALESCE(cor.plan_name, cpm.plan_name, tpm.plan_name, ipm.plan_name) AS plan_name,
    -- Разделение трафика и событий (по каждому периоду)
    cor.traffic_usage_bytes,
    cor.events_count,
    cor.data_usage_events,
    cor.mailbox_events,
    cor.registration_events,
    -- Суммы и превышения
    cor.spnet_total_amount,
    cor.included_kb,
    cor.total_usage_kb,
    cor.overage_kb,
    cor.calculated_overage,
    cor.steccom_monthly_amount,
    cor.steccom_suspended_amount,
    cor.steccom_total_amount,
    -- Две отдельные колонки для планов: основной и suspended
    cor.steccom_plan_name_monthly,
    cor.steccom_plan_name_suspended,
    -- Добавляем данные из биллинга (из импортированной таблицы)
    v.service_id,
    v.code_1c,
    v.organization_name,
    v.person_name,
    v.customer_name,
    -- Универсальное поле для отображения: организация или ФИО (используется в Streamlit)
    COALESCE(NULLIF(TRIM(v.organization_name), ''), NULLIF(TRIM(v.person_name), ''), '') AS display_name,
    v.agreement_number,
    v.order_number,
    v.status AS service_status,
    v.customer_id,
    v.account_id,
    v.tariff_id,
    -- Доп. поля: IMEI из биллинга (VSAT/IMEI) и номер сервиса при совпадении IMEI
    v.imei AS imei_vsat,
    CASE 
      WHEN v.contract_id LIKE 'SUB_%' AND v.imei IS NOT NULL AND cor.imei IS NOT NULL AND v.imei = cor.imei 
      THEN v.service_id 
      ELSE NULL 
    END AS service_id_vsat_match
FROM v_consolidated_overage_report cor
LEFT JOIN v_iridium_services_info v 
    ON cor.contract_id = v.contract_id
LEFT JOIN contract_plan_mapping cpm
    ON cor.contract_id = cpm.contract_id
LEFT JOIN tariff_plan_mapping tpm
    ON v.tariff_id = tpm.tariff_id
LEFT JOIN imei_plan_mapping ipm
    ON cor.imei = ipm.imei;

-- Комментарии
COMMENT ON VIEW v_consolidated_report_with_billing IS 
'Консолидированный отчет по Iridium с данными клиентов из биллинга.
Использует данные из IRIDIUM_SERVICES_INFO (импорт из Oracle).';

COMMENT ON COLUMN v_consolidated_report_with_billing.service_id IS 'ID сервиса из биллинга';
COMMENT ON COLUMN v_consolidated_report_with_billing.code_1c IS 'Код клиента из 1С';
COMMENT ON COLUMN v_consolidated_report_with_billing.organization_name IS 'Название организации (для юр.лиц)';
COMMENT ON COLUMN v_consolidated_report_with_billing.person_name IS 'ФИО физического лица';
COMMENT ON COLUMN v_consolidated_report_with_billing.customer_name IS 'Название организации или ФИО клиента (универсальное поле)';
COMMENT ON COLUMN v_consolidated_report_with_billing.display_name IS 'Отображаемое имя: организация или ФИО (для UI)';
COMMENT ON COLUMN v_consolidated_report_with_billing.agreement_number IS 'Номер договора в СТЭККОМ';
COMMENT ON COLUMN v_consolidated_report_with_billing.order_number IS 'Номер заказа/приложения';
COMMENT ON COLUMN v_consolidated_report_with_billing.imei_vsat IS 'IMEI из биллинга (VSAT/IMEI из IRIDIUM_SERVICES_INFO)';
COMMENT ON COLUMN v_consolidated_report_with_billing.service_id_vsat_match IS 'SERVICE_ID если login LIKE SUB_% и IMEI (VSAT) совпадает с отчетным IMEI';

\echo 'View V_CONSOLIDATED_REPORT_WITH_BILLING created successfully!'
\echo ''
\echo 'Now includes: SERVICE_ID, CODE_1C, ORGANIZATION_NAME, CUSTOMER_NAME'
\echo ''



