-- ============================================================================
-- V_CONSOLIDATED_REPORT_WITH_BILLING
-- Расширенный отчет с данными из биллинга (название клиента, договор, код 1С)
-- База данных: PostgreSQL (testing)
-- ============================================================================

DROP VIEW IF EXISTS V_CONSOLIDATED_REPORT_WITH_BILLING CASCADE;

CREATE OR REPLACE VIEW V_CONSOLIDATED_REPORT_WITH_BILLING AS
SELECT 
    -- Все поля из основного отчета (по каждому периоду отдельно!)
    cor.bill_month,
    cor.imei,
    cor.contract_id,
    cor.plan_name,
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
    cor.steccom_total_amount,
    -- Добавляем данные из биллинга (из импортированной таблицы)
    v.service_id,
    v.code_1c,
    v.organization_name,
    v.customer_name,
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
    ON cor.contract_id = v.contract_id;

-- Комментарии
COMMENT ON VIEW v_consolidated_report_with_billing IS 
'Консолидированный отчет по Iridium с данными клиентов из биллинга.
Использует данные из IRIDIUM_SERVICES_INFO (импорт из Oracle).';

COMMENT ON COLUMN v_consolidated_report_with_billing.service_id IS 'ID сервиса из биллинга';
COMMENT ON COLUMN v_consolidated_report_with_billing.code_1c IS 'Код клиента из 1С';
COMMENT ON COLUMN v_consolidated_report_with_billing.organization_name IS 'Название организации (для юр.лиц)';
COMMENT ON COLUMN v_consolidated_report_with_billing.customer_name IS 'Название организации или ФИО клиента (универсальное поле)';
COMMENT ON COLUMN v_consolidated_report_with_billing.agreement_number IS 'Номер договора в СТЭККОМ';
COMMENT ON COLUMN v_consolidated_report_with_billing.order_number IS 'Номер заказа/приложения';
COMMENT ON COLUMN v_consolidated_report_with_billing.imei_vsat IS 'IMEI из биллинга (VSAT/IMEI из IRIDIUM_SERVICES_INFO)';
COMMENT ON COLUMN v_consolidated_report_with_billing.service_id_vsat_match IS 'SERVICE_ID если login LIKE SUB_% и IMEI (VSAT) совпадает с отчетным IMEI';

\echo 'View V_CONSOLIDATED_REPORT_WITH_BILLING created successfully!'
\echo ''
\echo 'Now includes: SERVICE_ID, CODE_1C, ORGANIZATION_NAME, CUSTOMER_NAME'
\echo ''



