-- ============================================================================
-- V_CONSOLIDATED_REPORT_WITH_BILLING
-- Расширенный отчет с данными из биллинга (название клиента, договор, код 1С)
-- База данных: Oracle (billing7@bm7)
-- ============================================================================

CREATE OR REPLACE VIEW V_CONSOLIDATED_REPORT_WITH_BILLING AS
SELECT 
    -- Все поля из основного отчета (по каждому периоду отдельно!)
    cor.BILL_MONTH,
    cor.IMEI,
    cor.CONTRACT_ID,
    cor.PLAN_NAME,
    -- Разделение трафика и событий (по каждому периоду)
    cor.TRAFFIC_USAGE_BYTES,
    cor.EVENTS_COUNT,
    cor.DATA_USAGE_EVENTS,
    cor.MAILBOX_EVENTS,
    cor.REGISTRATION_EVENTS,
    -- Суммы и превышения
    cor.SPNET_TOTAL_AMOUNT,
    cor.INCLUDED_KB,
    cor.TOTAL_USAGE_KB,
    cor.OVERAGE_KB,
    cor.CALCULATED_OVERAGE,
    cor.STECCOM_MONTHLY_AMOUNT,
    cor.STECCOM_SUSPENDED_AMOUNT,
    cor.STECCOM_TOTAL_AMOUNT,
    -- Две отдельные колонки для планов: основной и suspended
    cor.STECCOM_PLAN_NAME_MONTHLY,
    cor.STECCOM_PLAN_NAME_SUSPENDED,
    -- Добавляем данные из биллинга
    v.SERVICE_ID,
    v.CODE_1C,
    v.ORGANIZATION_NAME,
    v.CUSTOMER_NAME,
    v.AGREEMENT_NUMBER,
    v.ORDER_NUMBER,
    v.STATUS AS SERVICE_STATUS,
    v.CUSTOMER_ID,
    v.ACCOUNT_ID,
    v.TARIFF_ID,
    -- Доп. поля: IMEI из биллинга (VSAT/IMEI) и номер сервиса при совпадении IMEI
    v.IMEI AS IMEI_VSAT,
    CASE 
      WHEN v.CONTRACT_ID LIKE 'SUB_%' AND v.IMEI IS NOT NULL AND cor.IMEI IS NOT NULL AND v.IMEI = cor.IMEI 
      THEN v.SERVICE_ID 
      ELSE NULL 
    END AS SERVICE_ID_VSAT_MATCH
FROM V_CONSOLIDATED_OVERAGE_REPORT cor
LEFT JOIN V_IRIDIUM_SERVICES_INFO v 
    ON cor.CONTRACT_ID = v.CONTRACT_ID;

-- Комментарии
COMMENT ON TABLE V_CONSOLIDATED_REPORT_WITH_BILLING IS 'Консолидированный отчет по Iridium с данными клиентов из биллинга. КАЖДАЯ СТРОКА = ОТДЕЛЬНЫЙ ПЕРИОД (BILL_MONTH)';
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.SERVICE_ID IS 'ID сервиса из биллинга';
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.CODE_1C IS 'Код клиента из 1С';
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.ORGANIZATION_NAME IS 'Название организации (для юр.лиц)';
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.CUSTOMER_NAME IS 'Название организации или ФИО клиента (универсальное поле)';
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.AGREEMENT_NUMBER IS 'Номер договора в СТЭККОМ';
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.ORDER_NUMBER IS 'Номер заказа/приложения';
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.IMEI_VSAT IS 'IMEI из биллинга (VSAT/IMEI из V_IRIDIUM_SERVICES_INFO)';
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.SERVICE_ID_VSAT_MATCH IS 'SERVICE_ID если login LIKE SUB_% и IMEI (VSAT) совпадает с отчетным IMEI';
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.TRAFFIC_USAGE_BYTES IS 'Трафик в байтах (только SBD Data Usage)';
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.EVENTS_COUNT IS 'Общее количество событий';
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.DATA_USAGE_EVENTS IS 'События SBD Data Usage';
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.MAILBOX_EVENTS IS 'События SBD Mailbox Checks';
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.REGISTRATION_EVENTS IS 'События SBD Registrations';
