-- ============================================================================
-- V_CONSOLIDATED_REPORT_WITH_BILLING
-- Расширенный отчет с данными из биллинга (название клиента, договор, код 1С)
-- База данных: Oracle (billing7@bm7)
-- ============================================================================

CREATE OR REPLACE VIEW V_CONSOLIDATED_REPORT_WITH_BILLING AS
SELECT 
    -- Все поля из основного отчета
    cor.BILL_MONTH,
    cor.IMEI,
    cor.CONTRACT_ID,
    cor.PLAN_NAME,
    cor.ACTIVATION_DATE,
    cor.SPNET_TOTAL_AMOUNT,
    cor.INCLUDED_KB,
    cor.TOTAL_USAGE_KB,
    cor.OVERAGE_KB,
    cor.CALCULATED_OVERAGE,
    cor.STECCOM_TOTAL_AMOUNT,
    -- Добавляем данные из биллинга
    v.SERVICE_ID,
    v.CODE_1C,
    v.ORGANIZATION_NAME,
    v.CUSTOMER_NAME,
    v.AGREEMENT_NUMBER,
    v.ORDER_NUMBER,
    v.STATUS AS SERVICE_STATUS,
    v.CUSTOMER_ID,
    v.ACCOUNT_ID
FROM V_CONSOLIDATED_OVERAGE_REPORT cor
LEFT JOIN V_IRIDIUM_SERVICES_INFO v 
    ON cor.CONTRACT_ID = v.CONTRACT_ID;

-- Комментарии
COMMENT ON TABLE V_CONSOLIDATED_REPORT_WITH_BILLING IS 'Консолидированный отчет по Iridium с данными клиентов из биллинга';
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.SERVICE_ID IS 'ID сервиса из биллинга';
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.CODE_1C IS 'Код клиента из 1С';
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.ORGANIZATION_NAME IS 'Название организации (для юр.лиц)';
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.CUSTOMER_NAME IS 'Название организации или ФИО клиента (универсальное поле)';
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.AGREEMENT_NUMBER IS 'Номер договора в СТЭККОМ';
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.ORDER_NUMBER IS 'Номер заказа/приложения';


