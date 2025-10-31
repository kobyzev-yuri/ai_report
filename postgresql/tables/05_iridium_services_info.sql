-- ============================================================================
-- IRIDIUM_SERVICES_INFO - Service and customer information table
-- Imported from Oracle V_IRIDIUM_SERVICES_INFO view for testing
-- База данных: PostgreSQL (testing)
-- ============================================================================

-- Удаление если существует
DROP TABLE IF EXISTS IRIDIUM_SERVICES_INFO CASCADE;

CREATE TABLE IRIDIUM_SERVICES_INFO (
    SERVICE_ID INTEGER,
    CONTRACT_ID VARCHAR(50),
    IMEI VARCHAR(50),
    TARIFF_ID INTEGER,
    AGREEMENT_NUMBER VARCHAR(200),
    ORDER_NUMBER VARCHAR(100),
    STATUS INTEGER,
    ACTUAL_STATUS INTEGER,
    CUSTOMER_ID INTEGER,
    ORGANIZATION_NAME VARCHAR(500),
    PERSON_NAME VARCHAR(500),
    CUSTOMER_NAME VARCHAR(500),
    CREATE_DATE TIMESTAMP,
    START_DATE TIMESTAMP,
    STOP_DATE TIMESTAMP,
    ACCOUNT_ID INTEGER,
    CODE_1C VARCHAR(100)
);

-- Индексы для быстрого поиска
CREATE INDEX idx_iridium_service_id ON IRIDIUM_SERVICES_INFO(SERVICE_ID);
CREATE INDEX idx_iridium_contract_id ON IRIDIUM_SERVICES_INFO(CONTRACT_ID);
CREATE INDEX idx_iridium_imei ON IRIDIUM_SERVICES_INFO(IMEI);
CREATE INDEX idx_iridium_customer_id ON IRIDIUM_SERVICES_INFO(CUSTOMER_ID);
CREATE INDEX idx_iridium_status ON IRIDIUM_SERVICES_INFO(STATUS);
CREATE INDEX idx_iridium_code_1c ON IRIDIUM_SERVICES_INFO(CODE_1C);

-- Комментарии
COMMENT ON TABLE IRIDIUM_SERVICES_INFO IS 'Информация о Iridium сервисах с данными клиентов (импорт из Oracle для тестирования)';
COMMENT ON COLUMN IRIDIUM_SERVICES_INFO.SERVICE_ID IS 'ID сервиса из billing';
COMMENT ON COLUMN IRIDIUM_SERVICES_INFO.CONTRACT_ID IS 'ID контракта (LOGIN) - связь с SPNET_TRAFFIC';
COMMENT ON COLUMN IRIDIUM_SERVICES_INFO.IMEI IS 'IMEI устройства';
COMMENT ON COLUMN IRIDIUM_SERVICES_INFO.CUSTOMER_NAME IS 'Название организации или ФИО клиента';
COMMENT ON COLUMN IRIDIUM_SERVICES_INFO.AGREEMENT_NUMBER IS 'Номер договора';
COMMENT ON COLUMN IRIDIUM_SERVICES_INFO.ORDER_NUMBER IS 'Номер заказа/приложения (бланк)';
COMMENT ON COLUMN IRIDIUM_SERVICES_INFO.CODE_1C IS 'Код клиента из 1С';
COMMENT ON COLUMN IRIDIUM_SERVICES_INFO.STATUS IS 'Статус сервиса (1=активный, 0=неактивный, -10=закрыт)';

\echo 'Table IRIDIUM_SERVICES_INFO created successfully!'



