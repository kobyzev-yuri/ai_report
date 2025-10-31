-- ============================================================================
-- SPNET_TRAFFIC - Таблица трафика SPNet
-- Назначение: Хранение данных для тестирования (копия из Oracle)
-- База данных: PostgreSQL (testing)
-- ============================================================================

-- Удаление если существует
DROP TABLE IF EXISTS SPNET_TRAFFIC CASCADE;

CREATE TABLE SPNET_TRAFFIC (
    ID SERIAL PRIMARY KEY,
    TOTAL_ROWS INTEGER,
    CONTRACT_ID VARCHAR(50),
    IMEI VARCHAR(50),
    SIM_ICCID VARCHAR(50),
    SERVICE VARCHAR(100),
    USAGE_TYPE VARCHAR(100),
    USAGE_BYTES NUMERIC,
    USAGE_UNIT VARCHAR(20),
    TOTAL_AMOUNT NUMERIC(10,2),
    BILL_MONTH INTEGER,
    PLAN_NAME VARCHAR(100),
    IMSI VARCHAR(50),
    MSISDN VARCHAR(50),
    ACTUAL_USAGE NUMERIC,
    CALL_SESSION_COUNT INTEGER,
    SP_ACCOUNT_NO INTEGER,
    SP_NAME VARCHAR(100),
    SP_REFERENCE VARCHAR(100),
    SOURCE_FILE VARCHAR(200),
    LOAD_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CREATED_BY VARCHAR(50) DEFAULT CURRENT_USER
);

-- Индексы
CREATE INDEX idx_spnet_imei ON SPNET_TRAFFIC(IMEI);
CREATE INDEX idx_spnet_contract ON SPNET_TRAFFIC(CONTRACT_ID);
CREATE INDEX idx_spnet_bill_month ON SPNET_TRAFFIC(BILL_MONTH);
CREATE INDEX idx_spnet_plan_name ON SPNET_TRAFFIC(PLAN_NAME);
CREATE INDEX idx_spnet_usage_type ON SPNET_TRAFFIC(USAGE_TYPE);

-- Комментарии
COMMENT ON TABLE SPNET_TRAFFIC IS 'Таблица трафика SPNet - данные об использовании Iridium M2M (тестовая копия)';
COMMENT ON COLUMN SPNET_TRAFFIC.CONTRACT_ID IS 'ID контракта - связь с биллингом';
COMMENT ON COLUMN SPNET_TRAFFIC.IMEI IS 'IMEI номер устройства';
COMMENT ON COLUMN SPNET_TRAFFIC.USAGE_BYTES IS 'Объем использованных данных в байтах';
COMMENT ON COLUMN SPNET_TRAFFIC.USAGE_TYPE IS 'Тип использования (SBD Data Usage, Mailbox Check, Registration)';
COMMENT ON COLUMN SPNET_TRAFFIC.BILL_MONTH IS 'Месяц биллинга (формат: month*10000+year)';
COMMENT ON COLUMN SPNET_TRAFFIC.TOTAL_AMOUNT IS 'Общая сумма за использование (USD)';

\echo 'Таблица SPNET_TRAFFIC создана успешно!'


