-- ============================================================================
-- TARIFF_PLANS - Справочник тарифных планов
-- Назначение: Хранение информации о тарифных планах для тестирования
-- База данных: PostgreSQL (testing)
-- ============================================================================

-- Удаление если существует
DROP TABLE IF EXISTS TARIFF_PLANS CASCADE;

CREATE TABLE TARIFF_PLANS (
    ID SERIAL PRIMARY KEY,
    PLAN_NAME VARCHAR(100) NOT NULL UNIQUE,
    PLAN_CODE VARCHAR(20),
    INCLUDED_KB NUMERIC NOT NULL,
    
    -- Первая ступень тарификации
    TIER1_FROM_KB NUMERIC,
    TIER1_TO_KB NUMERIC,
    TIER1_PRICE_USD NUMERIC(10,4),
    
    -- Вторая ступень тарификации
    TIER2_FROM_KB NUMERIC,
    TIER2_TO_KB NUMERIC,
    TIER2_PRICE_USD NUMERIC(10,4),
    
    -- Третья ступень тарификации
    TIER3_FROM_KB NUMERIC,
    TIER3_PRICE_USD NUMERIC(10,4),
    
    ACTIVE BOOLEAN DEFAULT TRUE,
    CREATED_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CREATED_BY VARCHAR(50) DEFAULT CURRENT_USER,
    COMMENTS VARCHAR(500)
);

-- Индексы
CREATE INDEX idx_tariff_plan_name ON TARIFF_PLANS(PLAN_NAME);
CREATE INDEX idx_tariff_plan_code ON TARIFF_PLANS(PLAN_CODE);
CREATE INDEX idx_tariff_active ON TARIFF_PLANS(ACTIVE);

-- Комментарии
COMMENT ON TABLE TARIFF_PLANS IS 'Справочник тарифных планов SBD с ценами превышения (тестовая)';
COMMENT ON COLUMN TARIFF_PLANS.PLAN_NAME IS 'Полное название тарифного плана из SPNet';
COMMENT ON COLUMN TARIFF_PLANS.INCLUDED_KB IS 'Объем трафика включенный в абонентку (КБ, 1KB=1000bytes)';
COMMENT ON COLUMN TARIFF_PLANS.TIER1_PRICE_USD IS 'Цена за 1 КБ на первой ступени превышения (USD)';
COMMENT ON COLUMN TARIFF_PLANS.TIER2_PRICE_USD IS 'Цена за 1 КБ на второй ступени превышения (USD)';
COMMENT ON COLUMN TARIFF_PLANS.TIER3_PRICE_USD IS 'Цена за 1 КБ на третьей ступени превышения (USD)';

\echo 'Таблица TARIFF_PLANS создана успешно!'


