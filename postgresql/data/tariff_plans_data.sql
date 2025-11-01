-- ============================================================================
-- Заполнение таблицы TARIFF_PLANS
-- Тарифные планы для SBD услуг Iridium
-- База данных: PostgreSQL (testing)
-- ============================================================================

-- Очистка существующих данных
TRUNCATE TABLE TARIFF_PLANS RESTART IDENTITY CASCADE;

-- Тариф SBD-1 (1 КБ включено)
-- Превышение: 1-10 КБ: $1.50/КБ, 10-25 КБ: $0.75/КБ, свыше 25 КБ: $0.50/КБ
INSERT INTO TARIFF_PLANS (
    PLAN_NAME, PLAN_CODE, INCLUDED_KB,
    TIER1_FROM_KB, TIER1_TO_KB, TIER1_PRICE_USD,
    TIER2_FROM_KB, TIER2_TO_KB, TIER2_PRICE_USD,
    TIER3_FROM_KB, TIER3_PRICE_USD,
    COMMENTS
) VALUES (
    'SBD Tiered 1250 1K', 'SBD-1', 1,
    1, 10, 1.50,
    10, 25, 0.75,
    25, 0.50,
    'Тариф SBD-1: 1 КБ включено, ступенчатая тарификация превышения'
);

-- Тариф SBD-1 (вариант "SBD Tiered 350 1K" - используется в реальных данных)
-- Превышение: 1-10 КБ: $1.50/КБ, 10-25 КБ: $0.75/КБ, свыше 25 КБ: $0.50/КБ
INSERT INTO TARIFF_PLANS (
    PLAN_NAME, PLAN_CODE, INCLUDED_KB,
    TIER1_FROM_KB, TIER1_TO_KB, TIER1_PRICE_USD,
    TIER2_FROM_KB, TIER2_TO_KB, TIER2_PRICE_USD,
    TIER3_FROM_KB, TIER3_PRICE_USD,
    COMMENTS
) VALUES (
    'SBD Tiered 350 1K', 'SBD-1', 1,
    1, 10, 1.50,
    10, 25, 0.75,
    25, 0.50,
    'Тариф SBD-1 (350): 1 КБ включено, ступенчатая тарификация превышения'
);

-- Тариф SBD-10 (10 КБ включено)
-- Превышение: 10-25 КБ: $0.30/КБ, 25-50 КБ: $0.20/КБ, свыше 50 КБ: $0.10/КБ
INSERT INTO TARIFF_PLANS (
    PLAN_NAME, PLAN_CODE, INCLUDED_KB,
    TIER1_FROM_KB, TIER1_TO_KB, TIER1_PRICE_USD,
    TIER2_FROM_KB, TIER2_TO_KB, TIER2_PRICE_USD,
    TIER3_FROM_KB, TIER3_PRICE_USD,
    COMMENTS
) VALUES (
    'SBD Tiered 1250 10K', 'SBD-10', 10,
    10, 25, 0.30,
    25, 50, 0.20,
    50, 0.10,
    'Тариф SBD-10: 10 КБ включено, ступенчатая тарификация превышения'
);

\echo ''
\echo '========================================'
\echo 'Загруженные тарифные планы'
\echo '========================================'

SELECT 
    PLAN_CODE,
    PLAN_NAME,
    INCLUDED_KB AS "Included KB",
    TIER1_FROM_KB || '-' || TIER1_TO_KB || ' KB: $' || TIER1_PRICE_USD AS "Tier 1",
    TIER2_FROM_KB || '-' || TIER2_TO_KB || ' KB: $' || TIER2_PRICE_USD AS "Tier 2",
    TIER3_FROM_KB || '+ KB: $' || TIER3_PRICE_USD AS "Tier 3",
    ACTIVE
FROM TARIFF_PLANS
WHERE ACTIVE = TRUE
ORDER BY INCLUDED_KB;

\echo ''
\echo 'Данные тарифных планов успешно загружены!'
\echo ''
\echo '========================================'
\echo 'Примеры расчета превышения'
\echo '========================================'
\echo ''
\echo 'Пример 1: SBD-1, использовано 30 КБ'
\echo 'Включено: 1 КБ (бесплатно)'
\echo '1-10 КБ: 9 КБ × $1.50 = $13.50'
\echo '10-25 КБ: 15 КБ × $0.75 = $11.25'
\echo '25+ КБ: 5 КБ × $0.50 = $2.50'
\echo 'ИТОГО: $27.25'
\echo ''
\echo 'Пример 2: SBD-10, использовано 35 КБ'
\echo 'Включено: 10 КБ (бесплатно)'
\echo '10-25 КБ: 15 КБ × $0.30 = $4.50'
\echo '25-50 КБ: 10 КБ × $0.20 = $2.00'
\echo 'ИТОГО: $6.50'
\echo ''





