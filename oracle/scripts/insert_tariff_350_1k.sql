-- ============================================================================
-- INSERT TARIFF: SBD Tiered 350 1K
-- Добавление тарифного плана "SBD Tiered 350 1K" в таблицу TARIFF_PLANS
-- ============================================================================

-- Проверяем, существует ли уже этот план
SELECT 
    PLAN_NAME,
    PLAN_CODE,
    INCLUDED_KB,
    ACTIVE
FROM TARIFF_PLANS
WHERE PLAN_NAME = 'SBD Tiered 350 1K';

-- Если план не существует, добавляем его
-- Параметры такие же, как у "SBD Tiered 1250 1K": 1 KB включено, ступенчатая тарификация
MERGE INTO TARIFF_PLANS tp
USING (
    SELECT 
        'SBD Tiered 350 1K' AS PLAN_NAME,
        'SBD-1' AS PLAN_CODE,
        1 AS INCLUDED_KB,
        1 AS TIER1_FROM_KB,
        10 AS TIER1_TO_KB,
        1.50 AS TIER1_PRICE_USD,
        10 AS TIER2_FROM_KB,
        25 AS TIER2_TO_KB,
        0.75 AS TIER2_PRICE_USD,
        25 AS TIER3_FROM_KB,
        0.50 AS TIER3_PRICE_USD,
        'Y' AS ACTIVE,
        'Тариф SBD-1: 1 КБ включено, ступенчатая тарификация превышения (базовый тариф 350)' AS COMMENTS
    FROM DUAL
) src
ON (tp.PLAN_NAME = src.PLAN_NAME)
WHEN NOT MATCHED THEN
    INSERT (
        PLAN_NAME,
        PLAN_CODE,
        INCLUDED_KB,
        TIER1_FROM_KB,
        TIER1_TO_KB,
        TIER1_PRICE_USD,
        TIER2_FROM_KB,
        TIER2_TO_KB,
        TIER2_PRICE_USD,
        TIER3_FROM_KB,
        TIER3_PRICE_USD,
        ACTIVE,
        COMMENTS
    ) VALUES (
        src.PLAN_NAME,
        src.PLAN_CODE,
        src.INCLUDED_KB,
        src.TIER1_FROM_KB,
        src.TIER1_TO_KB,
        src.TIER1_PRICE_USD,
        src.TIER2_FROM_KB,
        src.TIER2_TO_KB,
        src.TIER2_PRICE_USD,
        src.TIER3_FROM_KB,
        src.TIER3_PRICE_USD,
        src.ACTIVE,
        src.COMMENTS
    );

-- Проверяем результат
SELECT 
    PLAN_NAME,
    PLAN_CODE,
    INCLUDED_KB,
    TIER1_FROM_KB,
    TIER1_TO_KB,
    TIER1_PRICE_USD,
    TIER2_FROM_KB,
    TIER2_TO_KB,
    TIER2_PRICE_USD,
    TIER3_FROM_KB,
    TIER3_PRICE_USD,
    ACTIVE,
    COMMENTS
FROM TARIFF_PLANS
WHERE PLAN_NAME = 'SBD Tiered 350 1K';

PROMPT
PROMPT Тарифный план 'SBD Tiered 350 1K' добавлен в TARIFF_PLANS!
PROMPT

