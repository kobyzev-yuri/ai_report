-- ============================================================================
-- CALCULATE_OVERAGE
-- PL/SQL функция для расчета стоимости превышения по тарифному плану
-- База данных: Oracle
-- ============================================================================

CREATE OR REPLACE FUNCTION CALCULATE_OVERAGE(
    p_plan_name VARCHAR2,
    p_usage_bytes NUMBER
) RETURN NUMBER
IS
    v_usage_kb NUMBER;
    v_included_kb NUMBER;
    v_tier1_from NUMBER;
    v_tier1_to NUMBER;
    v_tier1_price NUMBER;
    v_tier2_from NUMBER;
    v_tier2_to NUMBER;
    v_tier2_price NUMBER;
    v_tier3_from NUMBER;
    v_tier3_price NUMBER;
    
    v_overage_kb NUMBER := 0;
    v_tier1_kb NUMBER := 0;
    v_tier2_kb NUMBER := 0;
    v_tier3_kb NUMBER := 0;
    v_total_charge NUMBER := 0;
BEGIN
    -- Конвертируем байты в килобайты (1KB = 1000 bytes)
    v_usage_kb := p_usage_bytes / 1000;
    
    -- Получаем параметры тарифного плана
    BEGIN
        SELECT 
            INCLUDED_KB,
            TIER1_FROM_KB, TIER1_TO_KB, TIER1_PRICE_USD,
            TIER2_FROM_KB, TIER2_TO_KB, TIER2_PRICE_USD,
            TIER3_FROM_KB, TIER3_PRICE_USD
        INTO
            v_included_kb,
            v_tier1_from, v_tier1_to, v_tier1_price,
            v_tier2_from, v_tier2_to, v_tier2_price,
            v_tier3_from, v_tier3_price
        FROM TARIFF_PLANS
        WHERE PLAN_NAME = p_plan_name
          AND ACTIVE = 'Y';
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            -- Тариф не найден, возвращаем 0
            RETURN 0;
    END;
    
    -- Если использование в пределах включенного объема
    IF v_usage_kb <= v_included_kb THEN
        RETURN 0;
    END IF;
    
    -- Рассчитываем превышение
    v_overage_kb := v_usage_kb - v_included_kb;
    
    -- Tier 1
    IF v_overage_kb > 0 THEN
        IF v_usage_kb > v_tier1_to THEN
            v_tier1_kb := v_tier1_to - v_tier1_from;
        ELSE
            v_tier1_kb := v_usage_kb - v_tier1_from;
        END IF;
        v_total_charge := v_total_charge + (v_tier1_kb * v_tier1_price);
    END IF;
    
    -- Tier 2
    IF v_usage_kb > v_tier2_from THEN
        IF v_usage_kb > v_tier2_to THEN
            v_tier2_kb := v_tier2_to - v_tier2_from;
        ELSE
            v_tier2_kb := v_usage_kb - v_tier2_from;
        END IF;
        v_total_charge := v_total_charge + (v_tier2_kb * v_tier2_price);
    END IF;
    
    -- Tier 3
    IF v_usage_kb > v_tier3_from THEN
        v_tier3_kb := v_usage_kb - v_tier3_from;
        v_total_charge := v_total_charge + (v_tier3_kb * v_tier3_price);
    END IF;
    
    RETURN ROUND(v_total_charge, 2);
    
END CALCULATE_OVERAGE;
/

-- Тестирование функции
PROMPT
PROMPT ========================================
PROMPT Тестирование функции CALCULATE_OVERAGE
PROMPT ========================================

SELECT 
    'SBD-1, 30 KB' AS test_case,
    CALCULATE_OVERAGE('SBD Tiered 1250 1K', 30000) AS overage_charge,
    27.25 AS expected
FROM DUAL
UNION ALL
SELECT 
    'SBD-10, 35 KB',
    CALCULATE_OVERAGE('SBD Tiered 1250 10K', 35000),
    6.50
FROM DUAL
UNION ALL
SELECT 
    'SBD-10, 6 KB (within included)',
    CALCULATE_OVERAGE('SBD Tiered 1250 10K', 6000),
    0.00
FROM DUAL;

PROMPT
PROMPT Функция CALCULATE_OVERAGE успешно создана!


