-- ============================================================================
-- calculate_overage - Функция расчета стоимости превышения
-- Назначение: PostgreSQL версия Oracle функции CALCULATE_OVERAGE
-- База данных: PostgreSQL (testing)
-- ============================================================================

-- Удаление если существует
DROP FUNCTION IF EXISTS calculate_overage(VARCHAR, NUMERIC) CASCADE;

CREATE OR REPLACE FUNCTION calculate_overage(
    p_plan_name VARCHAR,
    p_usage_bytes NUMERIC
) RETURNS NUMERIC AS $$
DECLARE
    v_usage_kb NUMERIC;
    v_included_kb NUMERIC;
    v_tier1_from NUMERIC;
    v_tier1_to NUMERIC;
    v_tier1_price NUMERIC;
    v_tier2_from NUMERIC;
    v_tier2_to NUMERIC;
    v_tier2_price NUMERIC;
    v_tier3_from NUMERIC;
    v_tier3_price NUMERIC;
    v_overage_kb NUMERIC := 0;
    v_tier1_kb NUMERIC := 0;
    v_tier2_kb NUMERIC := 0;
    v_tier3_kb NUMERIC := 0;
    v_total_charge NUMERIC := 0;
BEGIN
    -- Конвертируем байты в килобайты (1KB = 1000 bytes)
    v_usage_kb := p_usage_bytes / 1000;
    
    -- Получаем параметры тарифного плана
    BEGIN
        SELECT 
            included_kb,
            tier1_from_kb, tier1_to_kb, tier1_price_usd,
            tier2_from_kb, tier2_to_kb, tier2_price_usd,
            tier3_from_kb, tier3_price_usd
        INTO
            v_included_kb,
            v_tier1_from, v_tier1_to, v_tier1_price,
            v_tier2_from, v_tier2_to, v_tier2_price,
            v_tier3_from, v_tier3_price
        FROM tariff_plans
        WHERE plan_name = p_plan_name
          AND active = TRUE;
          
        IF NOT FOUND THEN
            RETURN 0;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
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
END;
$$ LANGUAGE plpgsql;

-- Комментарии
COMMENT ON FUNCTION calculate_overage(VARCHAR, NUMERIC) IS 'Расчет стоимости превышения трафика по тарифному плану';

-- Тестирование функции
\echo ''
\echo '========================================'
\echo 'Тестирование функции calculate_overage'
\echo '========================================'

SELECT 
    'SBD-1, 30 KB' AS test_case,
    calculate_overage('SBD Tiered 1250 1K', 30000) AS overage_charge,
    27.25 AS expected
UNION ALL
SELECT 
    'SBD-10, 35 KB',
    calculate_overage('SBD Tiered 1250 10K', 35000),
    6.50
UNION ALL
SELECT 
    'SBD-10, 6 KB (within included)',
    calculate_overage('SBD Tiered 1250 10K', 6000),
    0.00;

\echo ''
\echo 'Функция calculate_overage успешно создана!'


