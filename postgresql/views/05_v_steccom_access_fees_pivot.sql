-- ============================================================================
-- V_STECCOM_ACCESS_FEES_PIVOT
-- Сводная таблица категорий плат STECCOM по месяцам и контрактам
-- База данных: PostgreSQL (billing)
-- ============================================================================

-- Сначала создаем нормализованный view из steccom_expenses
DROP VIEW IF EXISTS v_steccom_access_fees_norm CASCADE;

CREATE OR REPLACE VIEW v_steccom_access_fees_norm AS
SELECT
    -- Используем дату из имени файла как период (YYYYMMDD, например 20251002)
    -- Это цикл 30 дней со 2-го по 2-е, а не календарный месяц
    CASE 
        WHEN source_file ~ '\.([0-9]{8})\.csv$' THEN
            (regexp_match(source_file, '\.([0-9]{8})\.csv$'))[1]::int
        ELSE NULL
    END AS fee_period_date,  -- YYYYMMDD из имени файла
    contract_id,
    description AS category,
    amount::numeric AS amount
FROM steccom_expenses
WHERE contract_id IS NOT NULL
  AND description IS NOT NULL
  AND amount IS NOT NULL
  AND source_file ~ '\.([0-9]{8})\.csv$';  -- Только записи с валидной датой в имени файла

-- Комментарии
COMMENT ON VIEW v_steccom_access_fees_norm IS 
'Нормализованный view категорий плат STECCOM из steccom_expenses.
fee_period_date в формате YYYYMMDD из имени файла (например, 20251002) - цикл 30 дней со 2-го по 2-е.';

-- Сводная таблица: категории -> колонки
DROP VIEW IF EXISTS v_steccom_access_fees_pivot CASCADE;

CREATE OR REPLACE VIEW v_steccom_access_fees_pivot AS
WITH aggregated AS (
    SELECT
        fee_period_date,
        contract_id,
        category,
        SUM(amount) AS total_amount
    FROM v_steccom_access_fees_norm
    GROUP BY fee_period_date, contract_id, category
)
SELECT
    fee_period_date,
    contract_id,
    -- Динамически создаем колонки для каждой категории через COALESCE
    -- Основные категории (можно расширить по мере появления новых)
    COALESCE(SUM(CASE WHEN category ILIKE '%Advance Charge%' THEN total_amount END), 0) AS fee_advance_charge,
    COALESCE(SUM(CASE WHEN category ILIKE '%Activation%' THEN total_amount END), 0) AS fee_activation,
    COALESCE(SUM(CASE WHEN category ILIKE '%Monthly%' OR category ILIKE '%Monthly Access%' THEN total_amount END), 0) AS fee_monthly,
    COALESCE(SUM(CASE WHEN category ILIKE '%Suspension%' THEN total_amount END), 0) AS fee_suspension,
    COALESCE(SUM(CASE WHEN category ILIKE '%Reactivation%' THEN total_amount END), 0) AS fee_reactivation,
    COALESCE(SUM(CASE WHEN category ILIKE '%Penalty%' THEN total_amount END), 0) AS fee_penalty,
    -- Остальные категории как сумма
    COALESCE(SUM(CASE 
        WHEN category NOT ILIKE '%Advance Charge%'
         AND category NOT ILIKE '%Activation%'
         AND category NOT ILIKE '%Monthly%'
         AND category NOT ILIKE '%Monthly Access%'
         AND category NOT ILIKE '%Suspension%'
         AND category NOT ILIKE '%Reactivation%'
         AND category NOT ILIKE '%Penalty%'
        THEN total_amount END), 0) AS fee_other,
    -- Общая сумма всех плат
    SUM(total_amount) AS fee_total
FROM aggregated
GROUP BY fee_period_date, contract_id;

-- Комментарии
COMMENT ON VIEW v_steccom_access_fees_pivot IS 
'Сводная таблица категорий плат STECCOM: каждая категория становится отдельной колонкой.
fee_period_date в формате YYYYMMDD из имени файла (например, 20251002) - цикл 30 дней со 2-го по 2-е.';

COMMENT ON COLUMN v_steccom_access_fees_pivot.fee_period_date IS 'Период в формате YYYYMMDD из имени файла (цикл 30 дней со 2-го по 2-е)';
COMMENT ON COLUMN v_steccom_access_fees_pivot.contract_id IS 'ID контракта (SUB-...)';
COMMENT ON COLUMN v_steccom_access_fees_pivot.fee_advance_charge IS 'Сумма Advance Charge';
COMMENT ON COLUMN v_steccom_access_fees_pivot.fee_activation IS 'Сумма Activation Fee';
COMMENT ON COLUMN v_steccom_access_fees_pivot.fee_monthly IS 'Сумма Monthly Access Fee';
COMMENT ON COLUMN v_steccom_access_fees_pivot.fee_suspension IS 'Сумма Suspension Fee';
COMMENT ON COLUMN v_steccom_access_fees_pivot.fee_reactivation IS 'Сумма Reactivation Fee';
COMMENT ON COLUMN v_steccom_access_fees_pivot.fee_penalty IS 'Сумма Penalty';
COMMENT ON COLUMN v_steccom_access_fees_pivot.fee_other IS 'Сумма прочих категорий';
COMMENT ON COLUMN v_steccom_access_fees_pivot.fee_total IS 'Общая сумма всех плат';

\echo 'Views V_STECCOM_ACCESS_FEES_NORM and V_STECCOM_ACCESS_FEES_PIVOT created successfully!'
\echo ''

