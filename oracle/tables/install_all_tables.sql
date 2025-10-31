-- ============================================================================
-- Установка всех таблиц для Iridium M2M Reporting
-- База данных: Oracle (production)
-- Порядок: tables → data → functions → views
-- ============================================================================

PROMPT
PROMPT ========================================
PROMPT Установка таблиц Oracle (Production)
PROMPT ========================================
PROMPT

-- 1. SPNET_TRAFFIC - основная таблица трафика
PROMPT [1/4] Создание SPNET_TRAFFIC...
@@01_spnet_traffic.sql

-- 2. STECCOM_EXPENSES - таблица расходов
PROMPT [2/4] Создание STECCOM_EXPENSES...
@@02_steccom_expenses.sql

-- 3. TARIFF_PLANS - справочник тарифов
PROMPT [3/4] Создание TARIFF_PLANS...
@@03_tariff_plans.sql

-- 4. LOAD_LOGS - журнал загрузок
PROMPT [4/4] Создание LOAD_LOGS...
@@04_load_logs.sql

PROMPT
PROMPT ========================================
PROMPT Проверка созданных таблиц
PROMPT ========================================

SELECT 
    table_name,
    num_rows,
    TO_CHAR(last_analyzed, 'YYYY-MM-DD HH24:MI') as last_analyzed
FROM user_tables
WHERE table_name IN ('SPNET_TRAFFIC', 'STECCOM_EXPENSES', 'TARIFF_PLANS', 'LOAD_LOGS')
ORDER BY table_name;

PROMPT
PROMPT ========================================
PROMPT Таблицы успешно созданы!
PROMPT ========================================
PROMPT
PROMPT Следующие шаги:
PROMPT 1. Загрузить данные тарифов: @../data/tariff_plans_data.sql
PROMPT 2. Создать функции: @../functions/calculate_overage.sql
PROMPT 3. Создать представления: @../views/install_all_views.sql
PROMPT


