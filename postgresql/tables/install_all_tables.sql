-- ============================================================================
-- Установка всех таблиц для тестирования Iridium M2M Reporting
-- База данных: PostgreSQL (testing)
-- Порядок: tables → data → functions → views
-- ============================================================================

\echo ''
\echo '========================================'
\echo 'Установка таблиц PostgreSQL (Testing)'
\echo '========================================'
\echo ''

-- 1. SPNET_TRAFFIC - основная таблица трафика
\echo '[1/4] Создание SPNET_TRAFFIC...'
\i 01_spnet_traffic.sql

-- 2. STECCOM_EXPENSES - таблица расходов
\echo '[2/4] Создание STECCOM_EXPENSES...'
\i 02_steccom_expenses.sql

-- 3. TARIFF_PLANS - справочник тарифов
\echo '[3/4] Создание TARIFF_PLANS...'
\i 03_tariff_plans.sql

-- 4. LOAD_LOGS - журнал загрузок
\echo '[4/4] Создание LOAD_LOGS...'
\i 04_load_logs.sql

\echo ''
\echo '========================================'
\echo 'Проверка созданных таблиц'
\echo '========================================'

SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE tablename IN ('spnet_traffic', 'steccom_expenses', 'tariff_plans', 'load_logs')
  AND schemaname = 'public'
ORDER BY tablename;

\echo ''
\echo '========================================'
\echo 'Таблицы успешно созданы!'
\echo '========================================'
\echo ''
\echo 'Следующие шаги:'
\echo '1. Загрузить данные тарифов: \i ../data/tariff_plans_data.sql'
\echo '2. Создать функции: \i ../functions/calculate_overage.sql'
\echo '3. Создать представления: \i ../views/install_all_views.sql'
\echo '4. Или загрузить данные из Oracle: \i ../scripts/load_from_oracle_views.sql'
\echo ''


