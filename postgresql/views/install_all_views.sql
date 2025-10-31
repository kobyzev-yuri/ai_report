-- ============================================================================
-- Установка всех представлений для тестирования Iridium M2M Reporting
-- База данных: PostgreSQL (testing)
-- Порядок установки: tables → data → functions → views
-- ============================================================================

\echo ''
\echo '========================================'
\echo 'Установка представлений PostgreSQL'
\echo '========================================'
\echo ''

-- 1. V_SPNET_OVERAGE_ANALYSIS - базовый анализ превышения
\echo '[1/4] Создание V_SPNET_OVERAGE_ANALYSIS...'
\i 01_v_spnet_overage_analysis.sql

-- 2. V_CONSOLIDATED_OVERAGE_REPORT - консолидированный отчет
\echo '[2/5] Создание V_CONSOLIDATED_OVERAGE_REPORT...'
\i 02_v_consolidated_overage_report.sql

-- 3. V_IRIDIUM_SERVICES_INFO - информация о сервисах (wrapper)
\echo '[3/5] Создание V_IRIDIUM_SERVICES_INFO...'
\i 03_v_iridium_services_info.sql

-- 4. V_CONSOLIDATED_REPORT_WITH_BILLING - отчет с данными клиентов
\echo '[4/5] Создание V_CONSOLIDATED_REPORT_WITH_BILLING...'
\i 04_v_consolidated_report_with_billing.sql

-- 5. V_STECCOM_ACCESS_FEES_PIVOT - сводная таблица категорий плат STECCOM
\echo '[5/5] Создание V_STECCOM_ACCESS_FEES_PIVOT...'
\i 05_v_steccom_access_fees_pivot.sql

\echo ''
\echo '========================================'
\echo 'Проверка созданных представлений'
\echo '========================================'

SELECT 
    'V_SPNET_OVERAGE_ANALYSIS' as view_name,
    COUNT(*) as record_count
FROM V_SPNET_OVERAGE_ANALYSIS
UNION ALL
SELECT 
    'V_CONSOLIDATED_OVERAGE_REPORT',
    COUNT(*)
FROM V_CONSOLIDATED_OVERAGE_REPORT
UNION ALL
SELECT 
    'V_IRIDIUM_SERVICES_INFO',
    COUNT(*)
FROM V_IRIDIUM_SERVICES_INFO
UNION ALL
SELECT 
    'V_CONSOLIDATED_REPORT_WITH_BILLING',
    COUNT(*)
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
UNION ALL
SELECT 
    'V_STECCOM_ACCESS_FEES_PIVOT',
    COUNT(*)
FROM V_STECCOM_ACCESS_FEES_PIVOT;

\echo ''
\echo '========================================'
\echo 'Установка представлений завершена!'
\echo '========================================'
\echo ''
\echo 'Примечание: V_IRIDIUM_SERVICES_INFO и V_CONSOLIDATED_REPORT_WITH_BILLING'
\echo 'используют данные из таблицы IRIDIUM_SERVICES_INFO (импорт из Oracle).'
\echo 'Убедитесь, что вы загрузили данные: ./data/import_oracle_dump.sh'
\echo ''


