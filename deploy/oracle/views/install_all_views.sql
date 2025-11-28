-- ============================================================================
-- Установка всех представлений для отчетности Iridium M2M
-- База данных: Oracle
-- Порядок установки: views должны создаваться после tables и functions
-- ============================================================================

PROMPT
PROMPT ========================================
PROMPT Установка представлений Oracle
PROMPT ========================================
PROMPT

-- 1. V_SPNET_OVERAGE_ANALYSIS - базовый анализ превышения
PROMPT Создание V_SPNET_OVERAGE_ANALYSIS...
@@01_v_spnet_overage_analysis.sql

-- 2. V_CONSOLIDATED_OVERAGE_REPORT - консолидированный отчет
PROMPT Создание V_CONSOLIDATED_OVERAGE_REPORT...
@@02_v_consolidated_overage_report.sql

-- 3. V_IRIDIUM_SERVICES_INFO - информация о сервисах из биллинга (требует billing7@bm7)
PROMPT Создание V_IRIDIUM_SERVICES_INFO...
@@03_v_iridium_services_info.sql

-- 4. V_CONSOLIDATED_REPORT_WITH_BILLING - расширенный отчет с данными биллинга
PROMPT Создание V_CONSOLIDATED_REPORT_WITH_BILLING...
@@04_v_consolidated_report_with_billing.sql

PROMPT
PROMPT ========================================
PROMPT Проверка созданных представлений
PROMPT ========================================

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
FROM V_CONSOLIDATED_REPORT_WITH_BILLING;

PROMPT
PROMPT ========================================
PROMPT Установка представлений завершена!
PROMPT ========================================


