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

-- 5. V_REVENUE_FROM_INVOICES - отчет по доходам из счетов-фактур
PROMPT Создание V_REVENUE_FROM_INVOICES...
@@05_v_revenue_from_invoices.sql

-- 5.1. V_COST_OF_GOODS_SOLD - себестоимость проданных товаров
PROMPT Создание V_COST_OF_GOODS_SOLD...
@@05_v_cost_of_goods_sold.sql

-- 6. V_PROFITABILITY_BY_PERIOD - базовая прибыльность по периодам
PROMPT Создание V_PROFITABILITY_BY_PERIOD...
@@06_v_profitability_by_period.sql

-- 7. V_PROFITABILITY_TREND - тенденции прибыльности с LAG
PROMPT Создание V_PROFITABILITY_TREND...
@@07_v_profitability_trend.sql

-- 8. V_UNPROFITABLE_CUSTOMERS - убыточные клиенты и клиенты с низкой маржой
PROMPT Создание V_UNPROFITABLE_CUSTOMERS...
@@08_v_unprofitable_customers.sql

-- 9. V_ANALYTICS_INVOICE_PERIOD - отчет "Счета за период" для биллинг операторов
PROMPT Создание V_ANALYTICS_INVOICE_PERIOD...
@@07_v_analytics_invoice_period.sql

PROMPT
PROMPT ========================================
PROMPT Конец установки (без проверочных SELECT по представлениям — они могли «висеть» на COUNT)
PROMPT При необходимости вручную: SELECT object_name, status FROM user_objects
PROMPT   WHERE object_type='VIEW' AND object_name LIKE 'V_%' ORDER BY 1;
PROMPT ========================================

SELECT SYSDATE AS install_finished_at FROM DUAL;

PROMPT
PROMPT ========================================
PROMPT Установка представлений завершена!
PROMPT ========================================


