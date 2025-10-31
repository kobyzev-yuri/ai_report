-- ============================================================================
-- V_IRIDIUM_SERVICES_INFO
-- View wrapper for IRIDIUM_SERVICES_INFO table (imported from Oracle)
-- База данных: PostgreSQL (testing)
-- ============================================================================

-- В PostgreSQL для тестирования мы используем таблицу IRIDIUM_SERVICES_INFO,
-- импортированную из Oracle, поэтому view просто оборачивает таблицу

DROP VIEW IF EXISTS V_IRIDIUM_SERVICES_INFO CASCADE;

CREATE OR REPLACE VIEW V_IRIDIUM_SERVICES_INFO AS
SELECT 
    service_id,
    contract_id,
    imei,
    tariff_id,
    agreement_number,
    order_number,
    status,
    actual_status,
    customer_id,
    organization_name,
    person_name,
    customer_name,
    create_date,
    start_date,
    stop_date,
    account_id,
    code_1c
FROM iridium_services_info;

-- Комментарии
COMMENT ON VIEW v_iridium_services_info IS 'View для доступа к данным Iridium сервисов (wrapper для таблицы импорта)';

\echo 'View V_IRIDIUM_SERVICES_INFO created successfully!'
\echo ''
\echo 'This view wraps IRIDIUM_SERVICES_INFO table imported from Oracle.'
\echo 'For production, this would query SERVICES, CUSTOMERS, ACCOUNTS tables directly.'
\echo ''



