-- ============================================================================
-- Предоставление прав доступа пользователю cnn на все views
-- База данных: PostgreSQL (billing)
-- ============================================================================

-- Предоставляем права на все существующие views
GRANT SELECT ON v_consolidated_overage_report TO cnn;
GRANT SELECT ON v_consolidated_report_with_billing TO cnn;
GRANT SELECT ON v_steccom_access_fees_norm TO cnn;
GRANT SELECT ON v_steccom_access_fees_pivot TO cnn;
GRANT SELECT ON v_iridium_services_info TO cnn;
GRANT SELECT ON v_spnet_overage_analysis TO cnn;

-- Устанавливаем права по умолчанию для будущих views (созданных пользователем postgres)
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT SELECT ON TABLES TO cnn;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT SELECT ON SEQUENCES TO cnn;

\echo 'Права доступа предоставлены пользователю cnn'


