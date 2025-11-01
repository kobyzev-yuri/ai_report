-- ============================================================================
-- Export Billing Integration Data for 1C (PostgreSQL)
-- Minimal data extract: customer_name, agreement_no, blank, code_1c
-- ============================================================================

\pset border 0
\pset format unaligned
\pset fieldsep '|'

-- Export to CSV for 1C integration
\o billing_integration.csv

\echo 'CONTRACT_ID|IMEI|CUSTOMER_NAME|AGREEMENT_NUMBER|ORDER_NUMBER|CODE_1C|STATUS'

SELECT 
    contract_id,
    imei,
    COALESCE(customer_name, ''),
    COALESCE(agreement_number, ''),
    COALESCE(order_number, ''),
    COALESCE(code_1c, ''),
    COALESCE(status::text, '')
FROM v_iridium_services_info
WHERE status = 1  -- Только активные сервисы
ORDER BY code_1c, contract_id;

\o

-- Export to SQL INSERT format (alternative)
\o billing_integration.sql

\echo '-- ============================================================================'
\echo '-- Billing Integration Data - SQL Format'
\echo '-- ============================================================================'
\echo ''
\echo '-- DROP TABLE IF EXISTS billing_integration CASCADE;'
\echo '-- CREATE TABLE billing_integration ('
\echo '--     contract_id VARCHAR(50),'
\echo '--     imei VARCHAR(50),'
\echo '--     customer_name VARCHAR(500),'
\echo '--     agreement_number VARCHAR(200),'
\echo '--     order_number VARCHAR(100),'
\echo '--     code_1c VARCHAR(100),'
\echo '--     status VARCHAR(20)'
\echo '-- );'
\echo ''

SELECT 
    'INSERT INTO billing_integration (contract_id, imei, customer_name, agreement_number, order_number, code_1c, status) VALUES (' ||
    '''' || contract_id || ''', ' ||
    '''' || imei || ''', ' ||
    CASE WHEN customer_name IS NULL THEN 'NULL' ELSE '''' || REPLACE(customer_name, '''', '''''') || '''' END || ', ' ||
    CASE WHEN agreement_number IS NULL THEN 'NULL' ELSE '''' || REPLACE(agreement_number, '''', '''''') || '''' END || ', ' ||
    CASE WHEN order_number IS NULL THEN 'NULL' ELSE '''' || order_number || '''' END || ', ' ||
    CASE WHEN code_1c IS NULL THEN 'NULL' ELSE '''' || code_1c || '''' END || ', ' ||
    '''' || status || '''' ||
    ');'
FROM v_iridium_services_info
WHERE status = 1
ORDER BY code_1c, contract_id;

\o

-- Extended report with usage and charges
\o billing_integration_with_charges.csv

\echo 'CONTRACT_ID|IMEI|CUSTOMER_NAME|AGREEMENT_NUMBER|ORDER_NUMBER|CODE_1C|BILL_MONTH|PLAN_NAME|USAGE_KB|OVERAGE_CHARGE|TOTAL_AMOUNT'

SELECT 
    r.contract_id,
    r.imei,
    COALESCE(r.customer_name, ''),
    COALESCE(r.agreement_number, ''),
    COALESCE(r.order_number, ''),
    COALESCE(r.code_1c, ''),
    TO_CHAR(r.bill_month, 'YYYY-MM'),
    COALESCE(r.plan_name, ''),
    COALESCE(r.total_usage_kb::text, '0'),
    COALESCE(r.calculated_overage::text, '0'),
    COALESCE((r.spnet_total_amount + r.steccom_total_amount)::text, '0')
FROM v_consolidated_report_with_billing r
WHERE r.service_status = 1
ORDER BY r.code_1c, r.bill_month DESC, r.contract_id;

\o

-- Summary
\o billing_integration_summary.txt

\echo '============================================================================'
\echo 'Billing Integration Export Summary'
\echo '============================================================================'
\echo ''

SELECT 'Total active services: ' || COUNT(*) FROM v_iridium_services_info WHERE status = 1;
SELECT 'Services with CODE_1C: ' || COUNT(*) FROM v_iridium_services_info WHERE status = 1 AND code_1c IS NOT NULL;
SELECT 'Services without CODE_1C: ' || COUNT(*) FROM v_iridium_services_info WHERE status = 1 AND code_1c IS NULL;

\echo ''
\echo 'Files generated:'
\echo '  1. billing_integration.csv - Basic customer data (CSV format)'
\echo '  2. billing_integration.sql - Basic customer data (SQL format)'
\echo '  3. billing_integration_with_charges.csv - With usage and charges'
\echo '  4. billing_integration_summary.txt - This summary'
\echo ''
\echo 'CSV Fields:'
\echo '  - CONTRACT_ID: Login from services'
\echo '  - IMEI: Device identifier'
\echo '  - CUSTOMER_NAME: Organization name or person FIO'
\echo '  - AGREEMENT_NUMBER: Contract number (договор)'
\echo '  - ORDER_NUMBER: Order/appendix number (бланк)'
\echo '  - CODE_1C: Customer code from 1C system'
\echo '  - STATUS: Service status (1=active)'
\echo ''
\echo 'Usage in 1C:'
\echo '  - Import billing_integration.csv into 1C'
\echo '  - Use CODE_1C for matching with existing customers'
\echo '  - Use billing_integration_with_charges.csv for monthly billing'
\echo ''
\echo '============================================================================'

\o

\echo ''
\echo 'Billing integration data exported successfully!'
\echo ''




