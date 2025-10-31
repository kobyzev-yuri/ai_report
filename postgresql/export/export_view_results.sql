-- ============================================================================
-- Export VIEW RESULTS from PostgreSQL for comparison with Oracle
-- Use this to verify both databases produce identical results
-- ============================================================================

\pset border 2
\pset format aligned

-- Export V_SPNET_OVERAGE_ANALYSIS results
\o postgres_v_spnet_overage_analysis.txt

\echo '============================================================================'
\echo 'V_SPNET_OVERAGE_ANALYSIS - PostgreSQL Results'
\echo '============================================================================'
\echo ''

SELECT 
    imei,
    contract_id,
    TO_CHAR(bill_month, 'YYYY-MM-DD') AS bill_month,
    plan_name,
    usage_type,
    plan_code,
    included_kb,
    record_count,
    total_usage_bytes,
    total_usage_kb,
    overage_kb,
    calculated_overage_charge,
    spnet_total_amount
FROM v_spnet_overage_analysis
ORDER BY bill_month, imei;

\echo ''
\echo 'Total records:'
SELECT COUNT(*) AS total_records FROM v_spnet_overage_analysis;

\o

-- Export V_CONSOLIDATED_OVERAGE_REPORT results
\o postgres_v_consolidated_overage_report.txt

\echo '============================================================================'
\echo 'V_CONSOLIDATED_OVERAGE_REPORT - PostgreSQL Results'
\echo '============================================================================'
\echo ''

SELECT 
    imei,
    contract_id,
    TO_CHAR(bill_month, 'YYYY-MM-DD') AS bill_month,
    plan_name,
    plan_code,
    included_kb,
    total_usage_kb,
    overage_kb,
    calculated_overage_charge,
    spnet_total_amount,
    difference
FROM v_consolidated_overage_report
ORDER BY bill_month, imei;

\echo ''
\echo 'Total records:'
SELECT COUNT(*) AS total_records FROM v_consolidated_overage_report;

\echo ''
\echo 'Summary by Plan:'
SELECT 
    plan_name,
    COUNT(*) AS records,
    ROUND(SUM(total_usage_kb)::numeric, 2) AS total_kb,
    ROUND(SUM(calculated_overage_charge)::numeric, 2) AS total_overage_charge
FROM v_consolidated_overage_report
GROUP BY plan_name
ORDER BY plan_name;

\o

-- Export V_IRIDIUM_SERVICES_INFO results
\o postgres_v_iridium_services_info.txt

\echo '============================================================================'
\echo 'V_IRIDIUM_SERVICES_INFO - PostgreSQL Results'
\echo '============================================================================'
\echo ''

SELECT 
    imei,
    phone_number,
    subscriber_name,
    TO_CHAR(billing_period, 'YYYY-MM-DD') AS billing_period,
    charge_amount,
    charge_description,
    TO_CHAR(charge_date, 'YYYY-MM-DD') AS charge_date
FROM v_iridium_services_info
ORDER BY billing_period, imei;

\echo ''
\echo 'Total records:'
SELECT COUNT(*) AS total_records FROM v_iridium_services_info;

\o

-- Export V_CONSOLIDATED_REPORT_WITH_BILLING results
\o postgres_v_consolidated_report_with_billing.txt

\echo '============================================================================'
\echo 'V_CONSOLIDATED_REPORT_WITH_BILLING - PostgreSQL Results'
\echo '============================================================================'
\echo ''

SELECT 
    imei,
    contract_id,
    TO_CHAR(billing_month, 'YYYY-MM-DD') AS billing_month,
    plan_name,
    plan_code,
    included_kb,
    total_usage_kb,
    overage_kb,
    calculated_overage_charge,
    spnet_total_amount,
    difference,
    phone_number,
    subscriber_name,
    steccom_charge_amount,
    steccom_charge_description
FROM v_consolidated_report_with_billing
ORDER BY billing_month, imei;

\echo ''
\echo 'Total records:'
SELECT COUNT(*) AS total_records FROM v_consolidated_report_with_billing;

\o

-- Export function test results
\o postgres_function_tests.txt

\echo '============================================================================'
\echo 'calculate_overage Function Tests - PostgreSQL Results'
\echo '============================================================================'
\echo ''

SELECT 
    'Test 1: SBD-1K, 30KB' AS test_case,
    calculate_overage('SBD Tiered 1250 1K', 30000) AS result,
    27.25 AS expected,
    CASE 
        WHEN calculate_overage('SBD Tiered 1250 1K', 30000) = 27.25 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
UNION ALL
SELECT 
    'Test 2: SBD-10K, 35KB',
    calculate_overage('SBD Tiered 1250 10K', 35000),
    6.50,
    CASE 
        WHEN calculate_overage('SBD Tiered 1250 10K', 35000) = 6.50 THEN 'PASS'
        ELSE 'FAIL'
    END
UNION ALL
SELECT 
    'Test 3: SBD-10K, 6KB (within)',
    calculate_overage('SBD Tiered 1250 10K', 6000),
    0.00,
    CASE 
        WHEN calculate_overage('SBD Tiered 1250 10K', 6000) = 0.00 THEN 'PASS'
        ELSE 'FAIL'
    END;

\o

-- Summary
\o export_views_summary.txt

\echo '============================================================================'
\echo 'Export Summary'
\echo '============================================================================'
\echo ''

SELECT 'v_spnet_overage_analysis: ' || COUNT(*) || ' records' FROM v_spnet_overage_analysis;
SELECT 'v_consolidated_overage_report: ' || COUNT(*) || ' records' FROM v_consolidated_overage_report;
SELECT 'v_iridium_services_info: ' || COUNT(*) || ' records' FROM v_iridium_services_info;
SELECT 'v_consolidated_report_with_billing: ' || COUNT(*) || ' records' FROM v_consolidated_report_with_billing;

\echo ''
\echo 'Files generated:'
\echo '  - postgres_v_spnet_overage_analysis.txt'
\echo '  - postgres_v_consolidated_overage_report.txt'
\echo '  - postgres_v_iridium_services_info.txt'
\echo '  - postgres_v_consolidated_report_with_billing.txt'
\echo '  - postgres_function_tests.txt'
\echo ''
\echo 'Compare with Oracle results using:'
\echo '  diff oracle_v_spnet_overage_analysis.txt postgres_v_spnet_overage_analysis.txt'
\echo ''
\echo '============================================================================'

\o

\echo ''
\echo 'View results exported successfully!'
\echo 'Compare with Oracle results to verify compatibility.'
\echo ''


