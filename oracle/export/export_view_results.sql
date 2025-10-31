-- ============================================================================
-- Export VIEW RESULTS from Oracle for comparison with PostgreSQL
-- Use this to verify both databases produce identical results
-- ============================================================================

SET ECHO OFF
SET FEEDBACK OFF
SET HEADING ON
SET PAGESIZE 50000
SET LINESIZE 32767
SET TRIMSPOOL ON
SET VERIFY OFF
SET COLSEP '|'

-- Export V_SPNET_OVERAGE_ANALYSIS results
SPOOL oracle_v_spnet_overage_analysis.txt

PROMPT ============================================================================
PROMPT V_SPNET_OVERAGE_ANALYSIS - Oracle Results
PROMPT ============================================================================
PROMPT

SELECT 
    IMEI,
    CONTRACT_ID,
    TO_CHAR(BILL_MONTH, 'YYYY-MM-DD') AS BILL_MONTH,
    PLAN_NAME,
    USAGE_TYPE,
    PLAN_CODE,
    INCLUDED_KB,
    RECORD_COUNT,
    TOTAL_USAGE_BYTES,
    TOTAL_USAGE_KB,
    OVERAGE_KB,
    CALCULATED_OVERAGE_CHARGE,
    SPNET_TOTAL_AMOUNT
FROM V_SPNET_OVERAGE_ANALYSIS
ORDER BY BILL_MONTH, IMEI;

PROMPT
PROMPT Total records:
SELECT COUNT(*) AS TOTAL_RECORDS FROM V_SPNET_OVERAGE_ANALYSIS;

SPOOL OFF

-- Export V_CONSOLIDATED_OVERAGE_REPORT results
SPOOL oracle_v_consolidated_overage_report.txt

PROMPT ============================================================================
PROMPT V_CONSOLIDATED_OVERAGE_REPORT - Oracle Results
PROMPT ============================================================================
PROMPT

SELECT 
    IMEI,
    CONTRACT_ID,
    TO_CHAR(BILL_MONTH, 'YYYY-MM-DD') AS BILL_MONTH,
    PLAN_NAME,
    PLAN_CODE,
    INCLUDED_KB,
    TOTAL_USAGE_KB,
    OVERAGE_KB,
    CALCULATED_OVERAGE_CHARGE,
    SPNET_TOTAL_AMOUNT,
    DIFFERENCE
FROM V_CONSOLIDATED_OVERAGE_REPORT
ORDER BY BILL_MONTH, IMEI;

PROMPT
PROMPT Total records:
SELECT COUNT(*) AS TOTAL_RECORDS FROM V_CONSOLIDATED_OVERAGE_REPORT;

PROMPT
PROMPT Summary by Plan:
SELECT 
    PLAN_NAME,
    COUNT(*) AS RECORDS,
    ROUND(SUM(TOTAL_USAGE_KB), 2) AS TOTAL_KB,
    ROUND(SUM(CALCULATED_OVERAGE_CHARGE), 2) AS TOTAL_OVERAGE_CHARGE
FROM V_CONSOLIDATED_OVERAGE_REPORT
GROUP BY PLAN_NAME
ORDER BY PLAN_NAME;

SPOOL OFF

-- Export V_IRIDIUM_SERVICES_INFO results
SPOOL oracle_v_iridium_services_info.txt

PROMPT ============================================================================
PROMPT V_IRIDIUM_SERVICES_INFO - Oracle Results
PROMPT ============================================================================
PROMPT

SELECT 
    IMEI,
    PHONE_NUMBER,
    SUBSCRIBER_NAME,
    TO_CHAR(BILLING_PERIOD, 'YYYY-MM-DD') AS BILLING_PERIOD,
    CHARGE_AMOUNT,
    CHARGE_DESCRIPTION,
    TO_CHAR(CHARGE_DATE, 'YYYY-MM-DD') AS CHARGE_DATE
FROM V_IRIDIUM_SERVICES_INFO
ORDER BY BILLING_PERIOD, IMEI;

PROMPT
PROMPT Total records:
SELECT COUNT(*) AS TOTAL_RECORDS FROM V_IRIDIUM_SERVICES_INFO;

SPOOL OFF

-- Export V_CONSOLIDATED_REPORT_WITH_BILLING results
SPOOL oracle_v_consolidated_report_with_billing.txt

PROMPT ============================================================================
PROMPT V_CONSOLIDATED_REPORT_WITH_BILLING - Oracle Results
PROMPT ============================================================================
PROMPT

SELECT 
    IMEI,
    CONTRACT_ID,
    TO_CHAR(BILLING_MONTH, 'YYYY-MM-DD') AS BILLING_MONTH,
    PLAN_NAME,
    PLAN_CODE,
    INCLUDED_KB,
    TOTAL_USAGE_KB,
    OVERAGE_KB,
    CALCULATED_OVERAGE_CHARGE,
    SPNET_TOTAL_AMOUNT,
    DIFFERENCE,
    PHONE_NUMBER,
    SUBSCRIBER_NAME,
    STECCOM_CHARGE_AMOUNT,
    STECCOM_CHARGE_DESCRIPTION
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
ORDER BY BILLING_MONTH, IMEI;

PROMPT
PROMPT Total records:
SELECT COUNT(*) AS TOTAL_RECORDS FROM V_CONSOLIDATED_REPORT_WITH_BILLING;

SPOOL OFF

-- Export function test results
SPOOL oracle_function_tests.txt

PROMPT ============================================================================
PROMPT CALCULATE_OVERAGE Function Tests - Oracle Results
PROMPT ============================================================================
PROMPT

SELECT 
    'Test 1: SBD-1K, 30KB' AS TEST_CASE,
    CALCULATE_OVERAGE('SBD Tiered 1250 1K', 30000) AS RESULT,
    27.25 AS EXPECTED,
    CASE 
        WHEN CALCULATE_OVERAGE('SBD Tiered 1250 1K', 30000) = 27.25 THEN 'PASS'
        ELSE 'FAIL'
    END AS STATUS
FROM DUAL
UNION ALL
SELECT 
    'Test 2: SBD-10K, 35KB',
    CALCULATE_OVERAGE('SBD Tiered 1250 10K', 35000),
    6.50,
    CASE 
        WHEN CALCULATE_OVERAGE('SBD Tiered 1250 10K', 35000) = 6.50 THEN 'PASS'
        ELSE 'FAIL'
    END
FROM DUAL
UNION ALL
SELECT 
    'Test 3: SBD-10K, 6KB (within)',
    CALCULATE_OVERAGE('SBD Tiered 1250 10K', 6000),
    0.00,
    CASE 
        WHEN CALCULATE_OVERAGE('SBD Tiered 1250 10K', 6000) = 0.00 THEN 'PASS'
        ELSE 'FAIL'
    END
FROM DUAL;

SPOOL OFF

-- Summary
SPOOL export_views_summary.txt

PROMPT ============================================================================
PROMPT Export Summary
PROMPT ============================================================================
PROMPT

SELECT 'V_SPNET_OVERAGE_ANALYSIS: ' || COUNT(*) || ' records' FROM V_SPNET_OVERAGE_ANALYSIS;
SELECT 'V_CONSOLIDATED_OVERAGE_REPORT: ' || COUNT(*) || ' records' FROM V_CONSOLIDATED_OVERAGE_REPORT;
SELECT 'V_IRIDIUM_SERVICES_INFO: ' || COUNT(*) || ' records' FROM V_IRIDIUM_SERVICES_INFO;
SELECT 'V_CONSOLIDATED_REPORT_WITH_BILLING: ' || COUNT(*) || ' records' FROM V_CONSOLIDATED_REPORT_WITH_BILLING;

PROMPT
PROMPT Files generated:
PROMPT   - oracle_v_spnet_overage_analysis.txt
PROMPT   - oracle_v_consolidated_overage_report.txt
PROMPT   - oracle_v_iridium_services_info.txt
PROMPT   - oracle_v_consolidated_report_with_billing.txt
PROMPT   - oracle_function_tests.txt
PROMPT
PROMPT Next steps:
PROMPT   1. Run same queries in PostgreSQL
PROMPT   2. Compare output files with diff
PROMPT   3. Verify calculations match exactly
PROMPT
PROMPT ============================================================================

SPOOL OFF

SET FEEDBACK ON
SET HEADING ON
SET ECHO ON

PROMPT
PROMPT View results exported successfully!
PROMPT Compare with PostgreSQL results using diff or file comparison tool.
PROMPT


