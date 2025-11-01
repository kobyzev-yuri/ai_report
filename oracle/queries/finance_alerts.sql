-- ============================================================================
-- Finance Alerts for Iridium Services
-- Detects financial issues and discrepancies requiring attention
-- ============================================================================

SET LINESIZE 250
SET PAGESIZE 100

PROMPT ============================================================================
PROMPT FINANCE ALERT REPORT - Iridium Services
PROMPT Generated: 
SELECT TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') FROM DUAL;
PROMPT ============================================================================
PROMPT

-- ============================================================================
PROMPT
PROMPT [ALERT 1] Services Missing in SPNET Traffic Data
PROMPT Issue: Active Iridium services with no usage data
PROMPT Action: Check if devices are transmitting or SPNET load failed
PROMPT ============================================================================

SELECT 
    s.SERVICE_ID,
    s.LOGIN AS CONTRACT_ID,
    s.PASSWD AS IMEI,
    c.CUSTOMER_ID,
    a.DESCRIPTION AS AGREEMENT,
    (SELECT oi.EXT_ID FROM OUTER_IDS oi 
     WHERE oi.ID = c.CUSTOMER_ID AND oi.TBL = 'CUSTOMERS' AND ROWNUM = 1) AS CODE_1C,
    s.CREATE_DATE,
    TRUNC(SYSDATE) - TRUNC(s.CREATE_DATE) AS DAYS_ACTIVE
FROM SERVICES s
JOIN CUSTOMERS c ON s.CUSTOMER_ID = c.CUSTOMER_ID
JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
WHERE s.TYPE_ID = 9002
  AND s.STATUS = 1
  AND s.LOGIN NOT LIKE '%-clone-%'
  AND s.CREATE_DATE < ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1)  -- Active > 1 month
  AND NOT EXISTS (
      SELECT 1 FROM SPNET_TRAFFIC st 
      WHERE st.CONTRACT_ID = s.LOGIN
      AND st.BILL_MONTH >= TO_NUMBER(TO_CHAR(ADD_MONTHS(SYSDATE, -3), 'YYYYMM'))  -- Last 3 months
  )
ORDER BY s.CREATE_DATE;

-- ============================================================================
PROMPT
PROMPT [ALERT 2] Overage Charge Discrepancies
PROMPT Issue: Calculated overage differs from billed amount by >$5
PROMPT Action: Review pricing, investigate billing errors
PROMPT ============================================================================

SELECT 
    r.IMEI,
    r.CONTRACT_ID,
    r.CUSTOMER_NAME,
    r.CODE_1C,
    r.BILL_MONTH,
    r.PLAN_NAME,
    r.TOTAL_USAGE_KB,
    r.OVERAGE_KB,
    r.CALCULATED_OVERAGE AS CALCULATED_CHARGE,
    r.SPNET_TOTAL_AMOUNT AS BILLED_CHARGE,
    ABS(r.SPNET_TOTAL_AMOUNT - r.CALCULATED_OVERAGE) AS DIFFERENCE,
    CASE 
        WHEN r.CALCULATED_OVERAGE > r.SPNET_TOTAL_AMOUNT THEN 'UNDERBILLED'
        ELSE 'OVERBILLED'
    END AS ISSUE_TYPE
FROM V_CONSOLIDATED_REPORT_WITH_BILLING r
WHERE r.SERVICE_STATUS = 1
  AND r.USAGE_TYPE = 'SBD Data Usage'
  AND ABS(r.SPNET_TOTAL_AMOUNT - r.CALCULATED_OVERAGE) > 5  -- Difference > $5
  AND r.BILL_MONTH >= ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -3)  -- Last 3 months
ORDER BY ABS(r.SPNET_TOTAL_AMOUNT - r.CALCULATED_OVERAGE) DESC;

-- ============================================================================
PROMPT
PROMPT [ALERT 3] High Overage Charges (>$50)
PROMPT Issue: Unusually high overage fees
PROMPT Action: Notify customer, consider plan upgrade
PROMPT ============================================================================

SELECT 
    r.IMEI,
    r.CONTRACT_ID,
    r.CUSTOMER_NAME,
    r.CODE_1C,
    r.AGREEMENT_NUMBER,
    r.BILL_MONTH,
    r.PLAN_NAME,
    r.INCLUDED_KB,
    r.TOTAL_USAGE_KB,
    r.OVERAGE_KB,
    r.CALCULATED_OVERAGE AS OVERAGE_CHARGE,
    ROUND((r.OVERAGE_KB / NULLIF(r.INCLUDED_KB, 0)) * 100, 0) AS OVERAGE_PERCENT
FROM V_CONSOLIDATED_REPORT_WITH_BILLING r
WHERE r.SERVICE_STATUS = 1
  AND r.CALCULATED_OVERAGE > 50  -- Over $50
  AND r.BILL_MONTH >= ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1)  -- Last month
ORDER BY r.CALCULATED_OVERAGE DESC;

-- ============================================================================
PROMPT
PROMPT [ALERT 4] Services Missing CODE_1C
PROMPT Issue: Active services not linked to 1C customer
PROMPT Action: Add CODE_1C mapping in OUTER_IDS table
PROMPT ============================================================================

SELECT 
    s.SERVICE_ID,
    s.LOGIN AS CONTRACT_ID,
    s.PASSWD AS IMEI,
    c.CUSTOMER_ID,
    a.DESCRIPTION AS AGREEMENT,
    s.BLANK AS ORDER_NUMBER,
    (SELECT VALUE FROM BM_CUSTOMER_CONTACT cc 
     JOIN BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID 
     WHERE cc.CUSTOMER_ID = c.CUSTOMER_ID AND cd.MNEMONIC = 'b_name' AND ROWNUM = 1) AS CUSTOMER_NAME,
    s.CREATE_DATE
FROM SERVICES s
JOIN CUSTOMERS c ON s.CUSTOMER_ID = c.CUSTOMER_ID
JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
WHERE s.TYPE_ID = 9002
  AND s.STATUS = 1
  AND s.LOGIN NOT LIKE '%-clone-%'
  AND NOT EXISTS (
      SELECT 1 FROM OUTER_IDS oi 
      WHERE oi.ID = c.CUSTOMER_ID 
      AND oi.TBL = 'CUSTOMERS'
  )
ORDER BY s.CREATE_DATE;

-- ============================================================================
PROMPT
PROMPT [ALERT 5] Consistent High Usage (Potential Plan Upgrade)
PROMPT Issue: Services with overage in 3+ consecutive months
PROMPT Action: Recommend higher tier plan to customer
PROMPT ============================================================================

WITH monthly_overage AS (
    SELECT 
        IMEI,
        CONTRACT_ID,
        CUSTOMER_NAME,
        CODE_1C,
        PLAN_NAME,
        BILL_MONTH,
        CALCULATED_OVERAGE,
        COUNT(*) OVER (PARTITION BY IMEI) AS MONTHS_WITH_OVERAGE
    FROM V_CONSOLIDATED_REPORT_WITH_BILLING
    WHERE CALCULATED_OVERAGE > 0
      AND BILL_MONTH >= ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -6)
)
SELECT 
    IMEI,
    CONTRACT_ID,
    CUSTOMER_NAME,
    CODE_1C,
    PLAN_NAME AS CURRENT_PLAN,
    MONTHS_WITH_OVERAGE,
    SUM(CALCULATED_OVERAGE) AS TOTAL_OVERAGE_6M,
    ROUND(AVG(CALCULATED_OVERAGE), 2) AS AVG_OVERAGE
FROM monthly_overage
WHERE MONTHS_WITH_OVERAGE >= 3
GROUP BY IMEI, CONTRACT_ID, CUSTOMER_NAME, CODE_1C, PLAN_NAME, MONTHS_WITH_OVERAGE
ORDER BY TOTAL_OVERAGE_6M DESC;

-- ============================================================================
PROMPT
PROMPT [ALERT 6] Duplicate IMEI Assignments
PROMPT Issue: Same IMEI assigned to multiple active services
PROMPT Action: Review and deactivate duplicates
PROMPT ============================================================================

SELECT 
    s.PASSWD AS IMEI,
    COUNT(*) AS ACTIVE_SERVICES,
    LISTAGG(s.LOGIN, ', ') WITHIN GROUP (ORDER BY s.SERVICE_ID) AS CONTRACT_IDS,
    LISTAGG(s.SERVICE_ID, ', ') WITHIN GROUP (ORDER BY s.SERVICE_ID) AS SERVICE_IDS
FROM SERVICES s
WHERE s.TYPE_ID = 9002
  AND s.STATUS = 1
  AND s.PASSWD IS NOT NULL
  AND s.LOGIN NOT LIKE '%-clone-%'
GROUP BY s.PASSWD
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;

-- ============================================================================
PROMPT
PROMPT [ALERT 7] Services Without Recent Traffic (Potential Inactive)
PROMPT Issue: No traffic data in last 60 days
PROMPT Action: Check if service should be suspended
PROMPT ============================================================================

SELECT 
    s.SERVICE_ID,
    s.LOGIN AS CONTRACT_ID,
    s.PASSWD AS IMEI,
    a.DESCRIPTION AS AGREEMENT,
    (SELECT MAX(st.BILL_MONTH) FROM SPNET_TRAFFIC st 
     WHERE st.CONTRACT_ID = s.LOGIN) AS LAST_TRAFFIC_MONTH,
    TRUNC(SYSDATE) - TRUNC(s.CREATE_DATE) AS DAYS_ACTIVE
FROM SERVICES s
JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
WHERE s.TYPE_ID = 9002
  AND s.STATUS = 1
  AND s.LOGIN NOT LIKE '%-clone-%'
  AND s.CREATE_DATE < TRUNC(SYSDATE) - 60
  AND NOT EXISTS (
      SELECT 1 FROM SPNET_TRAFFIC st 
      WHERE st.CONTRACT_ID = s.LOGIN
      AND st.BILL_MONTH >= TO_NUMBER(TO_CHAR(ADD_MONTHS(SYSDATE, -2), 'YYYYMM'))
  )
ORDER BY s.CREATE_DATE;

-- ============================================================================
PROMPT
PROMPT ============================================================================
PROMPT END OF FINANCE ALERT REPORT
PROMPT Review alerts and take appropriate action
PROMPT ============================================================================




