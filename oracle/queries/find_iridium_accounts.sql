-- ============================================================================
-- Find All Iridium Accounts in Billing System
-- Quick queries to locate and check Iridium services
-- ============================================================================

SET LINESIZE 200
SET PAGESIZE 100

PROMPT ============================================================================
PROMPT Query 1: All Active Iridium Services
PROMPT ============================================================================
PROMPT

SELECT 
    s.SERVICE_ID,
    s.LOGIN AS CONTRACT_ID,
    s.PASSWD AS IMEI,
    c.CUSTOMER_ID,
    NVL(
        (SELECT VALUE FROM BM_CUSTOMER_CONTACT cc 
         JOIN BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID 
         WHERE cc.CUSTOMER_ID = c.CUSTOMER_ID AND cd.MNEMONIC = 'b_name' AND ROWNUM = 1),
        (SELECT TRIM(
            NVL((SELECT VALUE FROM BM_CUSTOMER_CONTACT cc2 
                 JOIN BM_CONTACT_DICT cd2 ON cc2.CONTACT_DICT_ID = cd2.CONTACT_DICT_ID 
                 WHERE cc2.CUSTOMER_ID = c.CUSTOMER_ID AND cd2.MNEMONIC = 'last_name' AND ROWNUM = 1), '') || ' ' ||
            NVL((SELECT VALUE FROM BM_CUSTOMER_CONTACT cc3 
                 JOIN BM_CONTACT_DICT cd3 ON cc3.CONTACT_DICT_ID = cd3.CONTACT_DICT_ID 
                 WHERE cc3.CUSTOMER_ID = c.CUSTOMER_ID AND cd3.MNEMONIC = 'first_name' AND ROWNUM = 1), '')
         ) FROM DUAL)
    ) AS CUSTOMER_NAME,
    a.DESCRIPTION AS AGREEMENT,
    s.TARIFF_ID,
    s.STATUS,
    s.CREATE_DATE,
    a.ACCOUNT_ID
FROM SERVICES s
JOIN CUSTOMERS c ON s.CUSTOMER_ID = c.CUSTOMER_ID
JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
WHERE s.TYPE_ID = 9002  -- Iridium SBD services
  AND s.STATUS = 1      -- Active only
  AND s.LOGIN NOT LIKE '%-clone-%'
ORDER BY s.SERVICE_ID;

PROMPT
PROMPT ============================================================================
PROMPT Query 2: Services by Customer
PROMPT ============================================================================
PROMPT

SELECT 
    c.CUSTOMER_ID,
    COUNT(*) AS SERVICE_COUNT,
    MIN(s.CREATE_DATE) AS FIRST_SERVICE_DATE,
    MAX(s.CREATE_DATE) AS LAST_SERVICE_DATE,
    SUM(CASE WHEN s.STATUS = 1 THEN 1 ELSE 0 END) AS ACTIVE_COUNT
FROM SERVICES s
JOIN CUSTOMERS c ON s.CUSTOMER_ID = c.CUSTOMER_ID
WHERE s.TYPE_ID = 9002
GROUP BY c.CUSTOMER_ID
HAVING COUNT(*) > 0
ORDER BY SERVICE_COUNT DESC;

PROMPT
PROMPT ============================================================================
PROMPT Query 3: Find Specific Service by IMEI or CONTRACT_ID
PROMPT ============================================================================
PROMPT Enter search criteria:
PROMPT

-- Replace with your search value
ACCEPT search_value CHAR PROMPT 'Enter IMEI or CONTRACT_ID: '

SELECT 
    s.SERVICE_ID,
    s.LOGIN AS CONTRACT_ID,
    s.PASSWD AS IMEI,
    c.CUSTOMER_ID,
    a.DESCRIPTION AS AGREEMENT,
    s.STATUS,
    s.CREATE_DATE,
    s.START_DATE,
    s.STOP_DATE
FROM SERVICES s
JOIN CUSTOMERS c ON s.CUSTOMER_ID = c.CUSTOMER_ID
JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
WHERE s.TYPE_ID = 9002
  AND (s.LOGIN LIKE '%&search_value%' OR s.PASSWD LIKE '%&search_value%');

PROMPT
PROMPT ============================================================================
PROMPT Query 4: Services with No IMEI
PROMPT ============================================================================
PROMPT

SELECT 
    s.SERVICE_ID,
    s.LOGIN AS CONTRACT_ID,
    s.PASSWD AS IMEI,
    a.DESCRIPTION AS AGREEMENT,
    s.STATUS,
    s.CREATE_DATE
FROM SERVICES s
JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
WHERE s.TYPE_ID = 9002
  AND s.PASSWD IS NULL
ORDER BY s.CREATE_DATE DESC;

PROMPT
PROMPT ============================================================================
PROMPT Query 5: Recent Service Activity (Last 30 Days)
PROMPT ============================================================================
PROMPT

SELECT 
    s.SERVICE_ID,
    s.LOGIN AS CONTRACT_ID,
    s.PASSWD AS IMEI,
    s.STATUS,
    s.CREATE_DATE,
    s.START_DATE,
    s.STOP_DATE,
    CASE 
        WHEN s.CREATE_DATE >= TRUNC(SYSDATE) - 30 THEN 'NEW'
        WHEN s.STOP_DATE >= TRUNC(SYSDATE) - 30 THEN 'CLOSED'
        ELSE 'MODIFIED'
    END AS ACTIVITY
FROM SERVICES s
WHERE s.TYPE_ID = 9002
  AND (
    s.CREATE_DATE >= TRUNC(SYSDATE) - 30
    OR s.START_DATE >= TRUNC(SYSDATE) - 30
    OR s.STOP_DATE >= TRUNC(SYSDATE) - 30
  )
ORDER BY 
    CASE 
        WHEN s.CREATE_DATE >= TRUNC(SYSDATE) - 30 THEN s.CREATE_DATE
        WHEN s.STOP_DATE >= TRUNC(SYSDATE) - 30 THEN s.STOP_DATE
        ELSE s.START_DATE
    END DESC;

PROMPT
PROMPT ============================================================================







