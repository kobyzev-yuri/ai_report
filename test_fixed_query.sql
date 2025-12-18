-- Тест исправленного запроса для получения клиентов с адресами
SET PAGESIZE 50
SET LINESIZE 200
SET FEEDBACK OFF

PROMPT ============================================================================
PROMPT Тест исправленного запроса: клиенты с адресами
PROMPT ============================================================================
SELECT 
    cc.CUSTOMER_ID,
    COALESCE(
        MAX(CASE WHEN cd.MNEMONIC = 'description' AND cc.CONTACT_DICT_ID = 23 THEN cc.VALUE END),
        TRIM(NVL(MAX(CASE WHEN cd.MNEMONIC = 'last_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' || 
             NVL(MAX(CASE WHEN cd.MNEMONIC = 'first_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' || 
             NVL(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), ''))
    ) AS CUSTOMER_NAME,
    COALESCE(
        MAX(CASE WHEN cd.MNEMONIC = 'b_paddress' AND cc.CONTACT_DICT_ID = 140 THEN cc.VALUE END),
        MAX(CASE WHEN cd.MNEMONIC = 'paddress' AND cc.CONTACT_DICT_ID = 10 THEN cc.VALUE END),
        MAX(CASE WHEN cd.MNEMONIC = 'address' AND cc.CONTACT_DICT_ID = 9 THEN cc.VALUE END)
    ) AS LEGAL_ADDRESS
FROM 
    BM_CUSTOMER_CONTACT cc
JOIN 
    BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID
WHERE 
    cc.CONTACT_DICT_ID IN (9, 10, 11, 23, 140)
    AND EXISTS (
        SELECT 1 
        FROM BM_INVOICE inv 
        JOIN BM_PERIOD p ON inv.PERIOD_ID = p.PERIOD_ID 
        WHERE inv.CUSTOMER_ID = cc.CUSTOMER_ID 
        AND TO_CHAR(p.START_DATE, 'YYYY') = TO_CHAR(SYSDATE, 'YYYY')
        AND ROWNUM = 1
    )
    AND ROWNUM <= 10
GROUP BY 
    cc.CUSTOMER_ID
ORDER BY 
    CUSTOMER_NAME;

EXIT;
