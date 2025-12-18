-- Проверка адресов клиентов в Oracle
SET PAGESIZE 1000
SET LINESIZE 200
SET FEEDBACK OFF

PROMPT ============================================================================
PROMPT Проверка: правильное название таблицы периодов
PROMPT ============================================================================
SELECT TABLE_NAME FROM USER_TABLES WHERE UPPER(TABLE_NAME) LIKE '%PERIOD%' ORDER BY TABLE_NAME;

PROMPT 
PROMPT ============================================================================
PROMPT Детальная информация по типам адресов в BM_CONTACT_DICT для CONTACT_DICT_ID = 8
PROMPT ============================================================================
SELECT 
    CONTACT_DICT_ID,
    MNEMONIC,
    NAME
FROM 
    BM_CONTACT_DICT
WHERE 
    CONTACT_DICT_ID = 8
ORDER BY 
    MNEMONIC;

PROMPT 
PROMPT ============================================================================
PROMPT Проверка: какие контакты есть у клиента "AssetLink Global" (CUSTOMER_ID = 228211)
PROMPT ============================================================================
SELECT 
    cc.CUSTOMER_ID,
    cc.CONTACT_DICT_ID,
    cd.MNEMONIC,
    cd.NAME,
    cc.VALUE
FROM 
    BM_CUSTOMER_CONTACT cc
JOIN 
    BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID
WHERE 
    cc.CUSTOMER_ID = 228211
ORDER BY 
    cc.CONTACT_DICT_ID, cd.MNEMONIC;

PROMPT 
PROMPT ============================================================================
PROMPT Проверка: есть ли у клиентов адреса (CONTACT_DICT_ID = 8)
PROMPT ============================================================================
SELECT 
    COUNT(DISTINCT cc.CUSTOMER_ID) AS CUSTOMERS_WITH_ADDRESSES,
    cd.MNEMONIC,
    COUNT(*) AS RECORDS_COUNT
FROM 
    BM_CUSTOMER_CONTACT cc
JOIN 
    BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID
WHERE 
    cc.CONTACT_DICT_ID = 8
GROUP BY 
    cd.MNEMONIC
ORDER BY 
    cd.MNEMONIC;

PROMPT 
PROMPT ============================================================================
PROMPT Примеры клиентов с адресами
PROMPT ============================================================================
SELECT 
    cc.CUSTOMER_ID,
    cd.MNEMONIC,
    SUBSTR(cc.VALUE, 1, 50) AS ADDRESS_VALUE
FROM 
    BM_CUSTOMER_CONTACT cc
JOIN 
    BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID
WHERE 
    cc.CONTACT_DICT_ID = 8
    AND ROWNUM <= 10
ORDER BY 
    cc.CUSTOMER_ID, cd.MNEMONIC;

PROMPT 
PROMPT ============================================================================
PROMPT Проверка: клиенты с счетами за текущий год и их адреса
PROMPT ============================================================================
SELECT 
    cc.CUSTOMER_ID,
    COALESCE(
        MAX(CASE WHEN cd.MNEMONIC = 'description' AND cc.CONTACT_DICT_ID = 23 THEN cc.VALUE END),
        TRIM(NVL(MAX(CASE WHEN cd.MNEMONIC = 'last_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' || 
             NVL(MAX(CASE WHEN cd.MNEMONIC = 'first_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' || 
             NVL(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), ''))
    ) AS CUSTOMER_NAME,
    MAX(CASE WHEN cd.MNEMONIC = 'juridic_address_id' AND cc.CONTACT_DICT_ID = 8 THEN cc.VALUE END) AS JURIDIC_ADDRESS,
    MAX(CASE WHEN cd.MNEMONIC = 'post_address_id' AND cc.CONTACT_DICT_ID = 8 THEN cc.VALUE END) AS POST_ADDRESS,
    MAX(CASE WHEN cd.MNEMONIC = 'home_address_id' AND cc.CONTACT_DICT_ID = 8 THEN cc.VALUE END) AS HOME_ADDRESS,
    COALESCE(
        MAX(CASE WHEN cd.MNEMONIC = 'juridic_address_id' AND cc.CONTACT_DICT_ID = 8 THEN cc.VALUE END),
        MAX(CASE WHEN cd.MNEMONIC = 'post_address_id' AND cc.CONTACT_DICT_ID = 8 THEN cc.VALUE END),
        MAX(CASE WHEN cd.MNEMONIC = 'home_address_id' AND cc.CONTACT_DICT_ID = 8 THEN cc.VALUE END)
    ) AS LEGAL_ADDRESS
FROM 
    BM_CUSTOMER_CONTACT cc
JOIN 
    BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID
WHERE 
    cc.CONTACT_DICT_ID IN (8, 11, 23)
    AND EXISTS (
        SELECT 1 
        FROM BM_INVOICE inv 
        WHERE inv.CUSTOMER_ID = cc.CUSTOMER_ID 
        AND TO_CHAR(inv.MOMENT, 'YYYY') = TO_CHAR(SYSDATE, 'YYYY')
        AND ROWNUM = 1
    )
    AND ROWNUM <= 10
GROUP BY 
    cc.CUSTOMER_ID
ORDER BY 
    CUSTOMER_NAME;

EXIT;
