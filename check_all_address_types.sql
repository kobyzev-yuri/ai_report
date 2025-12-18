-- Проверка всех типов адресов в системе
SET PAGESIZE 1000
SET LINESIZE 200
SET FEEDBACK OFF

PROMPT ============================================================================
PROMPT Все типы контактов связанные с адресами
PROMPT ============================================================================
SELECT 
    CONTACT_DICT_ID,
    MNEMONIC,
    NAME,
    COUNT(*) AS USAGE_COUNT
FROM 
    BM_CONTACT_DICT cd
LEFT JOIN 
    BM_CUSTOMER_CONTACT cc ON cd.CONTACT_DICT_ID = cc.CONTACT_DICT_ID
WHERE 
    UPPER(cd.NAME) LIKE '%ADDRESS%' 
    OR UPPER(cd.NAME) LIKE '%АДРЕС%'
    OR UPPER(cd.MNEMONIC) LIKE '%ADDRESS%'
    OR UPPER(cd.MNEMONIC) LIKE '%ADDR%'
GROUP BY 
    CONTACT_DICT_ID, MNEMONIC, NAME
ORDER BY 
    CONTACT_DICT_ID, MNEMONIC;

PROMPT 
PROMPT ============================================================================
PROMPT Примеры адресов у клиентов
PROMPT ============================================================================
SELECT 
    cc.CUSTOMER_ID,
    cd.CONTACT_DICT_ID,
    cd.MNEMONIC,
    cd.NAME,
    SUBSTR(cc.VALUE, 1, 80) AS ADDRESS_VALUE
FROM 
    BM_CUSTOMER_CONTACT cc
JOIN 
    BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID
WHERE 
    (UPPER(cd.NAME) LIKE '%ADDRESS%' 
     OR UPPER(cd.NAME) LIKE '%АДРЕС%'
     OR UPPER(cd.MNEMONIC) LIKE '%ADDRESS%'
     OR UPPER(cd.MNEMONIC) LIKE '%ADDR%')
    AND cc.VALUE IS NOT NULL
    AND ROWNUM <= 20
ORDER BY 
    cc.CUSTOMER_ID, cd.CONTACT_DICT_ID;

EXIT;







