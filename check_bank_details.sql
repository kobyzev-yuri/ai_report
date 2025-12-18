-- Проверка мнемоник для банковских реквизитов
SET PAGESIZE 50
SET LINESIZE 200
SET FEEDBACK OFF

PROMPT ============================================================================
PROMPT Поиск мнемоник для банковских реквизитов в BM_CONTACT_DICT
PROMPT ============================================================================

-- Поиск по ключевым словам
SELECT 
    cd.CONTACT_DICT_ID,
    cd.MNEMONIC,
    cd.NAME,
    COUNT(cc.CUSTOMER_CONTACT_ID) AS USAGE_COUNT
FROM 
    BM_CONTACT_DICT cd
LEFT JOIN 
    BM_CUSTOMER_CONTACT cc ON cd.CONTACT_DICT_ID = cc.CONTACT_DICT_ID
WHERE 
    UPPER(cd.MNEMONIC) LIKE '%BANK%' 
    OR UPPER(cd.MNEMONIC) LIKE '%ACCOUNT%'
    OR UPPER(cd.MNEMONIC) LIKE '%RS%'
    OR UPPER(cd.MNEMONIC) LIKE '%KS%'
    OR UPPER(cd.MNEMONIC) LIKE '%BIK%'
    OR UPPER(cd.MNEMONIC) LIKE '%INN%'
    OR UPPER(cd.MNEMONIC) LIKE '%KPP%'
    OR UPPER(cd.NAME) LIKE '%БАНК%'
    OR UPPER(cd.NAME) LIKE '%СЧЕТ%'
    OR UPPER(cd.NAME) LIKE '%РЕКВИЗИТ%'
    OR UPPER(cd.NAME) LIKE '%РАСЧЕТН%'
    OR UPPER(cd.NAME) LIKE '%КОРРЕСПОНДЕНТ%'
GROUP BY 
    cd.CONTACT_DICT_ID, cd.MNEMONIC, cd.NAME
ORDER BY 
    cd.CONTACT_DICT_ID, cd.MNEMONIC;

PROMPT 
PROMPT ============================================================================
PROMPT Проверка примеров банковских реквизитов у клиентов
PROMPT ============================================================================

-- Проверяем первые 10 клиентов с банковскими реквизитами
SELECT 
    cc.CUSTOMER_ID,
    cd.CONTACT_DICT_ID,
    cd.MNEMONIC,
    cd.NAME,
    SUBSTR(cc.VALUE, 1, 100) AS VALUE_PREVIEW
FROM 
    BM_CUSTOMER_CONTACT cc
JOIN 
    BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID
WHERE 
    UPPER(cd.MNEMONIC) LIKE '%BANK%' 
    OR UPPER(cd.MNEMONIC) LIKE '%ACCOUNT%'
    OR UPPER(cd.MNEMONIC) LIKE '%RS%'
    OR UPPER(cd.MNEMONIC) LIKE '%KS%'
    OR UPPER(cd.MNEMONIC) LIKE '%BIK%'
    OR UPPER(cd.NAME) LIKE '%БАНК%'
    OR UPPER(cd.NAME) LIKE '%СЧЕТ%'
    OR UPPER(cd.NAME) LIKE '%РЕКВИЗИТ%'
    AND ROWNUM <= 10
ORDER BY 
    cc.CUSTOMER_ID, cd.CONTACT_DICT_ID;

EXIT;






