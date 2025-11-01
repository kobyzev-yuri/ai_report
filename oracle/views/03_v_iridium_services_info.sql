-- ============================================================================
-- V_IRIDIUM_SERVICES_INFO
-- Информация о SBD сервисах Iridium с данными клиентов из биллинга
-- База данных: Oracle (billing7@bm7)
-- ============================================================================

CREATE OR REPLACE VIEW V_IRIDIUM_SERVICES_INFO AS
SELECT 
    s.SERVICE_ID,
    s.LOGIN AS CONTRACT_ID,
    s.VSAT AS IMEI,
    s.TARIFF_ID,
    a.DESCRIPTION AS AGREEMENT_NUMBER,
    s.BLANK AS ORDER_NUMBER,
    s.STATUS,
    s.ACTUAL_STATUS,
    c.CUSTOMER_ID,
    -- Название организации (для юр.лиц)
    MAX(CASE WHEN cd.MNEMONIC = 'b_name' THEN cc.VALUE END) AS ORGANIZATION_NAME,
    -- ФИО для физ.лиц (собираем в одну строку)
    TRIM(
        NVL(MAX(CASE WHEN cd.MNEMONIC = 'last_name' THEN cc.VALUE END), '') || ' ' ||
        NVL(MAX(CASE WHEN cd.MNEMONIC = 'first_name' THEN cc.VALUE END), '') || ' ' ||
        NVL(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' THEN cc.VALUE END), '')
    ) AS PERSON_NAME,
    -- Универсальное поле (название организации или ФИО)
    NVL(
        MAX(CASE WHEN cd.MNEMONIC = 'b_name' THEN cc.VALUE END),
        TRIM(
            NVL(MAX(CASE WHEN cd.MNEMONIC = 'last_name' THEN cc.VALUE END), '') || ' ' ||
            NVL(MAX(CASE WHEN cd.MNEMONIC = 'first_name' THEN cc.VALUE END), '') || ' ' ||
            NVL(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' THEN cc.VALUE END), '')
        )
    ) AS CUSTOMER_NAME,
    s.CREATE_DATE,
    s.START_DATE,
    s.STOP_DATE,
    a.ACCOUNT_ID,
    -- CODE_1C из OUTER_IDS через подзапрос (более надежно, чем JOIN в GROUP BY)
    -- Используем ROWNUM = 1 на случай нескольких записей для одного CUSTOMER_ID
    (SELECT oi.EXT_ID 
     FROM OUTER_IDS oi 
     WHERE oi.ID = c.CUSTOMER_ID 
       AND oi.TBL = 'CUSTOMERS' 
       AND ROWNUM = 1) AS CODE_1C
FROM SERVICES s
JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
JOIN CUSTOMERS c ON s.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN BM_CUSTOMER_CONTACT cc ON c.CUSTOMER_ID = cc.CUSTOMER_ID
LEFT JOIN BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID
    AND cd.MNEMONIC IN ('b_name', 'first_name', 'last_name', 'middle_name')
WHERE s.TYPE_ID IN (9002, 9014)  -- SBD сервисы (9002 - тарификация по трафику) + услуги по сообщениям (9014 - в биллинге по сообщениям, у Iridium только по трафику)
GROUP BY 
    s.SERVICE_ID,
    s.LOGIN,
    s.VSAT,
    s.TARIFF_ID,
    a.DESCRIPTION,
    s.BLANK,
    s.STATUS,
    s.ACTUAL_STATUS,
    c.CUSTOMER_ID,
    s.CREATE_DATE,
    s.START_DATE,
    s.STOP_DATE,
    a.ACCOUNT_ID;

-- Комментарии
COMMENT ON TABLE V_IRIDIUM_SERVICES_INFO IS 'Информация о SBD сервисах Iridium с данными клиентов. TYPE_ID: 9002 (трафик), 9014 (сообщения в биллинге, трафик у Iridium)';
COMMENT ON COLUMN V_IRIDIUM_SERVICES_INFO.CONTRACT_ID IS 'ID контракта (LOGIN) - связь с SPNET_TRAFFIC.CONTRACT_ID';
COMMENT ON COLUMN V_IRIDIUM_SERVICES_INFO.IMEI IS 'IMEI устройства (VSAT из SERVICES)';
COMMENT ON COLUMN V_IRIDIUM_SERVICES_INFO.AGREEMENT_NUMBER IS 'Номер договора в СТЭККОМ';
COMMENT ON COLUMN V_IRIDIUM_SERVICES_INFO.ORDER_NUMBER IS 'Номер заказа/приложения к договору';
COMMENT ON COLUMN V_IRIDIUM_SERVICES_INFO.CUSTOMER_NAME IS 'Название организации или ФИО';
COMMENT ON COLUMN V_IRIDIUM_SERVICES_INFO.CODE_1C IS 'Код клиента из системы 1С (из OUTER_IDS по CUSTOMER_ID)';
