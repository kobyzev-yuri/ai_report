-- ============================================================================
-- V_IRIDIUM_SERVICES_INFO
-- Информация о SBD сервисах Iridium с данными клиентов из биллинга
-- База данных: Oracle (billing7@bm7)
-- ============================================================================

SET SQLBLANKLINES ON
SET DEFINE OFF

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
    -- Название организации (для юр.лиц) - CONTACT_DICT_ID = 23
    MAX(CASE WHEN cd.MNEMONIC = 'description' AND cc.CONTACT_DICT_ID = 23 THEN cc.VALUE END) AS ORGANIZATION_NAME,
    -- ФИО для физ.лиц (собираем в одну строку) - CONTACT_DICT_ID = 11
    TRIM(
        NVL(MAX(CASE WHEN cd.MNEMONIC = 'last_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' ||
        NVL(MAX(CASE WHEN cd.MNEMONIC = 'first_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' ||
        NVL(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '')
    ) AS PERSON_NAME,
    -- Универсальное поле (название организации или ФИО)
    NVL(
        MAX(CASE WHEN cd.MNEMONIC = 'description' AND cc.CONTACT_DICT_ID = 23 THEN cc.VALUE END),
        TRIM(
            NVL(MAX(CASE WHEN cd.MNEMONIC = 'last_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' ||
            NVL(MAX(CASE WHEN cd.MNEMONIC = 'first_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' ||
            NVL(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '')
        )
    ) AS CUSTOMER_NAME,
    s.CREATE_DATE,
    s.START_DATE,  -- open_date: начало предоставления услуги
    s.STOP_DATE,   -- stop_date: конец предоставления услуги
    a.ACCOUNT_ID,
    -- Проверка наличия активной услуги приостановления (TYPE_ID = 9008)
    -- Услуги 9002 и 9008 в противофазе:
    -- - 9002 (SBD) - основной сервис, LOGIN = SUB-XXXXX (контракт)
    -- - 9008 (suspend) - услуга приостановления, LOGIN = IMEI (VSAT)
    -- - Обе связаны по VSAT (IMEI) и ACCOUNT_ID
    -- - Обе заканчиваются при stop_date (конец предоставления услуги)
    -- - Когда 9008 активна, 9002 приостановлена и наоборот
    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM SERVICES s_suspend 
            WHERE s_suspend.VSAT = s.VSAT  -- Связь по VSAT (IMEI)
              AND s_suspend.ACCOUNT_ID = s.ACCOUNT_ID  -- Связь по ACCOUNT_ID
              AND s_suspend.TYPE_ID = 9008  -- iridium_sbd_suspend
              AND s_suspend.STATUS = 10  -- активная услуга приостановления
              AND (s_suspend.STOP_DATE IS NULL OR s_suspend.STOP_DATE > SYSDATE)  -- не завершена
        ) THEN 'Y' 
        ELSE 'N' 
    END AS IS_SUSPENDED,
    -- CODE_1C из OUTER_IDS через подзапрос (более надежно, чем JOIN в GROUP BY)
    -- Используем подзапрос с ROWNUM = 1 на случай нескольких записей для одного CUSTOMER_ID
    -- ВАЖНО: TBL может быть в любом регистре, поэтому используем UPPER для сравнения
    (SELECT oi.EXT_ID 
     FROM (
       SELECT oi2.EXT_ID
       FROM OUTER_IDS oi2 
       WHERE oi2.ID = c.CUSTOMER_ID 
         AND UPPER(TRIM(oi2.TBL)) = 'CUSTOMERS'
       ORDER BY oi2.EXT_ID NULLS LAST
     ) oi
     WHERE ROWNUM = 1) AS CODE_1C
FROM SERVICES s
JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
JOIN CUSTOMERS c ON s.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN BM_CUSTOMER_CONTACT cc ON c.CUSTOMER_ID = cc.CUSTOMER_ID
LEFT JOIN BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID
    AND (
        (cd.MNEMONIC = 'description' AND cc.CONTACT_DICT_ID = 23)
        OR (cd.MNEMONIC IN ('first_name', 'last_name', 'middle_name') AND cc.CONTACT_DICT_ID = 11)
    )
WHERE s.TYPE_ID IN (9002, 9014)  -- SBD сервисы (9002 - тарификация по трафику) + услуги по сообщениям (9014 - в биллинге по сообщениям, у Iridium только по трафику)
  -- Примечание: TYPE_ID = 9008 (iridium_sbd_suspend) - услуга приостановления
  -- Услуги 9002 и 9008 в противофазе, связаны по VSAT (IMEI) и ACCOUNT_ID
  -- У 9008: LOGIN = IMEI (VSAT), у 9002: LOGIN = SUB-XXXXX (контракт)
  -- start_date (open_date) = начало предоставления услуги
  -- stop_date = конец предоставления услуги
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
    a.ACCOUNT_ID
/

-- Комментарии
COMMENT ON TABLE V_IRIDIUM_SERVICES_INFO IS 'Информация о SBD сервисах Iridium с данными клиентов. TYPE_ID: 9002 (трафик), 9014 (сообщения в биллинге, трафик у Iridium). Услуги 9002 и 9008 (suspend) в противофазе, связаны по VSAT (IMEI) и ACCOUNT_ID. У 9008 LOGIN=IMEI, у 9002 LOGIN=SUB-XXXXX. start_date (open_date) = начало предоставления услуги, stop_date = конец предоставления услуги'
/
COMMENT ON COLUMN V_IRIDIUM_SERVICES_INFO.CONTRACT_ID IS 'ID контракта (LOGIN) - связь с SPNET_TRAFFIC.CONTRACT_ID'
/
COMMENT ON COLUMN V_IRIDIUM_SERVICES_INFO.IMEI IS 'IMEI устройства (VSAT из SERVICES)'
/
COMMENT ON COLUMN V_IRIDIUM_SERVICES_INFO.AGREEMENT_NUMBER IS 'Номер договора в СТЭККОМ'
/
COMMENT ON COLUMN V_IRIDIUM_SERVICES_INFO.ORDER_NUMBER IS 'Номер заказа/приложения к договору'
/
COMMENT ON COLUMN V_IRIDIUM_SERVICES_INFO.CUSTOMER_NAME IS 'Название организации или ФИО'
/
COMMENT ON COLUMN V_IRIDIUM_SERVICES_INFO.CODE_1C IS 'Код клиента из системы 1С (из OUTER_IDS по CUSTOMER_ID)'
/
COMMENT ON COLUMN V_IRIDIUM_SERVICES_INFO.IS_SUSPENDED IS 'Признак приостановки: Y - есть активная услуга приостановления (TYPE_ID=9008), N - нет. Услуги 9002 и 9008 в противофазе, связаны по VSAT (IMEI) и ACCOUNT_ID. У 9008 LOGIN=IMEI, у 9002 LOGIN=SUB-XXXXX'
/
COMMENT ON COLUMN V_IRIDIUM_SERVICES_INFO.START_DATE IS 'Начало предоставления услуги (open_date)'
/
COMMENT ON COLUMN V_IRIDIUM_SERVICES_INFO.STOP_DATE IS 'Конец предоставления услуги (stop_date)'
/

SET DEFINE ON
