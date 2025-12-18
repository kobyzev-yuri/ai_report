-- ============================================================================
-- V_ANALYTICS_INVOICE_PERIOD
-- Отчет по счетам за период на основе таблицы ANALYTICS
-- Для биллинг операторов финансового отдела
-- База данных: Oracle (billing7@bm7)
-- ============================================================================

SET SQLBLANKLINES ON
SET DEFINE OFF

CREATE OR REPLACE VIEW V_ANALYTICS_INVOICE_PERIOD AS
WITH -- Фильтр коммерческих клиентов с кодом 1С (кроме 521 - СТЭККОМ)
commercial_customers AS (
    SELECT DISTINCT
        c.CUSTOMER_ID,
        oi.EXT_ID AS CODE_1C
    FROM CUSTOMERS c
    LEFT JOIN OUTER_IDS oi 
        ON oi.ID = c.CUSTOMER_ID
       AND UPPER(TRIM(oi.TBL)) = 'CUSTOMERS'
    WHERE oi.EXT_ID IS NOT NULL
      AND oi.EXT_ID != '521'  -- Исключаем СТЭККОМ
      AND LENGTH(TRIM(oi.EXT_ID)) > 0
),
-- Основные данные из ANALYTICS с иерархией клиент-договор-сервис
analytics_data AS (
    SELECT 
        a.AID,
        a.PERIOD_ID,
        a.SERVICE_ID,
        a.CUSTOMER_ID,
        a.ACCOUNT_ID,
        a.TYPE_ID,
        a.TARIFF_ID,
        a.TARIFFEL_ID,
        a.VSAT AS IMEI,
        a.MONEY,
        a.PRICE,
        a.TRAF,
        a.TOTAL_TRAF,
        a.ZONE_ID,
        a.RESOURCE_TYPE_ID,
        a.CLASS_ID,
        a.CLASS_NAME,
        a.INVOICE_ITEM_ID,
        a.FLAG,
        -- Связь с услугами для получения CONTRACT_ID
        s.LOGIN AS CONTRACT_ID,
        s.STATUS AS SERVICE_STATUS,
        s.START_DATE AS SERVICE_START_DATE,
        s.STOP_DATE AS SERVICE_STOP_DATE
    FROM ANALYTICS a
    JOIN SERVICES s ON a.SERVICE_ID = s.SERVICE_ID
    JOIN commercial_customers cc ON a.CUSTOMER_ID = cc.CUSTOMER_ID
    WHERE s.TYPE_ID IN (9002, 9005, 9008, 9013, 9014)  -- Iridium услуги
),
-- Информация о клиентах
customer_info AS (
    SELECT 
        c.CUSTOMER_ID,
        oi.EXT_ID AS CODE_1C,
        -- Получаем название организации из BM_CUSTOMER_CONTACT (CONTACT_DICT_ID = 23, MNEMONIC = 'description')
        MAX(CASE WHEN cd.MNEMONIC = 'description' AND cc.CONTACT_DICT_ID = 23 THEN cc.VALUE END) AS ORGANIZATION_NAME,
        -- Получаем ФИО физического лица из BM_CUSTOMER_CONTACT (CONTACT_DICT_ID = 11)
        TRIM(
            NVL(MAX(CASE WHEN cd.MNEMONIC = 'last_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' ||
            NVL(MAX(CASE WHEN cd.MNEMONIC = 'first_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' ||
            NVL(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '')
        ) AS PERSON_NAME,
        -- Общее название клиента: сначала организация, если нет - ФИО
        COALESCE(
            MAX(CASE WHEN cd.MNEMONIC = 'description' AND cc.CONTACT_DICT_ID = 23 THEN cc.VALUE END),
            TRIM(
                NVL(MAX(CASE WHEN cd.MNEMONIC = 'last_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' ||
                NVL(MAX(CASE WHEN cd.MNEMONIC = 'first_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' ||
                NVL(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '')
            )
        ) AS CUSTOMER_NAME
    FROM CUSTOMERS c
    LEFT JOIN OUTER_IDS oi 
        ON oi.ID = c.CUSTOMER_ID
       AND UPPER(TRIM(oi.TBL)) = 'CUSTOMERS'
    LEFT JOIN BM_CUSTOMER_CONTACT cc 
        ON cc.CUSTOMER_ID = c.CUSTOMER_ID
    LEFT JOIN BM_CONTACT_DICT cd 
        ON cd.CONTACT_DICT_ID = cc.CONTACT_DICT_ID
    GROUP BY c.CUSTOMER_ID, oi.EXT_ID
),
-- Информация о договорах (аккаунтах)
account_info AS (
    SELECT 
        acc.ACCOUNT_ID,
        acc.DESCRIPTION AS ACCOUNT_NAME,
        acc.CUSTOMER_ID AS ACCOUNT_CUSTOMER_ID
    FROM ACCOUNTS acc
),
-- Информация о тарифах
tariff_info AS (
    SELECT 
        t.TARIFF_ID,
        t.NAME AS TARIFF_NAME,
        t.DESCRIPTION AS TARIFF_DESCRIPTION
    FROM BM_TARIFF t
),
-- Информация о зонах
zone_info AS (
    SELECT 
        z.ZONE_ID,
        z.DESCRIPTION AS ZONE_NAME,
        z.DESCRIPTION AS ZONE_DESCRIPTION
    FROM BM_ZONE z
),
-- Информация о типах ресурсов (для расшифровки услуг)
resource_type_info AS (
    SELECT 
        rt.RESOURCE_TYPE_ID,
        rt.MNEMONIC,
        rt.NAME AS RESOURCE_TYPE_NAME,
        rt.NAME AS RESOURCE_TYPE_DESCRIPTION
    FROM BM_RESOURCE_TYPE rt
),
-- Информация о периодах
period_info AS (
    SELECT 
        p.PERIOD_ID,
        p.START_DATE,
        p.STOP_DATE AS END_DATE,
        TO_CHAR(p.START_DATE, 'YYYY-MM') AS PERIOD_YYYYMM,
        p.MONTH AS PERIOD_NAME
    FROM BM_PERIOD p
)
SELECT 
    -- Иерархия: Клиент
    ci.CUSTOMER_ID,
    ci.CODE_1C,
    ci.CUSTOMER_NAME,
    ci.ORGANIZATION_NAME,
    ci.PERSON_NAME,
    
    -- Иерархия: Договор (аккаунт)
    ad.ACCOUNT_ID,
    acc.ACCOUNT_NAME,
    
    -- Иерархия: Сервис
    ad.SERVICE_ID,
    ad.CONTRACT_ID,
    ad.IMEI,
    ad.SERVICE_STATUS,
    ad.SERVICE_START_DATE,
    ad.SERVICE_STOP_DATE,
    
    -- Период
    ad.PERIOD_ID,
    pi.PERIOD_YYYYMM,
    pi.PERIOD_NAME,
    pi.START_DATE AS PERIOD_START_DATE,
    pi.END_DATE AS PERIOD_END_DATE,
    
    -- Тариф и зона
    ad.TARIFF_ID,
    ti.TARIFF_NAME,
    ti.TARIFF_DESCRIPTION,
    ad.TARIFFEL_ID,
    ad.ZONE_ID,
    zi.ZONE_NAME,
    zi.ZONE_DESCRIPTION,
    
    -- Тип услуги и ресурса
    ad.TYPE_ID,
    ad.CLASS_ID,
    ad.CLASS_NAME,
    ad.RESOURCE_TYPE_ID,
    rt.MNEMONIC AS RESOURCE_MNEMONIC,
    rt.RESOURCE_TYPE_NAME,
    rt.RESOURCE_TYPE_DESCRIPTION,
    
    -- Деньги и трафик
    ad.MONEY,
    ad.PRICE,
    ad.TRAF,
    ad.TOTAL_TRAF,
    
    -- Разделение на абонплату и трафик по зонам
    CASE 
        WHEN rt.MNEMONIC IN ('fee_sbd', 'abon_payment', 'iridium_sbd_suspend', 
                            'iridium_monitoring', 'abo_gsm_block', 'fee_iridium_msg') 
        THEN ad.MONEY 
        ELSE 0 
    END AS MONEY_ABON,
    CASE 
        WHEN rt.MNEMONIC IN ('IRIDIUM_SBD', 'kb_traffic_pay', 'IRIDIUM_SBD_MBOX', 
                            'sbd_reg', 'woufzwv', 'll_traffic_pay') 
        THEN ad.MONEY 
        ELSE 0 
    END AS MONEY_TRAFFIC,
    
    -- Связь со счетами-фактурами
    ad.INVOICE_ITEM_ID,
    CASE WHEN ad.INVOICE_ITEM_ID IS NOT NULL THEN 'Y' ELSE 'N' END AS IN_INVOICE,
    ad.FLAG,
    
    -- Дополнительные поля
    ad.AID
FROM analytics_data ad
LEFT JOIN customer_info ci ON ad.CUSTOMER_ID = ci.CUSTOMER_ID
LEFT JOIN account_info acc ON ad.ACCOUNT_ID = acc.ACCOUNT_ID
LEFT JOIN tariff_info ti ON ad.TARIFF_ID = ti.TARIFF_ID
LEFT JOIN zone_info zi ON ad.ZONE_ID = zi.ZONE_ID
LEFT JOIN resource_type_info rt ON ad.RESOURCE_TYPE_ID = rt.RESOURCE_TYPE_ID
LEFT JOIN period_info pi ON ad.PERIOD_ID = pi.PERIOD_ID
/

COMMENT ON TABLE V_ANALYTICS_INVOICE_PERIOD IS 'Отчет по счетам за период на основе ANALYTICS. Иерархия: клиент (CUSTOMERS) -> договор (ACCOUNTS) -> сервис (SERVICES). Группировка по тарифам (TARIFF_ID) и зонам (ZONE_ID). Разделение на абонплату и трафик. Только коммерческие клиенты с кодом 1С (кроме 521 - СТЭККОМ).'
/

COMMENT ON COLUMN V_ANALYTICS_INVOICE_PERIOD.CUSTOMER_ID IS 'ID клиента из CUSTOMERS'
/
COMMENT ON COLUMN V_ANALYTICS_INVOICE_PERIOD.CODE_1C IS 'Код 1С клиента (только коммерческие, кроме 521)'
/
COMMENT ON COLUMN V_ANALYTICS_INVOICE_PERIOD.ACCOUNT_ID IS 'ID договора из ACCOUNTS (customer_id ref)'
/
COMMENT ON COLUMN V_ANALYTICS_INVOICE_PERIOD.SERVICE_ID IS 'ID услуги из SERVICES (ref account_id)'
/
COMMENT ON COLUMN V_ANALYTICS_INVOICE_PERIOD.TARIFF_ID IS 'ID тарифа из BM_TARIFF'
/
COMMENT ON COLUMN V_ANALYTICS_INVOICE_PERIOD.ZONE_ID IS 'ID зоны из BM_ZONE'
/
COMMENT ON COLUMN V_ANALYTICS_INVOICE_PERIOD.MONEY_ABON IS 'Сумма абонентской платы (по MNEMONIC)'
/
COMMENT ON COLUMN V_ANALYTICS_INVOICE_PERIOD.MONEY_TRAFFIC IS 'Сумма за трафик по зонам (по MNEMONIC)'
/
COMMENT ON COLUMN V_ANALYTICS_INVOICE_PERIOD.IN_INVOICE IS 'Попал ли в счет-фактуру (Y/N)'
/

SET DEFINE ON

