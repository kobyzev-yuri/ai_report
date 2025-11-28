-- ============================================================================
-- V_REVENUE_FROM_INVOICES
-- Отчет по доходам из счетов-фактур (BM_INVOICE_ITEM)
-- Группировка по базовому SUB- (без -clone-...) и периоду
-- Разделение доходов по типам услуг (SBD, SUSPEND, мониторинг, блокировка, сообщения)
-- с разделением на трафик и абонплату по BM_RESOURCE_TYPE
-- База данных: Oracle (billing7@bm7)
-- ============================================================================

SET SQLBLANKLINES ON
SET DEFINE OFF

CREATE OR REPLACE VIEW V_REVENUE_FROM_INVOICES AS
WITH -- Сначала находим все услуги SBD (9002) с SUB-XXXXX - это основная услуга
main_sbd_services AS (
    SELECT DISTINCT
        s.SERVICE_ID,
        REGEXP_REPLACE(s.LOGIN, '-clone-.*', '') AS BASE_CONTRACT_ID,
        s.VSAT AS IMEI,
        s.ACCOUNT_ID,
        s.CUSTOMER_ID
    FROM SERVICES s
    WHERE s.TYPE_ID = 9002  -- SBD услуга
      AND s.LOGIN LIKE 'SUB-%'
),
-- Теперь находим все invoice items для всех Iridium услуг
-- Для 9002: BASE_CONTRACT_ID = SUB-XXXXX
-- Для 9005, 9008, 9013: BASE_CONTRACT_ID берется из связанной услуги 9002 по VSAT=IMEI
-- Для 9014: BASE_CONTRACT_ID = SUB-XXXXX (если есть) или из связанной услуги 9002
base_contracts AS (
    SELECT DISTINCT
        ii.INVOICE_ITEM_ID,
        ii.SERVICE_ID,
        ii.PERIOD_ID,
        ii.MONEY,
        NVL(ii.MONEY_REVERSED, 0) AS MONEY_REVERSED,
        s.TYPE_ID,
        s.VSAT AS IMEI,
        s.ACCOUNT_ID,
        s.CUSTOMER_ID,
        rt.RESOURCE_TYPE_ID,
        rt.MNEMONIC AS RESOURCE_MNEMONIC,
        -- Валюта счета-фактуры (рубли) - для выставления счетов
        COALESCE(ii.CURRENCY_ID, i.CURRENCY_ID) AS CURRENCY_ID,
        -- Валюта лицевого счета (валюта учета договора) - для группировки
        COALESCE(ii.ACC_CURRENCY_ID, i.CURRENCY_ID) AS ACC_CURRENCY_ID,
        -- Суммы в валюте лицевого счета (валюта учета)
        ii.ACC_MONEY,
        -- BASE_CONTRACT_ID: для 9002 берем из LOGIN, для остальных - из связанной услуги 9002 по VSAT
        CASE 
            WHEN s.TYPE_ID = 9002 THEN
                REGEXP_REPLACE(COALESCE(ii.LOGIN, s.LOGIN), '-clone-.*', '')
            ELSE
                -- Для сопутствующих услуг (9005, 9008, 9013) ищем связанную услугу 9002 по VSAT
                (SELECT REGEXP_REPLACE(ms.BASE_CONTRACT_ID, '-clone-.*', '')
                 FROM main_sbd_services ms
                 WHERE ms.IMEI = s.VSAT
                   AND ms.ACCOUNT_ID = s.ACCOUNT_ID
                   AND ROWNUM = 1)
        END AS BASE_CONTRACT_ID
    FROM BM_INVOICE_ITEM ii
    JOIN BM_INVOICE i ON ii.INVOICE_ID = i.INVOICE_ID
    JOIN SERVICES s ON ii.SERVICE_ID = s.SERVICE_ID
    LEFT JOIN BM_RESOURCE_TYPE rt ON ii.RESOURCE_TYPE_ID = rt.RESOURCE_TYPE_ID
    WHERE s.TYPE_ID IN (9002, 9005, 9008, 9013, 9014)  -- Iridium услуги
      -- Для 9002: должен быть SUB-XXXXX
      -- Для 9005, 9008, 9013: связаны по VSAT с услугой 9002
      -- Для 9014: может быть SUB-XXXXX или связана по VSAT
      AND (
          -- Услуги SBD (9002) с SUB-XXXXX
          (s.TYPE_ID = 9002 AND (ii.LOGIN LIKE 'SUB-%' OR s.LOGIN LIKE 'SUB-%'))
          OR
          -- Сопутствующие услуги (9005, 9008, 9013) - связаны по VSAT с услугой 9002
          (s.TYPE_ID IN (9005, 9008, 9013) 
           AND EXISTS (
               SELECT 1 
               FROM main_sbd_services ms
               WHERE ms.IMEI = s.VSAT
                 AND ms.ACCOUNT_ID = s.ACCOUNT_ID
           ))
          OR
          -- Услуги сообщений (9014) - могут быть с SUB-XXXXX или связаны по VSAT
          (s.TYPE_ID = 9014 
           AND (
               (ii.LOGIN LIKE 'SUB-%' OR s.LOGIN LIKE 'SUB-%')
               OR EXISTS (
                   SELECT 1 
                   FROM main_sbd_services ms
                   WHERE ms.IMEI = s.VSAT
                     AND ms.ACCOUNT_ID = s.ACCOUNT_ID
               )
           ))
      )
),
-- Получаем данные клиентов из V_IRIDIUM_SERVICES_INFO
customer_info AS (
    SELECT DISTINCT
        CONTRACT_ID,
        CUSTOMER_NAME,
        CODE_1C,
        AGREEMENT_NUMBER,
        ORDER_NUMBER,
        SERVICE_ID AS INFO_SERVICE_ID
    FROM V_IRIDIUM_SERVICES_INFO
    WHERE CONTRACT_ID LIKE 'SUB-%'
),
-- Маппинг PERIOD_ID на BILL_MONTH через BM_PERIOD
periods_mapping AS (
    SELECT 
        PERIOD_ID,
        TO_NUMBER(TO_CHAR(START_DATE, 'YYYYMM')) AS BILL_MONTH_START,
        TO_NUMBER(TO_CHAR(STOP_DATE, 'YYYYMM')) AS BILL_MONTH_END,
        TO_CHAR(START_DATE, 'YYYY-MM') AS PERIOD_YYYYMM,
        MONTH AS PERIOD_MONTH_NAME
    FROM BM_PERIOD
),
-- Справочник валют (для валюты счета-фактуры - рубли)
currency_info AS (
    SELECT 
        CURRENCY_ID,
        NAME AS CURRENCY_NAME,
        CODE AS CURRENCY_CODE,
        MNEMONIC AS CURRENCY_MNEMONIC
    FROM BM_CURRENCY
    WHERE CURRENCY_ID IN (1, 4)  -- 1 = рубли, 4 = УЕ (доллары)
),
-- Справочник валют лицевого счета (валюта учета договора)
acc_currency_info AS (
    SELECT 
        CURRENCY_ID,
        NAME AS ACC_CURRENCY_NAME,
        CODE AS ACC_CURRENCY_CODE,
        MNEMONIC AS ACC_CURRENCY_MNEMONIC
    FROM BM_CURRENCY
    WHERE CURRENCY_ID IN (1, 4)  -- 1 = рубли, 4 = УЕ (доллары)
)
SELECT 
    -- Информационные колонки (как в затратах)
    ms.SERVICE_ID,
    bc.BASE_CONTRACT_ID AS CONTRACT_ID,
    bc.IMEI,
    ci.CUSTOMER_NAME,
    ci.CODE_1C,
    ms.ACCOUNT_ID,
    ms.CUSTOMER_ID,
    ci.AGREEMENT_NUMBER,
    ci.ORDER_NUMBER,
    
    -- Валюта счета-фактуры (рубли)
    bc.CURRENCY_ID,
    curr.CURRENCY_NAME,
    curr.CURRENCY_CODE,
    curr.CURRENCY_MNEMONIC,
    -- Валюта лицевого счета (валюта учета договора)
    bc.ACC_CURRENCY_ID,
    acc_curr.ACC_CURRENCY_NAME,
    acc_curr.ACC_CURRENCY_CODE,
    acc_curr.ACC_CURRENCY_MNEMONIC,
    
    -- Период
    bc.PERIOD_ID,
    pm.BILL_MONTH_START AS BILL_MONTH,
    pm.PERIOD_YYYYMM AS PERIOD_YYYYMM,
    pm.PERIOD_MONTH_NAME,
    
    -- Доходы SBD (9002) - трафик превышения (overage) (в рублях - валюта счета-фактуры)
    -- В счетах-фактурах показывается только трафик, превышающий включенный в абонплату
    SUM(CASE 
        WHEN bc.TYPE_ID = 9002 
        AND bc.RESOURCE_MNEMONIC IN ('IRIDIUM_SBD', 'kb_traffic_pay', 'IRIDIUM_SBD_MBOX', 'sbd_reg', 'woufzwv')
        THEN bc.MONEY - bc.MONEY_REVERSED
        ELSE 0
    END) AS REVENUE_SBD_TRAFFIC,
    
    -- Доходы SBD (9002) - абонплата (в рублях - валюта счета-фактуры)
    SUM(CASE 
        WHEN bc.TYPE_ID = 9002 
        AND bc.RESOURCE_MNEMONIC IN ('fee_sbd', 'abon_payment')
        THEN bc.MONEY - bc.MONEY_REVERSED
        ELSE 0
    END) AS REVENUE_SBD_ABON,
    
    -- Доходы SBD (9002) - всего (в рублях - валюта счета-фактуры)
    SUM(CASE 
        WHEN bc.TYPE_ID = 9002 
        THEN bc.MONEY - bc.MONEY_REVERSED
        ELSE 0
    END) AS REVENUE_SBD_TOTAL,
    
    -- Доходы SUSPEND (9008) - абонплата (в рублях - валюта счета-фактуры)
    SUM(CASE 
        WHEN bc.TYPE_ID = 9008 
        AND bc.RESOURCE_MNEMONIC = 'iridium_sbd_suspend'
        THEN bc.MONEY - bc.MONEY_REVERSED
        ELSE 0
    END) AS REVENUE_SUSPEND_ABON,
    
    -- Доходы мониторинга (9005) - абонплата (в рублях - валюта счета-фактуры)
    SUM(CASE 
        WHEN bc.TYPE_ID = 9005 
        AND bc.RESOURCE_MNEMONIC = 'iridium_monitoring'
        THEN bc.MONEY - bc.MONEY_REVERSED
        ELSE 0
    END) AS REVENUE_MONITORING_ABON,
    
    -- Доходы блокировки мониторинга (9013) - абонплата (в рублях - валюта счета-фактуры)
    SUM(CASE 
        WHEN bc.TYPE_ID = 9013 
        AND bc.RESOURCE_MNEMONIC = 'abo_gsm_block'
        THEN bc.MONEY - bc.MONEY_REVERSED
        ELSE 0
    END) AS REVENUE_MONITORING_BLOCK_ABON,
    
    -- Доходы сообщений (9014) - абонплата (в рублях - валюта счета-фактуры)
    -- Для сообщений трафик не практикуется - услуга блокируется при достижении включенного в абонплату трафика
    SUM(CASE 
        WHEN bc.TYPE_ID = 9014 
        AND bc.RESOURCE_MNEMONIC = 'fee_iridium_msg'
        THEN bc.MONEY - bc.MONEY_REVERSED
        ELSE 0
    END) AS REVENUE_MSG_ABON,
    
    -- Итого доходов (в рублях - валюта счета-фактуры)
    SUM(bc.MONEY - bc.MONEY_REVERSED) AS REVENUE_TOTAL,
    
    -- Опционально: суммы в валюте лицевого счета (для УЕ договоров, где ACC_CURRENCY_ID = 4)
    -- Используется только для справки, основная валюта - рубли (MONEY)
    -- Примечание: MONEY_REVERSED в рублях, поэтому для ACC_MONEY используем только положительные суммы
    SUM(CASE 
        WHEN bc.ACC_CURRENCY_ID = 4 THEN bc.ACC_MONEY 
        ELSE NULL 
    END) AS REVENUE_TOTAL_ACC_CURRENCY,
    
    -- Количество позиций в счетах-фактурах
    COUNT(DISTINCT bc.INVOICE_ITEM_ID) AS INVOICE_ITEMS_COUNT
    
FROM base_contracts bc
-- Джойним с главной услугой SBD для получения SERVICE_ID, IMEI, ACCOUNT_ID, CUSTOMER_ID
JOIN main_sbd_services ms 
    ON bc.BASE_CONTRACT_ID = ms.BASE_CONTRACT_ID
    AND bc.IMEI = ms.IMEI
    AND bc.ACCOUNT_ID = ms.ACCOUNT_ID
-- Джойним с данными клиентов
LEFT JOIN customer_info ci 
    ON bc.BASE_CONTRACT_ID = REGEXP_REPLACE(ci.CONTRACT_ID, '-clone-.*', '')
    AND ms.SERVICE_ID = ci.INFO_SERVICE_ID
-- Маппинг периодов
LEFT JOIN periods_mapping pm 
    ON bc.PERIOD_ID = pm.PERIOD_ID
-- Справочник валют счета-фактуры (рубли)
LEFT JOIN currency_info curr 
    ON bc.CURRENCY_ID = curr.CURRENCY_ID
-- Справочник валют лицевого счета (валюта учета договора)
LEFT JOIN acc_currency_info acc_curr 
    ON bc.ACC_CURRENCY_ID = acc_curr.CURRENCY_ID
GROUP BY 
    ms.SERVICE_ID,
    bc.BASE_CONTRACT_ID,
    bc.IMEI,
    ci.CUSTOMER_NAME,
    ci.CODE_1C,
    ms.ACCOUNT_ID,
    ms.CUSTOMER_ID,
    ci.AGREEMENT_NUMBER,
    ci.ORDER_NUMBER,
    bc.CURRENCY_ID,
    curr.CURRENCY_NAME,
    curr.CURRENCY_CODE,
    curr.CURRENCY_MNEMONIC,
    bc.ACC_CURRENCY_ID,
    acc_curr.ACC_CURRENCY_NAME,
    acc_curr.ACC_CURRENCY_CODE,
    acc_curr.ACC_CURRENCY_MNEMONIC,
    bc.PERIOD_ID,
    pm.BILL_MONTH_START,
    pm.PERIOD_YYYYMM,
    pm.PERIOD_MONTH_NAME
ORDER BY 
    bc.BASE_CONTRACT_ID,
    bc.PERIOD_ID NULLS LAST
/

-- Комментарии
COMMENT ON TABLE V_REVENUE_FROM_INVOICES IS 'Отчет по доходам из счетов-фактур (BM_INVOICE_ITEM). Группировка по базовому SUB- (без -clone-...) и периоду. Разделение доходов по типам услуг (SBD, SUSPEND, мониторинг, блокировка, сообщения) с разделением на трафик и абонплату. Одна строка на SUB- + период. Сопутствующие услуги (9005, 9008, 9013) связаны с основной услугой 9002 по VSAT=IMEI.'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.CONTRACT_ID IS 'Базовый SUB-XXXXX (без -clone-...) - ключевая связь для сопоставления с затратами'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.SERVICE_ID IS 'SERVICE_ID главной услуги SBD (TYPE_ID = 9002)'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.REVENUE_SBD_TRAFFIC IS 'Доходы от трафика превышения SBD (IRIDIUM_SBD, kb_traffic_pay, IRIDIUM_SBD_MBOX, sbd_reg, woufzwv). В счетах-фактурах показывается только трафик, превышающий включенный в абонплату (overage)'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.REVENUE_SBD_ABON IS 'Доходы от абонплаты SBD (fee_sbd, abon_payment)'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.REVENUE_SUSPEND_ABON IS 'Доходы от абонплаты приостановки SBD (iridium_sbd_suspend). Услуга 9008 связана с основной услугой 9002 по VSAT=IMEI.'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.REVENUE_MONITORING_ABON IS 'Доходы от абонплаты мониторинга (iridium_monitoring). Услуга 9005 связана с основной услугой 9002 по VSAT=IMEI.'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.REVENUE_MONITORING_BLOCK_ABON IS 'Доходы от абонплаты блокировки мониторинга (abo_gsm_block). Услуга 9013 связана с основной услугой 9002 по VSAT=IMEI.'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.REVENUE_MSG_ABON IS 'Доходы от абонплаты сообщений (fee_iridium_msg). Для сообщений трафик не практикуется - услуга блокируется при достижении включенного в абонплату трафика'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.REVENUE_TOTAL IS 'Итого доходов (сумма всех типов услуг) в рублях (MONEY) - основная валюта для всех договоров'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.REVENUE_TOTAL_ACC_CURRENCY IS 'Опционально: итого доходов в валюте лицевого счета (ACC_MONEY) - только для УЕ договоров (ACC_CURRENCY_ID = 4), используется для справки. Основная валюта - рубли (REVENUE_TOTAL)'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.BILL_MONTH IS 'Месяц биллинга (YYYYMM) для сопоставления с затратами'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.PERIOD_YYYYMM IS 'Финансовый период в формате YYYY-MM (например, 2025-02) для фильтрации и сопоставления с FINANCIAL_PERIOD из затрат'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.PERIOD_MONTH_NAME IS 'Название месяца из BM_PERIOD (например, Фев, Мар)'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.CURRENCY_ID IS 'ID валюты счета-фактуры (рубли) - всегда рубли для выставления счетов'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.CURRENCY_NAME IS 'Название валюты счета-фактуры из BM_CURRENCY (обычно Российский рубль)'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.CURRENCY_CODE IS 'Код валюты счета-фактуры из BM_CURRENCY (810 = рубли)'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.CURRENCY_MNEMONIC IS 'Мнемоника валюты счета-фактуры из BM_CURRENCY (RUR)'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.ACC_CURRENCY_ID IS 'ID валюты лицевого счета (валюта учета договора): 1 = рубли, 4 = УЕ (доллары по курсу ЦБРФ на последний день месяца выставления счета). ВАЖНО: Все суммы (REVENUE_*) показываются в рублях (CURRENCY_ID), но группировка идет по ACC_CURRENCY_ID'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.ACC_CURRENCY_NAME IS 'Название валюты лицевого счета из BM_CURRENCY (Российский рубль или Доллары США)'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.ACC_CURRENCY_CODE IS 'Код валюты лицевого счета из BM_CURRENCY (810 = рубли, 840 = доллары)'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.ACC_CURRENCY_MNEMONIC IS 'Мнемоника валюты лицевого счета из BM_CURRENCY (RUR, USD)'
/
