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
WITH -- Главная услуга: SBD (9002) или Stectrace (9014) с SUB-XXXXX; плюс «осиротевшие» 9008 / 9004,9010,… когда в СФ есть только они (часто счёт из 1С без полного набора позиций биллинга)
main_sbd_services AS (
    -- 9002/9014: ключ строки отчёта — VSAT (IMEI); LOGIN для BASE_CONTRACT_ID = SUB-
    SELECT DISTINCT
        s.SERVICE_ID,
        REGEXP_REPLACE(s.LOGIN, '-clone-.*', '') AS BASE_CONTRACT_ID,
        TRIM(TO_CHAR(s.VSAT)) AS IMEI,
        s.ACCOUNT_ID,
        s.CUSTOMER_ID
    FROM SERVICES s
    WHERE s.TYPE_ID IN (9002, 9014)
      AND s.LOGIN LIKE 'SUB-%'
    UNION ALL
    -- Приостановка (9008): когда по IMEI+ACCOUNT нет главной 9002/9014, строка в отчёте по IMEI всё равно нужна (REVENUE_SUSPEND_ABON)
    -- Ключ устройства: VSAT, иначе LOGIN (у части записей VSAT пустой, IMEI только в LOGIN)
    SELECT DISTINCT
        s.SERVICE_ID,
        NVL(NULLIF(TRIM(TO_CHAR(s.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s.LOGIN)), '')) AS BASE_CONTRACT_ID,
        NVL(NULLIF(TRIM(TO_CHAR(s.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s.LOGIN)), '')) AS IMEI,
        s.ACCOUNT_ID,
        s.CUSTOMER_ID
    FROM SERVICES s
    WHERE s.TYPE_ID = 9008
      AND NVL(NULLIF(TRIM(TO_CHAR(s.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s.LOGIN)), '')) IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM SERVICES m
          WHERE m.TYPE_ID IN (9002, 9014)
            AND m.ACCOUNT_ID = s.ACCOUNT_ID
            AND TRIM(TO_CHAR(m.VSAT)) = NVL(NULLIF(TRIM(TO_CHAR(s.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s.LOGIN)), ''))
      )
    UNION ALL
    -- Мониторинг/трекинг/блокировка (9004, 9005, 9010, 9013): нет 9002/9014 и нет 9008 по IMEI+ЛС — якорь для позиций СФ только по сопутствующим (типично 9010 или 9004)
    SELECT x.SERVICE_ID, x.BASE_CONTRACT_ID, x.IMEI, x.ACCOUNT_ID, x.CUSTOMER_ID
    FROM (
        SELECT
            s.SERVICE_ID,
            NVL(NULLIF(TRIM(TO_CHAR(s.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s.LOGIN)), '')) AS BASE_CONTRACT_ID,
            NVL(NULLIF(TRIM(TO_CHAR(s.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s.LOGIN)), '')) AS IMEI,
            s.ACCOUNT_ID,
            s.CUSTOMER_ID,
            ROW_NUMBER() OVER (
                PARTITION BY NVL(NULLIF(TRIM(TO_CHAR(s.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s.LOGIN)), '')), s.ACCOUNT_ID
                ORDER BY
                    CASE s.TYPE_ID
                        WHEN 9010 THEN 1
                        WHEN 9004 THEN 2
                        WHEN 9005 THEN 3
                        WHEN 9013 THEN 4
                        ELSE 5
                    END,
                    s.SERVICE_ID
            ) AS rn
        FROM SERVICES s
        WHERE s.TYPE_ID IN (9004, 9005, 9010, 9013)
          AND NVL(NULLIF(TRIM(TO_CHAR(s.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s.LOGIN)), '')) IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM SERVICES m
              WHERE m.TYPE_ID IN (9002, 9014)
                AND m.ACCOUNT_ID = s.ACCOUNT_ID
                AND TRIM(TO_CHAR(m.VSAT)) = NVL(NULLIF(TRIM(TO_CHAR(s.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s.LOGIN)), ''))
          )
          AND NOT EXISTS (
              SELECT 1 FROM SERVICES m
              WHERE m.TYPE_ID = 9008
                AND m.ACCOUNT_ID = s.ACCOUNT_ID
                AND NVL(NULLIF(TRIM(TO_CHAR(m.VSAT)), ''), NULLIF(TRIM(TO_CHAR(m.LOGIN)), ''))
                    = NVL(NULLIF(TRIM(TO_CHAR(s.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s.LOGIN)), ''))
          )
    ) x
    WHERE x.rn = 1
),
-- Якоря для JOIN: main_sbd_services + услуги сопутствующих по факту СФ за период, если в этом периоде нет строк 9002/9014 с тем же IMEI+ЛС
-- Один якорь на (BASE_CONTRACT_ID, IMEI, ACCOUNT_ID): при конфликте 9008 vs 9010 приоритет у 9010/9004/9005/9013 (строка СФ)
revenue_service_anchors AS (
    SELECT SERVICE_ID, BASE_CONTRACT_ID, IMEI, ACCOUNT_ID, CUSTOMER_ID
    FROM (
        SELECT
            x.SERVICE_ID,
            x.BASE_CONTRACT_ID,
            x.IMEI,
            x.ACCOUNT_ID,
            x.CUSTOMER_ID,
            ROW_NUMBER() OVER (
                PARTITION BY x.BASE_CONTRACT_ID, x.IMEI, x.ACCOUNT_ID
                ORDER BY
                    CASE svc.TYPE_ID
                        WHEN 9010 THEN 1
                        WHEN 9004 THEN 2
                        WHEN 9005 THEN 3
                        WHEN 9013 THEN 4
                        WHEN 9008 THEN 5
                        WHEN 9002 THEN 6
                        WHEN 9014 THEN 7
                        ELSE 9
                    END,
                    x.SERVICE_ID
            ) AS rn
        FROM (
            SELECT SERVICE_ID, BASE_CONTRACT_ID, IMEI, ACCOUNT_ID, CUSTOMER_ID FROM main_sbd_services
            UNION ALL
            SELECT DISTINCT
                s.SERVICE_ID,
                NVL(NULLIF(TRIM(TO_CHAR(s.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s.LOGIN)), '')) AS BASE_CONTRACT_ID,
                NVL(NULLIF(TRIM(TO_CHAR(s.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s.LOGIN)), '')) AS IMEI,
                s.ACCOUNT_ID,
                s.CUSTOMER_ID
            FROM BM_INVOICE_ITEM ii
            JOIN SERVICES s ON ii.SERVICE_ID = s.SERVICE_ID
            WHERE s.TYPE_ID IN (9004, 9005, 9008, 9010, 9013)
              AND NVL(NULLIF(TRIM(TO_CHAR(s.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s.LOGIN)), '')) IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM BM_INVOICE_ITEM ii_m
                  JOIN SERVICES s_m ON ii_m.SERVICE_ID = s_m.SERVICE_ID
                  WHERE ii_m.PERIOD_ID = ii.PERIOD_ID
                    AND ii_m.ACCOUNT_ID = s.ACCOUNT_ID
                    AND s_m.TYPE_ID IN (9002, 9014)
                    AND NVL(NULLIF(TRIM(TO_CHAR(s_m.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s_m.LOGIN)), ''))
                        = NVL(NULLIF(TRIM(TO_CHAR(s.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s.LOGIN)), ''))
              )
        ) x
        JOIN SERVICES svc ON x.SERVICE_ID = svc.SERVICE_ID
    )
    WHERE rn = 1
),
-- Теперь находим все invoice items для всех Iridium услуг
-- Для 9002/9014: BASE_CONTRACT_ID = SUB-XXXXX
-- Для 9004, 9005, 9008, 9010, 9013: если в СФ за период есть 9002/9014 с тем же IMEI+ЛС — BASE = SUB- из main_sbd; иначе BASE = IMEI и якорь — сама услуга строки СФ (9010 и т.д.)
base_contracts AS (
    SELECT DISTINCT
        ii.INVOICE_ITEM_ID,
        ii.SERVICE_ID,
        ii.PERIOD_ID,
        ii.MONEY,
        NVL(ii.MONEY_REVERSED, 0) AS MONEY_REVERSED,
        s.TYPE_ID,
        CASE
            WHEN s.TYPE_ID IN (9004, 9005, 9008, 9010, 9013) THEN
                NVL(NULLIF(TRIM(TO_CHAR(s.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s.LOGIN)), ''))
            ELSE
                TRIM(TO_CHAR(s.VSAT))
        END AS IMEI,
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
        -- BASE_CONTRACT_ID: 9002/9014 — из LOGIN; сопутствующие — SUB- только если в этом PERIOD_ID в СФ уже есть 9002/9014 с тем же IMEI+ЛС
        CASE 
            WHEN s.TYPE_ID IN (9002, 9014) THEN
                REGEXP_REPLACE(COALESCE(ii.LOGIN, s.LOGIN), '-clone-.*', '')
            WHEN s.TYPE_ID IN (9004, 9005, 9008, 9010, 9013)
                 AND NOT EXISTS (
                     SELECT 1
                     FROM BM_INVOICE_ITEM ii_m
                     JOIN SERVICES s_m ON ii_m.SERVICE_ID = s_m.SERVICE_ID
                     WHERE ii_m.PERIOD_ID = ii.PERIOD_ID
                       AND ii_m.ACCOUNT_ID = s.ACCOUNT_ID
                       AND s_m.TYPE_ID IN (9002, 9014)
                       AND NVL(NULLIF(TRIM(TO_CHAR(s_m.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s_m.LOGIN)), ''))
                           = NVL(NULLIF(TRIM(TO_CHAR(s.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s.LOGIN)), ''))
                 )
            THEN
                NVL(NULLIF(TRIM(TO_CHAR(s.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s.LOGIN)), ''))
            ELSE
                (SELECT REGEXP_REPLACE(ms.BASE_CONTRACT_ID, '-clone-.*', '')
                 FROM main_sbd_services ms
                 WHERE ms.ACCOUNT_ID = s.ACCOUNT_ID
                   AND ms.IMEI = CASE
                       WHEN s.TYPE_ID IN (9004, 9005, 9008, 9010, 9013) THEN
                           NVL(NULLIF(TRIM(TO_CHAR(s.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s.LOGIN)), ''))
                       ELSE
                           TRIM(TO_CHAR(s.VSAT))
                   END
                   AND ROWNUM = 1)
        END AS BASE_CONTRACT_ID
    FROM BM_INVOICE_ITEM ii
    JOIN BM_INVOICE i ON ii.INVOICE_ID = i.INVOICE_ID
    JOIN SERVICES s ON ii.SERVICE_ID = s.SERVICE_ID
    LEFT JOIN BM_RESOURCE_TYPE rt ON ii.RESOURCE_TYPE_ID = rt.RESOURCE_TYPE_ID
    WHERE s.TYPE_ID IN (9002, 9004, 9005, 9008, 9010, 9013, 9014)  -- Iridium / сопутствующие SBD
      -- Для 9002: должен быть SUB-XXXXX
      -- Для 9004, 9005, 9008, 9010, 9013: связаны по VSAT с главной 9002/9014 (не основные, но начисления при активном SBD возможны)
      -- Для 9014: может быть SUB-XXXXX или связана по VSAT
      AND (
          -- Главные услуги SBD (9002) и Stectrace (9014) с SUB-XXXXX
          (s.TYPE_ID IN (9002, 9014) AND (ii.LOGIN LIKE 'SUB-%' OR s.LOGIN LIKE 'SUB-%'))
          OR
          -- Сопутствующие (9004 трекинг, 9005 мониторинг, 9008 suspend, 9010 GSM-мониторинг, 9013 блокировка) — по VSAT с главной (9002 или 9014)
          (s.TYPE_ID IN (9004, 9005, 9008, 9010, 9013) 
           AND EXISTS (
               SELECT 1 
               FROM main_sbd_services ms
               WHERE ms.ACCOUNT_ID = s.ACCOUNT_ID
                 AND ms.IMEI = NVL(NULLIF(TRIM(TO_CHAR(s.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s.LOGIN)), ''))
           ))
      )
),
-- Получаем данные клиентов из V_IRIDIUM_SERVICES_INFO
customer_info_raw AS (
    SELECT DISTINCT
        CONTRACT_ID,
        IMEI,
        ACCOUNT_ID,
        CUSTOMER_ID,
        CUSTOMER_NAME,
        ORGANIZATION_NAME,
        PERSON_NAME,
        CODE_1C,
        AGREEMENT_NUMBER,
        ORDER_NUMBER,
        SERVICE_ID AS INFO_SERVICE_ID,
        TARIFF_ID,
        IS_SUSPENDED,
        START_DATE,
        STOP_DATE
    FROM V_IRIDIUM_SERVICES_INFO
    WHERE CONTRACT_ID LIKE 'SUB-%'
),
customer_info_sub AS (
    SELECT
        CONTRACT_ID,
        IMEI,
        ACCOUNT_ID,
        CUSTOMER_ID,
        CUSTOMER_NAME,
        ORGANIZATION_NAME,
        PERSON_NAME,
        CODE_1C,
        AGREEMENT_NUMBER,
        ORDER_NUMBER,
        INFO_SERVICE_ID,
        TARIFF_ID,
        IS_SUSPENDED,
        START_DATE,
        STOP_DATE
    FROM (
        SELECT
            r.*,
            ROW_NUMBER() OVER (
                PARTITION BY r.CONTRACT_ID
                ORDER BY r.START_DATE DESC NULLS LAST, r.STOP_DATE DESC NULLS LAST, r.INFO_SERVICE_ID DESC
            ) AS rn
        FROM customer_info_raw r
    )
    WHERE rn = 1
),
customer_info_imei AS (
    SELECT
        CONTRACT_ID,
        IMEI,
        ACCOUNT_ID,
        CUSTOMER_ID,
        CUSTOMER_NAME,
        ORGANIZATION_NAME,
        PERSON_NAME,
        CODE_1C,
        AGREEMENT_NUMBER,
        ORDER_NUMBER,
        INFO_SERVICE_ID,
        TARIFF_ID,
        IS_SUSPENDED,
        START_DATE,
        STOP_DATE
    FROM (
        SELECT
            r.*,
            ROW_NUMBER() OVER (
                PARTITION BY r.ACCOUNT_ID, TRIM(TO_CHAR(r.IMEI))
                ORDER BY r.START_DATE DESC NULLS LAST, r.STOP_DATE DESC NULLS LAST, r.INFO_SERVICE_ID DESC
            ) AS rn
        FROM customer_info_raw r
        WHERE r.IMEI IS NOT NULL
    )
    WHERE rn = 1
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
),
-- Информация о тарифных планах для разделения трафика превышения на SBD-1 и SBD-10
-- Связываем через CONTRACT_ID, IMEI и период
plan_info AS (
    SELECT DISTINCT
        cor.CONTRACT_ID,
        cor.IMEI,
        -- Используем BILL_MONTH_YYYMM для связи с периодами доходов
        -- BILL_MONTH_YYYMM в формате YYYYMM (число), PERIOD_YYYYMM в формате YYYY-MM (строка)
        TO_CHAR(cor.BILL_MONTH_YYYMM) AS BILL_MONTH_STR,
        TO_CHAR(TO_DATE(SUBSTR(TO_CHAR(cor.BILL_MONTH_YYYMM), 1, 6), 'YYYYMM'), 'YYYY-MM') AS PERIOD_YYYYMM,
        cor.PLAN_NAME,
        -- Определяем PLAN_CODE из PLAN_NAME
        CASE 
            WHEN cor.PLAN_NAME = 'SBD Tiered 1250 1K' THEN 'SBD-1'
            WHEN cor.PLAN_NAME = 'SBD Tiered 1250 10K' THEN 'SBD-10'
            ELSE NULL
        END AS PLAN_CODE
    FROM V_CONSOLIDATED_REPORT_WITH_BILLING cor
    WHERE cor.PLAN_NAME IN ('SBD Tiered 1250 1K', 'SBD Tiered 1250 10K')
      AND cor.CONTRACT_ID IS NOT NULL
      AND cor.IMEI IS NOT NULL
      AND cor.BILL_MONTH_YYYMM IS NOT NULL
),
-- Разовый платёж «Подключение устройства» (single_payment) по тарифу услуги 9002/9014:
-- BM_TARIFFEL_TYPE (MNEMONIC=single_payment, TYPE_ID = услуге) + BM_TARIFFEL.MONEY при TARIFF_ID услуги
tariff_single_payment AS (
    SELECT
        s.SERVICE_ID,
        MAX(tf.MONEY) AS TARIFF_SINGLE_PAYMENT_MONEY
    FROM SERVICES s
    INNER JOIN BM_TARIFFEL tf ON tf.TARIFF_ID = s.TARIFF_ID
    INNER JOIN BM_TARIFFEL_TYPE tyt ON tf.TARIFFEL_TYPE_ID = tyt.TARIFFEL_TYPE_ID
    WHERE s.TYPE_ID IN (9002, 9014)
      AND tyt.MNEMONIC = 'single_payment'
      AND tyt.TYPE_ID = s.TYPE_ID
      AND NVL(tf.MONEY, 0) > 0
    GROUP BY s.SERVICE_ID
)
SELECT 
    -- Информационные колонки (как в затратах)
    ms.SERVICE_ID,
    bc.BASE_CONTRACT_ID AS CONTRACT_ID,
    bc.IMEI,
    -- SUB-контракт (если строка привязана к IMEI, а не к SUB-)
    COALESCE(ci_sub.CONTRACT_ID, ci_imei.CONTRACT_ID) AS SUB_CONTRACT_ID,
    COALESCE(ci_sub.CUSTOMER_NAME, ci_imei.CUSTOMER_NAME) AS CUSTOMER_NAME,
    COALESCE(ci_sub.ORGANIZATION_NAME, ci_imei.ORGANIZATION_NAME) AS ORGANIZATION_NAME,
    COALESCE(ci_sub.PERSON_NAME, ci_imei.PERSON_NAME) AS PERSON_NAME,
    COALESCE(ci_sub.CODE_1C, ci_imei.CODE_1C) AS CODE_1C,
    ms.ACCOUNT_ID,
    ms.CUSTOMER_ID,
    COALESCE(ci_sub.AGREEMENT_NUMBER, ci_imei.AGREEMENT_NUMBER) AS AGREEMENT_NUMBER,
    COALESCE(ci_sub.ORDER_NUMBER, ci_imei.ORDER_NUMBER) AS ORDER_NUMBER,
    COALESCE(ci_sub.INFO_SERVICE_ID, ci_imei.INFO_SERVICE_ID) AS INFO_SERVICE_ID,
    COALESCE(ci_sub.TARIFF_ID, ci_imei.TARIFF_ID) AS TARIFF_ID,
    COALESCE(ci_sub.IS_SUSPENDED, ci_imei.IS_SUSPENDED) AS IS_SUSPENDED,
    COALESCE(ci_sub.START_DATE, ci_imei.START_DATE) AS SERVICE_START_DATE,
    COALESCE(ci_sub.STOP_DATE, ci_imei.STOP_DATE) AS SERVICE_STOP_DATE,
    
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
    
    -- Доходы SBD (9002) - трафик превышения для SBD-1 (в рублях - валюта счета-фактуры)
    SUM(CASE 
        WHEN bc.TYPE_ID = 9002 
        AND bc.RESOURCE_MNEMONIC IN ('IRIDIUM_SBD', 'kb_traffic_pay', 'IRIDIUM_SBD_MBOX', 'sbd_reg', 'woufzwv')
        AND pi.PLAN_CODE = 'SBD-1'
        THEN bc.MONEY - bc.MONEY_REVERSED
        ELSE 0
    END) AS REVENUE_SBD_TRAFFIC_SBD1,
    
    -- Доходы SBD (9002) - трафик превышения для SBD-10 (в рублях - валюта счета-фактуры)
    SUM(CASE 
        WHEN bc.TYPE_ID = 9002 
        AND bc.RESOURCE_MNEMONIC IN ('IRIDIUM_SBD', 'kb_traffic_pay', 'IRIDIUM_SBD_MBOX', 'sbd_reg', 'woufzwv')
        AND pi.PLAN_CODE = 'SBD-10'
        THEN bc.MONEY - bc.MONEY_REVERSED
        ELSE 0
    END) AS REVENUE_SBD_TRAFFIC_SBD10,
    
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
    
    -- Доходы мониторинга (9004 трекинг, 9005 мониторинг, 9010 GSM-мониторинг)
    -- Суммируем в одну колонку REVENUE_MONITORING_ABON без разбиения по подтипам.
    SUM(CASE
        WHEN bc.TYPE_ID IN (9004, 9005, 9010)
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

    -- Справочно: разовый платёж подключения из тарифного плана (не из СФ; BM_TARIFFEL по single_payment)
    MAX(tsp.TARIFF_SINGLE_PAYMENT_MONEY) AS TARIFF_SINGLE_PAYMENT_MONEY,
    
    -- Опционально: суммы в валюте лицевого счета (для УЕ договоров, где ACC_CURRENCY_ID = 4)
    -- Используется только для справки, основная валюта - рубли (MONEY)
    -- Примечание: MONEY_REVERSED в рублях, поэтому для ACC_MONEY используем только положительные суммы
    SUM(CASE 
        WHEN bc.ACC_CURRENCY_ID = 4 THEN bc.ACC_MONEY 
        ELSE NULL 
    END) AS REVENUE_TOTAL_ACC_CURRENCY,
    
    -- Количество позиций в счетах-фактурах
    COUNT(DISTINCT bc.INVOICE_ITEM_ID) AS INVOICE_ITEMS_COUNT,
    
    -- Пояснение для нештатных строк (нет 9002/9014 в биллинге по IMEI+ЛС); NULL — обычный случай
    MAX(
        CASE
            WHEN sa.TYPE_ID IN (9002, 9014) THEN NULL
            WHEN sa.TYPE_ID = 9008 THEN
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM SERVICES s_m
                        WHERE s_m.TYPE_ID IN (9002, 9014)
                          AND s_m.ACCOUNT_ID = ms.ACCOUNT_ID
                          AND TRIM(TO_CHAR(s_m.VSAT)) = bc.IMEI
                    ) THEN
                        'СФ содержит приостановку 9008 без 9002/9014 в этом периоде (возможен штатный случай при приостановке). CONTRACT_ID=IMEI.'
                    ELSE
                        'Нестандартно: в SERVICES нет 9002/9014 по IMEI+лицевому счёту; строка привязана к 9008, CONTRACT_ID=IMEI. Проверить полноту счёта-фактуры (в т.ч. выставление из 1С).'
                END
            WHEN sa.TYPE_ID IN (9004, 9005, 9010, 9013) THEN
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM SERVICES s_m
                        WHERE s_m.TYPE_ID IN (9002, 9014, 9008)
                          AND s_m.ACCOUNT_ID = ms.ACCOUNT_ID
                          AND NVL(NULLIF(TRIM(TO_CHAR(s_m.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s_m.LOGIN)), '')) = bc.IMEI
                    ) THEN
                        'СФ содержит сопутствующую услугу TYPE_ID=' || TO_CHAR(sa.TYPE_ID)
                        || ' без 9002/9014 в этом периоде (возможен штатный случай). CONTRACT_ID=IMEI.'
                    ELSE
                        'Нестандартно: в SERVICES нет 9002/9014/9008 по IMEI+лицевому счёту; строка привязана к сопутствующей услуге TYPE_ID=' || TO_CHAR(sa.TYPE_ID)
                        || ' (напр. 9010 GSM-мониторинг, 9004 трекинг), CONTRACT_ID=IMEI. Рекомендуется сверить с биллингом и источником СФ.'
                END
            ELSE NULL
        END
    ) AS REVENUE_ANOMALY_NOTE
    
FROM base_contracts bc
-- Джойним с якорем: SUB+9002/9014, осиротевшие в SERVICES, либо якорь по факту СФ (9010 без 9002 в СФ за период)
JOIN revenue_service_anchors ms 
    ON bc.BASE_CONTRACT_ID = ms.BASE_CONTRACT_ID
    AND bc.IMEI = ms.IMEI
    AND bc.ACCOUNT_ID = ms.ACCOUNT_ID
JOIN SERVICES sa ON ms.SERVICE_ID = sa.SERVICE_ID
-- Джойним с данными клиентов
LEFT JOIN customer_info_sub ci_sub
    ON bc.BASE_CONTRACT_ID = REGEXP_REPLACE(ci_sub.CONTRACT_ID, '-clone-.*', '')
LEFT JOIN customer_info_imei ci_imei
    ON ci_imei.ACCOUNT_ID = ms.ACCOUNT_ID
    AND TRIM(TO_CHAR(ci_imei.IMEI)) = bc.IMEI
-- Маппинг периодов
LEFT JOIN periods_mapping pm 
    ON bc.PERIOD_ID = pm.PERIOD_ID
-- Справочник валют счета-фактуры (рубли)
LEFT JOIN currency_info curr 
    ON bc.CURRENCY_ID = curr.CURRENCY_ID
-- Справочник валют лицевого счета (валюта учета договора)
LEFT JOIN acc_currency_info acc_curr 
    ON bc.ACC_CURRENCY_ID = acc_curr.CURRENCY_ID
-- Информация о тарифных планах для разделения трафика на SBD-1 и SBD-10
LEFT JOIN plan_info pi
    ON bc.BASE_CONTRACT_ID = pi.CONTRACT_ID
    AND bc.IMEI = pi.IMEI
    AND pm.PERIOD_YYYYMM = pi.PERIOD_YYYYMM
LEFT JOIN tariff_single_payment tsp ON ms.SERVICE_ID = tsp.SERVICE_ID
GROUP BY 
    ms.SERVICE_ID,
    bc.BASE_CONTRACT_ID,
    bc.IMEI,
    COALESCE(ci_sub.CONTRACT_ID, ci_imei.CONTRACT_ID),
    COALESCE(ci_sub.CUSTOMER_NAME, ci_imei.CUSTOMER_NAME),
    COALESCE(ci_sub.ORGANIZATION_NAME, ci_imei.ORGANIZATION_NAME),
    COALESCE(ci_sub.PERSON_NAME, ci_imei.PERSON_NAME),
    COALESCE(ci_sub.CODE_1C, ci_imei.CODE_1C),
    ms.ACCOUNT_ID,
    ms.CUSTOMER_ID,
    COALESCE(ci_sub.AGREEMENT_NUMBER, ci_imei.AGREEMENT_NUMBER),
    COALESCE(ci_sub.ORDER_NUMBER, ci_imei.ORDER_NUMBER),
    COALESCE(ci_sub.INFO_SERVICE_ID, ci_imei.INFO_SERVICE_ID),
    COALESCE(ci_sub.TARIFF_ID, ci_imei.TARIFF_ID),
    COALESCE(ci_sub.IS_SUSPENDED, ci_imei.IS_SUSPENDED),
    COALESCE(ci_sub.START_DATE, ci_imei.START_DATE),
    COALESCE(ci_sub.STOP_DATE, ci_imei.STOP_DATE),
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
/

-- Комментарии
COMMENT ON TABLE V_REVENUE_FROM_INVOICES IS 'Отчет по доходам из счетов-фактур (BM_INVOICE_ITEM). Сопутствующие услуги по VSAT=IMEI; см. комментарии к колонкам. Справочно: TARIFF_SINGLE_PAYMENT_MONEY из тарифа single_payment для 9002/9014.'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.CONTRACT_ID IS 'Базовый SUB-XXXXX (без -clone-...) — ключ для сопоставления с затратами; при нестандартной привязке без 9002/9014 — числовой IMEI'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.SUB_CONTRACT_ID IS 'SUB-XXXXX контракт, найденный по (ACCOUNT_ID+IMEI) или по CONTRACT_ID. Полезно, когда CONTRACT_ID=IMEI (строка привязана к услугам без 9002/9014 в СФ периода).'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.ORGANIZATION_NAME IS 'Название организации (для юр.лиц) из V_IRIDIUM_SERVICES_INFO'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.PERSON_NAME IS 'ФИО (для физ.лиц) из V_IRIDIUM_SERVICES_INFO'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.INFO_SERVICE_ID IS 'SERVICE_ID из V_IRIDIUM_SERVICES_INFO, по которому подтянуты клиентские атрибуты (не обязательно совпадает с SERVICE_ID строки отчёта)'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.TARIFF_ID IS 'TARIFF_ID из V_IRIDIUM_SERVICES_INFO (тариф 9002/9014), связанный с IMEI/контрактом'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.IS_SUSPENDED IS 'Признак приостановки (Y/N) из V_IRIDIUM_SERVICES_INFO: есть активная 9008 по IMEI+ACCOUNT_ID'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.SERVICE_START_DATE IS 'START_DATE (open_date) основной услуги 9002/9014 из V_IRIDIUM_SERVICES_INFO'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.SERVICE_STOP_DATE IS 'STOP_DATE (stop_date) основной услуги 9002/9014 из V_IRIDIUM_SERVICES_INFO'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.SERVICE_ID IS 'SERVICE_ID услуги-якоря строки: обычно 9002 SBD или 9014 Stectrace; при отсутствии их в биллинге по IMEI+ЛС — 9008 или одна из 9004/9005/9010/9013 (см. REVENUE_ANOMALY_NOTE)'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.REVENUE_SBD_TRAFFIC IS 'Доходы от трафика превышения SBD (IRIDIUM_SBD, kb_traffic_pay, IRIDIUM_SBD_MBOX, sbd_reg, woufzwv). В счетах-фактурах показывается только трафик, превышающий включенный в абонплату (overage). Итого для всех тарифов.'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.REVENUE_SBD_TRAFFIC_SBD1 IS 'Доходы от трафика превышения SBD для тарифа SBD-1 (SBD Tiered 1250 1K). Разделение по тарифным планам.'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.REVENUE_SBD_TRAFFIC_SBD10 IS 'Доходы от трафика превышения SBD для тарифа SBD-10 (SBD Tiered 1250 10K). Разделение по тарифным планам.'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.REVENUE_SBD_ABON IS 'Доходы от абонплаты SBD (fee_sbd, abon_payment)'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.REVENUE_SUSPEND_ABON IS 'Доходы от абонплаты приостановки SBD (iridium_sbd_suspend). Услуга 9008 связана с основной услугой 9002 по VSAT=IMEI.'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.REVENUE_MONITORING_ABON IS 'Доходы мониторинга в одной колонке: 9004 (Платформа трекинг), 9005 (Платформа мониторинг), 9010 (Услуга мониторинга GSM). Сопутствующие SBD, связь по VSAT=IMEI.'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.REVENUE_MONITORING_BLOCK_ABON IS 'Доходы от абонплаты блокировки мониторинга (abo_gsm_block). Услуга 9013 связана с основной услугой 9002 по VSAT=IMEI.'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.REVENUE_MSG_ABON IS 'Доходы от абонплаты сообщений (fee_iridium_msg). Для сообщений трафик не практикуется - услуга блокируется при достижении включенного в абонплату трафика'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.REVENUE_TOTAL IS 'Итого доходов (сумма всех типов услуг) в рублях (MONEY) - основная валюта для всех договоров'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.TARIFF_SINGLE_PAYMENT_MONEY IS 'Разовый платёж подключения устройства (SBD/Stectrace) по тарифному плану: BM_TARIFFEL.MONEY для типа BM_TARIFFEL_TYPE с MNEMONIC=single_payment и TYPE_ID услуги (9002/9014). Справочно, не сумма из счёта-фактуры.'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.REVENUE_TOTAL_ACC_CURRENCY IS 'Опционально: итого доходов в валюте лицевого счета (ACC_MONEY) - только для УЕ договоров (ACC_CURRENCY_ID = 4), используется для справки. Основная валюта - рубли (REVENUE_TOTAL)'
/
COMMENT ON COLUMN V_REVENUE_FROM_INVOICES.REVENUE_ANOMALY_NOTE IS 'Предупреждение для нештатной привязки (нет 9002/9014 в биллинге по IMEI+ЛС). NULL — штатный случай.'
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
