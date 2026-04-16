-- ============================================================================
-- V_LBS_SERVICES
-- Сервисный отчет по услугам LBS (в биллинге TYPE_ID=9002, тарифы BM_TARIFF.NAME like '%LBS%')
-- Без денежных колонок: только атрибуты сервиса/клиента + признак попадания в СФ
-- База данных: Oracle (billing7@bm7)
-- ============================================================================

SET SQLBLANKLINES ON
SET DEFINE OFF

CREATE OR REPLACE VIEW V_LBS_SERVICES AS
WITH
lbs_tariffs AS (
    SELECT t.TARIFF_ID, t.NAME AS TARIFF_NAME
    FROM BM_TARIFF t
    WHERE UPPER(t.NAME) LIKE '%LBS%'
),
lbs_services AS (
    SELECT
        s.SERVICE_ID,
        REGEXP_REPLACE(s.LOGIN, '-clone-.*', '') AS CONTRACT_ID,
        TRIM(TO_CHAR(s.VSAT)) AS IMEI,
        s.ACCOUNT_ID,
        s.CUSTOMER_ID,
        s.TARIFF_ID,
        s.START_DATE AS OPEN_DATE,
        s.CLOSE_DATE AS CLOSE_DATE,
        s.STATUS,
        s.ACTUAL_STATUS
    FROM SERVICES s
    JOIN lbs_tariffs lt ON s.TARIFF_ID = lt.TARIFF_ID
    WHERE s.TYPE_ID = 9002
      AND s.LOGIN LIKE 'SUB-%'
),
customer_info_raw AS (
    SELECT DISTINCT
        SERVICE_ID,
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
        TARIFF_ID AS INFO_TARIFF_ID,
        IS_SUSPENDED,
        START_DATE AS INFO_START_DATE,
        STOP_DATE AS INFO_STOP_DATE
    FROM V_IRIDIUM_SERVICES_INFO
    WHERE CONTRACT_ID LIKE 'SUB-%'
),
service_info AS (
    SELECT *
    FROM (
        SELECT
            r.*,
            ROW_NUMBER() OVER (
                PARTITION BY r.SERVICE_ID
                ORDER BY r.INFO_START_DATE DESC NULLS LAST, r.INFO_STOP_DATE DESC NULLS LAST, r.SERVICE_ID DESC
            ) AS rn
        FROM customer_info_raw r
    )
    WHERE rn = 1
),
invoice_min_period AS (
    SELECT
        ii.SERVICE_ID,
        MIN(ii.PERIOD_ID) AS FIRST_INVOICE_PERIOD_ID
    FROM BM_INVOICE_ITEM ii
    GROUP BY ii.SERVICE_ID
)
SELECT
    ls.SERVICE_ID,
    ls.CONTRACT_ID,
    ls.IMEI,
    ci.CUSTOMER_NAME,
    ci.ORGANIZATION_NAME,
    ci.PERSON_NAME,
    ci.CODE_1C,
    ls.ACCOUNT_ID,
    ls.CUSTOMER_ID,
    ci.AGREEMENT_NUMBER,
    ci.ORDER_NUMBER,
    ls.TARIFF_ID,
    lt.TARIFF_NAME,
    ls.OPEN_DATE,
    ls.CLOSE_DATE,
    ls.STATUS,
    ls.ACTUAL_STATUS,
    CASE WHEN imp.FIRST_INVOICE_PERIOD_ID IS NOT NULL THEN 'Y' ELSE 'N' END AS IN_INVOICE,
    imp.FIRST_INVOICE_PERIOD_ID,
    TO_CHAR(p.START_DATE, 'YYYY-MM') AS FIRST_INVOICE_PERIOD_YYYYMM
FROM lbs_services ls
JOIN lbs_tariffs lt ON ls.TARIFF_ID = lt.TARIFF_ID
LEFT JOIN service_info ci
    ON ls.SERVICE_ID = ci.SERVICE_ID
    AND ls.CUSTOMER_ID = ci.CUSTOMER_ID
LEFT JOIN invoice_min_period imp ON ls.SERVICE_ID = imp.SERVICE_ID
LEFT JOIN BM_PERIOD p ON imp.FIRST_INVOICE_PERIOD_ID = p.PERIOD_ID
/

COMMENT ON TABLE V_LBS_SERVICES IS 'Сервисный отчет по LBS-услугам (TYPE_ID=9002, тарифы BM_TARIFF.NAME like %LBS%). Без сумм: атрибуты клиента/сервиса + признак попадания в счёт-фактуру и первый период СФ.'
/
COMMENT ON COLUMN V_LBS_SERVICES.IN_INVOICE IS 'Y/N: есть ли SERVICE_ID в BM_INVOICE_ITEM хотя бы в одном периоде'
/
COMMENT ON COLUMN V_LBS_SERVICES.FIRST_INVOICE_PERIOD_ID IS 'Минимальный PERIOD_ID из BM_INVOICE_ITEM для данного SERVICE_ID'
/
COMMENT ON COLUMN V_LBS_SERVICES.FIRST_INVOICE_PERIOD_YYYYMM IS 'Период (YYYY-MM) минимального PERIOD_ID из BM_PERIOD'
/

SET DEFINE ON

