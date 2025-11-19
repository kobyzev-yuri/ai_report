-- ============================================================================
-- V_CONSOLIDATED_REPORT_WITH_BILLING
-- Расширенный отчет с данными из биллинга (название клиента, договор, код 1С)
-- База данных: Oracle (billing7@bm7)
-- ============================================================================

SET SQLBLANKLINES ON
SET DEFINE OFF

CREATE OR REPLACE VIEW V_CONSOLIDATED_REPORT_WITH_BILLING AS
WITH -- Удаляем дубликаты из STECCOM_EXPENSES перед агрегацией fees
-- Берем только уникальные записи по ключевым полям (исключаем полные дубликаты)
-- ВАЖНО: включаем TRANSACTION_DATE в PARTITION BY, чтобы разные транзакции не считались дубликатами
unique_steccom_expenses AS (
    SELECT 
        se.*,
        ROW_NUMBER() OVER (
            PARTITION BY 
                TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
                se.CONTRACT_ID,
                se.ICC_ID_IMEI,
                UPPER(TRIM(se.DESCRIPTION)),
                se.AMOUNT,
                se.TRANSACTION_DATE
            ORDER BY se.ID
        ) AS rn
    FROM STECCOM_EXPENSES se
    WHERE se.CONTRACT_ID IS NOT NULL
      AND se.ICC_ID_IMEI IS NOT NULL
      AND se.INVOICE_DATE IS NOT NULL
      AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
),
-- Агрегируем fees из уникальных записей STECCOM_EXPENSES по типам для каждого периода
-- ВАЖНО: Используем ту же логику определения периода, что и в V_CONSOLIDATED_OVERAGE_REPORT
-- BILL_MONTH - это месяц из INVOICE_DATE (без вычитания)
steccom_fees AS (
    SELECT 
        -- Определяем bill_month по той же логике, что и в V_CONSOLIDATED_OVERAGE_REPORT
        -- Возвращаем как строку VARCHAR2 для совместимости с cor.BILL_MONTH
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS bill_month,
        se.CONTRACT_ID,
        se.ICC_ID_IMEI AS imei,
        SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ACTIVATION%' OR UPPER(TRIM(se.DESCRIPTION)) = 'ACTIVATION FEE' THEN se.AMOUNT ELSE 0 END) AS fee_activation_fee,
        SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' OR UPPER(TRIM(se.DESCRIPTION)) = 'ADVANCE CHARGE' THEN se.AMOUNT ELSE 0 END) AS fee_advance_charge,
        SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%CREDIT%' AND UPPER(TRIM(se.DESCRIPTION)) NOT LIKE '%CREDITED%' THEN se.AMOUNT ELSE 0 END) AS fee_credit,
        SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%CREDITED%' THEN se.AMOUNT ELSE 0 END) AS fee_credited,
        SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%PRORATED%' OR UPPER(TRIM(se.DESCRIPTION)) = 'PRORATED' THEN se.AMOUNT ELSE 0 END) AS fee_prorated,
        SUM(se.AMOUNT) AS fees_total
    FROM unique_steccom_expenses se
    WHERE se.rn = 1  -- Берем только первую запись из каждой группы дубликатов
    GROUP BY 
        -- Группируем по вычисленному bill_month (используем то же выражение, что и в SELECT)
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
        se.CONTRACT_ID, 
        se.ICC_ID_IMEI
)
SELECT 
    -- Bill Month в формате YYYY-MM для отображения
    -- cor.BILL_MONTH в формате YYYYMM (например, '202510')
    -- Убеждаемся, что берем только первые 6 символов и форматируем правильно
    CASE 
        WHEN cor.BILL_MONTH IS NOT NULL AND LENGTH(TRIM(cor.BILL_MONTH)) >= 6 THEN
            TO_CHAR(TO_DATE(SUBSTR(TRIM(cor.BILL_MONTH), 1, 6), 'YYYYMM'), 'YYYY-MM')
        ELSE cor.BILL_MONTH
    END AS BILL_MONTH,
    -- Сохраняем оригинальный формат для связи (YYYYMM как строка)
    -- Убеждаемся, что берем только первые 6 символов для JOIN
    CASE 
        WHEN cor.BILL_MONTH IS NOT NULL AND LENGTH(TRIM(cor.BILL_MONTH)) >= 6 THEN
            SUBSTR(TRIM(cor.BILL_MONTH), 1, 6)
        ELSE cor.BILL_MONTH
    END AS BILL_MONTH_YYYMM,
    -- Отчетный Период (Financial Period) - месяц на 1 меньше, чем BILL_MONTH
    -- BILL_MONTH = 202511 (ноябрь) → Отчетный Период = 2025-10 (октябрь)
    -- INVOICE_DATE = 2025-11-02 (ноябрь) → BILL_MONTH = 202511 (ноябрь) → FINANCIAL_PERIOD = 2025-10 (октябрь)
    CASE 
        WHEN cor.BILL_MONTH IS NOT NULL AND LENGTH(TRIM(cor.BILL_MONTH)) >= 6 THEN
            TO_CHAR(ADD_MONTHS(TO_DATE(SUBSTR(TRIM(cor.BILL_MONTH), 1, 6), 'YYYYMM'), -1), 'YYYY-MM')
        ELSE NULL
    END AS FINANCIAL_PERIOD,
    cor.IMEI,
    cor.CONTRACT_ID,
    cor.ACTIVATION_DATE,
    cor.PLAN_NAME,
    -- Разделение трафика и событий (по каждому периоду)
    cor.TRAFFIC_USAGE_BYTES,
    cor.EVENTS_COUNT,
    cor.DATA_USAGE_EVENTS,
    cor.MAILBOX_EVENTS,
    cor.REGISTRATION_EVENTS,
    -- Суммы и превышения
    cor.INCLUDED_KB,
    cor.TOTAL_USAGE_KB,
    cor.OVERAGE_KB,
    cor.CALCULATED_OVERAGE,
    cor.SPNET_TOTAL_AMOUNT,
    -- Две отдельные колонки для планов: основной и suspended
    cor.STECCOM_PLAN_NAME_MONTHLY,
    cor.STECCOM_PLAN_NAME_SUSPENDED,
    -- Добавляем данные из биллинга
    v.SERVICE_ID,
    v.CODE_1C,
    v.ORGANIZATION_NAME,
    v.CUSTOMER_NAME,
    v.AGREEMENT_NUMBER,
    v.ORDER_NUMBER,
    v.STATUS AS SERVICE_STATUS,
    v.STOP_DATE AS SERVICE_STOP_DATE,
    v.IS_SUSPENDED,
    v.CUSTOMER_ID,
    v.ACCOUNT_ID,
    v.TARIFF_ID,
    -- Доп. поля: IMEI из биллинга (VSAT/IMEI) и номер сервиса при совпадении IMEI
    v.IMEI AS IMEI_VSAT,
    CASE 
      WHEN v.CONTRACT_ID LIKE 'SUB_%' AND v.IMEI IS NOT NULL AND cor.IMEI IS NOT NULL AND v.IMEI = cor.IMEI 
      THEN v.SERVICE_ID 
      ELSE NULL 
    END AS SERVICE_ID_VSAT_MATCH,
    -- Fees из STECCOM_EXPENSES
    NVL(sf.fee_activation_fee, 0) AS FEE_ACTIVATION_FEE,
    NVL(sf.fee_advance_charge, 0) AS FEE_ADVANCE_CHARGE,
    NVL(sf_prev.fee_advance_charge, 0) AS FEE_ADVANCE_CHARGE_PREVIOUS_MONTH,
    NVL(sf.fee_credit, 0) AS FEE_CREDIT,
    NVL(sf.fee_credited, 0) AS FEE_CREDITED,
    NVL(sf.fee_prorated, 0) AS FEE_PRORATED,
    NVL(sf.fees_total, 0) AS FEES_TOTAL
FROM (
    -- Агрегируем данные для одного периода (IMEI + CONTRACT_ID + BILL_MONTH), чтобы избежать дубликатов fees
    -- Суммируем трафик и события, берем максимальные/первые значения для остальных полей
    SELECT 
        IMEI,
        CONTRACT_ID,
        BILL_MONTH,
        -- ACTIVATION_DATE: берем минимальную дату активации из всех записей периода
        MIN(ACTIVATION_DATE) AS ACTIVATION_DATE,
        -- PLAN_NAME: берем первый непустой из всех записей периода
        MAX(CASE WHEN PLAN_NAME IS NOT NULL AND LENGTH(TRIM(PLAN_NAME)) > 0 THEN PLAN_NAME END) AS PLAN_NAME,
        -- Суммируем трафик и события для всех записей периода
        SUM(TRAFFIC_USAGE_BYTES) AS TRAFFIC_USAGE_BYTES,
        SUM(EVENTS_COUNT) AS EVENTS_COUNT,
        SUM(DATA_USAGE_EVENTS) AS DATA_USAGE_EVENTS,
        SUM(MAILBOX_EVENTS) AS MAILBOX_EVENTS,
        SUM(REGISTRATION_EVENTS) AS REGISTRATION_EVENTS,
        -- Суммируем SPNet суммы
        MAX(INCLUDED_KB) AS INCLUDED_KB,
        SUM(TOTAL_USAGE_KB) AS TOTAL_USAGE_KB,
        SUM(OVERAGE_KB) AS OVERAGE_KB,
        SUM(CALCULATED_OVERAGE) AS CALCULATED_OVERAGE,
        SUM(SPNET_TOTAL_AMOUNT) AS SPNET_TOTAL_AMOUNT,
        -- STECCOM данные: берем максимальные/первые значения
        MAX(STECCOM_PLAN_NAME_MONTHLY) AS STECCOM_PLAN_NAME_MONTHLY,
        MAX(STECCOM_PLAN_NAME_SUSPENDED) AS STECCOM_PLAN_NAME_SUSPENDED
    FROM V_CONSOLIDATED_OVERAGE_REPORT
    GROUP BY IMEI, CONTRACT_ID, BILL_MONTH
) cor
-- Используем подзапрос с ROW_NUMBER() для v_iridium_services_info, чтобы избежать дубликатов fees
-- Берем одну запись на contract_id (с максимальным service_id, если несколько)
LEFT JOIN (
    SELECT v1.SERVICE_ID, v1.CONTRACT_ID, v1.IMEI, v1.TARIFF_ID, v1.AGREEMENT_NUMBER, v1.ORDER_NUMBER,
           v1.STATUS, v1.ACTUAL_STATUS, v1.CUSTOMER_ID, v1.ORGANIZATION_NAME, v1.PERSON_NAME, v1.CUSTOMER_NAME,
           v1.CREATE_DATE, v1.START_DATE, v1.STOP_DATE, v1.ACCOUNT_ID, v1.IS_SUSPENDED, v1.CODE_1C
    FROM (
        SELECT v0.*,
               ROW_NUMBER() OVER (PARTITION BY v0.CONTRACT_ID ORDER BY v0.SERVICE_ID DESC NULLS LAST) AS rn
        FROM V_IRIDIUM_SERVICES_INFO v0
    ) v1
    WHERE v1.rn = 1
) v ON cor.CONTRACT_ID = v.CONTRACT_ID
-- JOIN с fees - теперь будет только одна запись на период
-- cor.BILL_MONTH из V_CONSOLIDATED_OVERAGE_REPORT в формате YYYYMM (строка, например '202510')
-- sf.bill_month в формате YYYYMM (например '202510')
LEFT JOIN steccom_fees sf
    ON CASE 
           WHEN cor.BILL_MONTH IS NOT NULL AND LENGTH(TRIM(cor.BILL_MONTH)) >= 6 THEN
               SUBSTR(TRIM(cor.BILL_MONTH), 1, 6)
           ELSE cor.BILL_MONTH
       END = sf.bill_month
    AND RTRIM(cor.CONTRACT_ID) = RTRIM(sf.CONTRACT_ID)
    AND cor.IMEI = sf.imei
-- Advance Charge за предыдущий месяц
-- cor.BILL_MONTH в формате YYYYMM (строка, например '202510')
-- Вычисляем предыдущий месяц используя ADD_MONTHS
LEFT JOIN steccom_fees sf_prev
    ON sf_prev.bill_month = CASE 
           WHEN cor.BILL_MONTH IS NOT NULL AND LENGTH(TRIM(cor.BILL_MONTH)) >= 6 THEN
               TO_CHAR(ADD_MONTHS(TO_DATE(SUBSTR(TRIM(cor.BILL_MONTH), 1, 6), 'YYYYMM'), -1), 'YYYYMM')
           ELSE
               NULL
       END
    AND RTRIM(cor.CONTRACT_ID) = RTRIM(sf_prev.CONTRACT_ID)
    AND cor.IMEI = sf_prev.imei
UNION ALL
-- Включаем строки для финансовых периодов, где есть аванс за предыдущий месяц,
-- но нет данных о трафике/событиях за следующий месяц (IMEI был выключен)
-- ВАЖНО: Добавляем только если действительно нет данных в cor для следующего месяца
-- 
-- ЛОГИКА:
-- Если аванс за месяц X (например, сентябрь 202509),
-- то создаем строку для FINANCIAL_PERIOD = X+1 (октябрь 2025-10)
-- BILL_MONTH = X+1 (октябрь 202510), чтобы трафик за октябрь был в BILL_MONTH = октябрь
-- По формуле основного SELECT: FINANCIAL_PERIOD = BILL_MONTH - 1, но здесь мы переопределяем FINANCIAL_PERIOD = X+1
-- чтобы показать этот аванс как "аванс за предыдущий месяц" в отчете за октябрь
--
-- ПРИМЕРЫ ПОЯВЛЕНИЯ ДАННЫХ:
-- 1. В октябре (текущий месяц = 202510):
--    - Авансы за сентябрь (202509) → FINANCIAL_PERIOD = 2025-10, BILL_MONTH = 202510
--      ВАЖНО: Аванс за сентябрь отображается в отчете за октябрь (FINANCIAL_PERIOD = 2025-10)
--      Это позволяет финансистам видеть авансы за предыдущий месяц в правильном финансовом периоде
--    - Авансы за октябрь (202510) → НЕ создаются (будущий период еще не наступил)
--
-- 2. В ноябре (текущий месяц = 202511):
--    - Авансы за октябрь (202510) → FINANCIAL_PERIOD = 2025-11, BILL_MONTH = 202511
--      ВАЖНО: Аванс за октябрь отображается в отчете за ноябрь (FINANCIAL_PERIOD = 2025-11)
--      Аванс отображается как "аванс за предыдущий месяц" (FEE_ADVANCE_CHARGE_PREVIOUS_MONTH)
--      Это позволяет видеть авансы даже если IMEI был выключен в ноябре и нет трафика
--
-- 3. В декабре (текущий месяц = 202512):
--    - Авансы за ноябрь (202511) → FINANCIAL_PERIOD = 2025-12, BILL_MONTH = 202512
--      И так далее...
SELECT 
    -- BILL_MONTH = месяц через 1 после аванса (bill_month + 1)
    -- Например: аванс за сентябрь (202509) → BILL_MONTH = октябрь (2025-10)
    -- Это нужно для того, чтобы трафик за октябрь был в BILL_MONTH = октябрь, а не в ноябре
    TO_CHAR(ADD_MONTHS(TO_DATE(sf_prev.bill_month, 'YYYYMM'), 1), 'YYYY-MM') AS BILL_MONTH,
    -- BILL_MONTH_YYYMM для связи
    TO_CHAR(ADD_MONTHS(TO_DATE(sf_prev.bill_month, 'YYYYMM'), 1), 'YYYYMM') AS BILL_MONTH_YYYMM,
    -- FINANCIAL_PERIOD = месяц через 1 после аванса (bill_month + 1)
    -- ВАЖНО: Аванс за месяц X должен отображаться в отчете за месяц X+1
    -- Например: аванс за сентябрь (202509) → FINANCIAL_PERIOD = октябрь (2025-10)
    -- Это переопределяет формулу основного SELECT, чтобы показать аванс в правильном финансовом периоде
    -- Аванс за сентябрь должен быть виден в отчете за октябрь, а не за сентябрь
    TO_CHAR(ADD_MONTHS(TO_DATE(sf_prev.bill_month, 'YYYYMM'), 1), 'YYYY-MM') AS FINANCIAL_PERIOD,
    sf_prev.imei AS IMEI,
    sf_prev.CONTRACT_ID,
    NULL AS ACTIVATION_DATE,
    NULL AS PLAN_NAME,
    0 AS TRAFFIC_USAGE_BYTES,
    0 AS EVENTS_COUNT,
    0 AS DATA_USAGE_EVENTS,
    0 AS MAILBOX_EVENTS,
    0 AS REGISTRATION_EVENTS,
    0 AS INCLUDED_KB,
    0 AS TOTAL_USAGE_KB,
    0 AS OVERAGE_KB,
    0 AS CALCULATED_OVERAGE,
    0 AS SPNET_TOTAL_AMOUNT,
    NULL AS STECCOM_PLAN_NAME_MONTHLY,
    NULL AS STECCOM_PLAN_NAME_SUSPENDED,
    v.SERVICE_ID,
    v.CODE_1C,
    v.ORGANIZATION_NAME,
    v.CUSTOMER_NAME,
    v.AGREEMENT_NUMBER,
    v.ORDER_NUMBER,
    v.STATUS AS SERVICE_STATUS,
    v.STOP_DATE AS SERVICE_STOP_DATE,
    v.IS_SUSPENDED,
    v.CUSTOMER_ID,
    v.ACCOUNT_ID,
    v.TARIFF_ID,
    v.IMEI AS IMEI_VSAT,
    NULL AS SERVICE_ID_VSAT_MATCH,
    NVL(sf_next.fee_activation_fee, 0) AS FEE_ACTIVATION_FEE,
    NVL(sf_next.fee_advance_charge, 0) AS FEE_ADVANCE_CHARGE,
    NVL(sf_prev.fee_advance_charge, 0) AS FEE_ADVANCE_CHARGE_PREVIOUS_MONTH,
    NVL(sf_next.fee_credit, 0) AS FEE_CREDIT,
    NVL(sf_next.fee_credited, 0) AS FEE_CREDITED,
    NVL(sf_next.fee_prorated, 0) AS FEE_PRORATED,
    NVL(sf_next.fees_total, 0) AS FEES_TOTAL
FROM steccom_fees sf_prev
LEFT JOIN (
    SELECT v1.SERVICE_ID, v1.CONTRACT_ID, v1.IMEI, v1.TARIFF_ID, v1.AGREEMENT_NUMBER, v1.ORDER_NUMBER,
           v1.STATUS, v1.ACTUAL_STATUS, v1.CUSTOMER_ID, v1.ORGANIZATION_NAME, v1.PERSON_NAME, v1.CUSTOMER_NAME,
           v1.CREATE_DATE, v1.START_DATE, v1.STOP_DATE, v1.ACCOUNT_ID, v1.IS_SUSPENDED, v1.CODE_1C
    FROM (
        SELECT v0.*,
               ROW_NUMBER() OVER (PARTITION BY v0.CONTRACT_ID ORDER BY v0.SERVICE_ID DESC NULLS LAST) AS rn
        FROM V_IRIDIUM_SERVICES_INFO v0
    ) v1
    WHERE v1.rn = 1
) v ON RTRIM(sf_prev.CONTRACT_ID) = RTRIM(v.CONTRACT_ID)
LEFT JOIN steccom_fees sf_next
    ON sf_next.bill_month = TO_CHAR(ADD_MONTHS(TO_DATE(sf_prev.bill_month, 'YYYYMM'), 1), 'YYYYMM')
    AND RTRIM(sf_prev.CONTRACT_ID) = RTRIM(sf_next.CONTRACT_ID)
    AND sf_prev.imei = sf_next.imei
-- КРИТИЧЕСКИ ВАЖНО: Добавляем строку ТОЛЬКО если:
-- 1. Есть аванс за предыдущий месяц
-- 2. НЕТ данных в cor для BILL_MONTH = bill_month + 1 (месяц после аванса)
-- 3. И НЕТ данных в основном SELECT (проверяем через NOT EXISTS с полным набором условий)
WHERE sf_prev.fee_advance_charge > 0
  -- Проверяем, что нет данных в основном SELECT (cor) для этого периода
  AND NOT EXISTS (
      SELECT 1
      FROM (
          SELECT IMEI, CONTRACT_ID, BILL_MONTH
          FROM V_CONSOLIDATED_OVERAGE_REPORT
          GROUP BY IMEI, CONTRACT_ID, BILL_MONTH
      ) cor_check
      WHERE cor_check.IMEI = sf_prev.imei
        AND RTRIM(cor_check.CONTRACT_ID) = RTRIM(sf_prev.CONTRACT_ID)
        AND CASE 
                WHEN cor_check.BILL_MONTH IS NOT NULL AND LENGTH(TRIM(cor_check.BILL_MONTH)) >= 6 THEN
                    SUBSTR(TRIM(cor_check.BILL_MONTH), 1, 6)
                ELSE cor_check.BILL_MONTH
            END = TO_CHAR(ADD_MONTHS(TO_DATE(sf_prev.bill_month, 'YYYYMM'), 1), 'YYYYMM')
  )
  -- Дополнительная проверка: не добавляем строки для слишком старых авансов (более 3 месяцев назад)
  AND TO_DATE(sf_prev.bill_month, 'YYYYMM') >= ADD_MONTHS(SYSDATE, -3)
  -- ВАЖНО: не создаем строки для будущих финансовых периодов
  -- Проверяем, что bill_month < текущий месяц (аванс должен быть за прошедший месяц)
  -- 
  -- ЭТО ОБЕСПЕЧИВАЕТ СЛЕДУЮЩЕЕ ПОВЕДЕНИЕ:
  -- - В октябре: авансы за сентябрь создают строки для FINANCIAL_PERIOD = 2025-10
  -- - В ноябре: авансы за октябрь создают строки для FINANCIAL_PERIOD = 2025-11
  --   Это ОЖИДАЕМО: когда наступает ноябрь, авансы за октябрь появляются в отчете за ноябрь
  --   как "аванс за предыдущий месяц" (FEE_ADVANCE_CHARGE_PREVIOUS_MONTH)
  -- - В ноябре: авансы за ноябрь НЕ создают строки (будущий период 2025-12 еще не наступил)
  --
  -- Пример: если SYSDATE = 2025-11-15 (ноябрь):
  --   - Аванс за октябрь (202510): 2025-10-01 < 2025-11-01 = TRUE → создается строка для FINANCIAL_PERIOD = 2025-11
  --   - Аванс за ноябрь (202511): 2025-11-01 < 2025-11-01 = FALSE → НЕ создается (будущий период)
  AND TO_DATE(sf_prev.bill_month, 'YYYYMM') < TRUNC(SYSDATE, 'MM')
/

-- Комментарии
COMMENT ON TABLE V_CONSOLIDATED_REPORT_WITH_BILLING IS 'Консолидированный отчет по Iridium с данными клиентов из биллинга. КАЖДАЯ СТРОКА = ОТДЕЛЬНЫЙ ПЕРИОД (BILL_MONTH)'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.SERVICE_ID IS 'ID сервиса из биллинга'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.SERVICE_STATUS IS 'Статус сервиса из биллинга (10=активный, -10=приостановленный)'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.SERVICE_STOP_DATE IS 'Конец предоставления услуги (stop_date) из биллинга'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.IS_SUSPENDED IS 'Признак приостановки: Y - есть активная услуга приостановления (TYPE_ID=9008), N - нет'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.CODE_1C IS 'Код клиента из 1С'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.ORGANIZATION_NAME IS 'Название организации (для юр.лиц)'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.CUSTOMER_NAME IS 'Название организации или ФИО клиента (универсальное поле)'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.AGREEMENT_NUMBER IS 'Номер договора в СТЭККОМ'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.ORDER_NUMBER IS 'Номер заказа/приложения'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.IMEI_VSAT IS 'IMEI из биллинга (VSAT/IMEI из V_IRIDIUM_SERVICES_INFO)'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.SERVICE_ID_VSAT_MATCH IS 'SERVICE_ID если login LIKE SUB_% и IMEI (VSAT) совпадает с отчетным IMEI'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.BILL_MONTH IS 'Период в формате YYYY-MM для отображения'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.BILL_MONTH_YYYMM IS 'Период в формате YYYYMM (число) для связи с другими таблицами'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.FINANCIAL_PERIOD IS 'Отчетный Период (Financial Period) - месяц на 1 меньше, чем BILL_MONTH. BILL_MONTH = 2025-11 (ноябрь) → Отчетный Период = 2025-10 (октябрь). INVOICE_DATE = 2025-11-02 (ноябрь) → BILL_MONTH = 2025-11 (ноябрь) → FINANCIAL_PERIOD = 2025-10 (октябрь). Используется для отображения финансистам.'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.FEE_ACTIVATION_FEE IS 'Fee: Activation Fee из STECCOM_EXPENSES'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.FEE_ADVANCE_CHARGE IS 'Fee: Advance Charge из STECCOM_EXPENSES'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.FEE_CREDIT IS 'Fee: Credit из STECCOM_EXPENSES'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.FEE_CREDITED IS 'Fee: Credited из STECCOM_EXPENSES'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.FEE_ADVANCE_CHARGE IS 'Fee: Advance Charge из STECCOM_EXPENSES'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.FEE_ADVANCE_CHARGE_PREVIOUS_MONTH IS 'Fee: Advance Charge за предыдущий месяц из STECCOM_EXPENSES'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.FEE_PRORATED IS 'Fee: Prorated из STECCOM_EXPENSES'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.FEES_TOTAL IS 'Fees Total ($) - сумма всех fees'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.TRAFFIC_USAGE_BYTES IS 'Трафик в байтах (только SBD Data Usage)'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.EVENTS_COUNT IS 'Общее количество событий'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.DATA_USAGE_EVENTS IS 'События SBD Data Usage'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.MAILBOX_EVENTS IS 'События SBD Mailbox Checks'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.REGISTRATION_EVENTS IS 'События SBD Registrations'
/

SET DEFINE ON
