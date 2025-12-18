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
-- ВАЖНО: для Advance Charge учитываем TRANSACTION_DATE в PARTITION BY,
-- чтобы разные авансы по разным датам (как в инвойсе Iridium) не схлопывались
unique_steccom_expenses AS (
    SELECT 
        se.*,
        -- Для всех типов fees используем ROW_NUMBER() для дедупликации
        -- Для Advance Charge: дедуплицируем по INVOICE_DATE, CONTRACT_ID, IMEI, TRANSACTION_DATE, AMOUNT
        -- Это убирает только полные дубликаты (включая TRANSACTION_DATE),
        -- но сохраняет разные авансы с разными TRANSACTION_DATE или AMOUNT
        ROW_NUMBER() OVER (
            PARTITION BY 
                TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
                se.CONTRACT_ID,
                se.ICC_ID_IMEI,
                UPPER(TRIM(se.DESCRIPTION)),
                se.AMOUNT,
                se.TRANSACTION_DATE
            ORDER BY se.ID
        ) AS rn,
        -- Флаг для Advance Charge
        CASE 
            WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' OR UPPER(TRIM(se.DESCRIPTION)) = 'ADVANCE CHARGE' THEN 1
            ELSE 0
        END AS is_advance_charge
    FROM STECCOM_EXPENSES se
    WHERE se.CONTRACT_ID IS NOT NULL
      AND se.ICC_ID_IMEI IS NOT NULL
      AND se.INVOICE_DATE IS NOT NULL
      AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
),
-- Агрегируем fees из уникальных записей STECCOM_EXPENSES по типам для каждого периода
-- ВАЖНО: Используем ту же логику определения периода, что и в V_CONSOLIDATED_OVERAGE_REPORT
-- BILL_MONTH - это месяц из INVOICE_DATE (без вычитания)
-- Для Advance Charge: агрегируем все записи по bill_month (месяц инвойса), но сохраняем transaction_month для правильного распределения
steccom_fees AS (
    SELECT 
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
    WHERE se.rn = 1  -- Берем только первую запись из каждой группы дубликатов (для всех типов fees, включая Advance Charge)
    GROUP BY 
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
        se.CONTRACT_ID, 
        se.ICC_ID_IMEI
),
-- Отдельная таблица для Advance Charge с transaction_month для правильного распределения по периодам
-- ИНВОЙС-ЦЕНТРИЧНАЯ ЛОГИКА:
--   - Период X (например, октябрь 2025, 202510) определяется как "месяц на 1 меньше, чем месяц INVOICE_DATE"
--   - Т.е. файлы с INVOICE_DATE в ноябре 2025 (202511) дают авансы за октябрь 202510
--   - TRANSACTION_DATE для Advance Charge не используется для определения периода, чтобы совпасть с логикой Iridium Invoice
steccom_advance_charge_by_period_raw AS (
    SELECT 
        se.CONTRACT_ID,
        se.ICC_ID_IMEI AS imei,
        -- transaction_month = месяц на 1 меньше, чем месяц INVOICE_DATE (в формате YYYYMM)
        CASE 
            WHEN EXTRACT(MONTH FROM se.INVOICE_DATE) = 1 THEN
                TO_CHAR(EXTRACT(YEAR FROM se.INVOICE_DATE) - 1) || '12'
            ELSE
                TO_CHAR(EXTRACT(YEAR FROM se.INVOICE_DATE)) ||
                LPAD(TO_CHAR(EXTRACT(MONTH FROM se.INVOICE_DATE) - 1), 2, '0')
        END AS transaction_month,
        SUM(se.AMOUNT) AS fee_advance_charge
    FROM unique_steccom_expenses se
    WHERE se.is_advance_charge = 1
      AND se.rn = 1
    GROUP BY 
        se.CONTRACT_ID,
        se.ICC_ID_IMEI,
        CASE 
            WHEN EXTRACT(MONTH FROM se.INVOICE_DATE) = 1 THEN
                TO_CHAR(EXTRACT(YEAR FROM se.INVOICE_DATE) - 1) || '12'
            ELSE
                TO_CHAR(EXTRACT(YEAR FROM se.INVOICE_DATE)) ||
                LPAD(TO_CHAR(EXTRACT(MONTH FROM se.INVOICE_DATE) - 1), 2, '0')
        END
),
-- Финальная агрегация: для одного CONTRACT_ID, imei, transaction_month должна быть только одна запись
-- Это предотвращает дубликаты при JOIN для sf_advance (который использует imei)
steccom_advance_charge_by_period AS (
    SELECT 
        CONTRACT_ID,
        imei,
        transaction_month,
        SUM(fee_advance_charge) AS fee_advance_charge
    FROM steccom_advance_charge_by_period_raw
    GROUP BY 
        CONTRACT_ID,
        imei,
        transaction_month
),
-- Дополнительная агрегация для sf_prev: группируем по CONTRACT_ID и transaction_month
-- Это предотвращает дубликаты при JOIN для sf_prev (который НЕ использует imei)
-- ВАЖНО: При свопе IMEI для одного CONTRACT_ID может быть несколько разных imei с авансами
-- за один transaction_month, поэтому суммируем все авансы по CONTRACT_ID
steccom_advance_charge_by_period_for_prev AS (
    SELECT 
        CONTRACT_ID,
        transaction_month,
        SUM(fee_advance_charge) AS fee_advance_charge
    FROM steccom_advance_charge_by_period_raw
    GROUP BY 
        CONTRACT_ID,
        transaction_month
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
    cor.MAILBOX_EVENTS,
    cor.REGISTRATION_EVENTS,
    cor.EVENTS_COUNT,
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
    -- AGREEMENT_NUMBER: сначала из v (по CONTRACT_ID), потом по IMEI через SERVICES_EXT и SERVICES.VSAT
    COALESCE(
        v.AGREEMENT_NUMBER,
        imei_service_ext_info.AGREEMENT_NUMBER,
        imei_service_info.AGREEMENT_NUMBER
    ) AS AGREEMENT_NUMBER,
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
    -- Для Advance Charge используем данные из steccom_advance_charge_by_period (правильное распределение по периодам)
    -- ВАЖНО: Используем DISTINCT или агрегацию, чтобы избежать дубликатов при JOIN
    -- Если для одного IMEI есть несколько строк в cor с разными BILL_MONTH, но одинаковым transaction_month,
    -- то все они JOIN'ятся с одной и той же записью из sf_advance, что может привести к дублированию суммы
    -- Но так как FINANCIAL_PERIOD = BILL_MONTH - 1, для разных BILL_MONTH будут разные FINANCIAL_PERIOD,
    -- поэтому каждая строка будет JOIN'иться с правильной записью из sf_advance
    NVL(sf_advance.fee_advance_charge, 0) AS FEE_ADVANCE_CHARGE,
    -- ВАЖНО: Для Advance Charge Previous Month используем оконную функцию,
    -- чтобы для одного CONTRACT_ID и FINANCIAL_PERIOD брать сумму только один раз
    -- Это предотвращает дублирование суммы при swap IMEI (когда для одного CONTRACT_ID
    -- есть несколько разных IMEI в cor, и каждая строка JOIN'ится с одной и той же записью из sf_prev)
    CASE 
        WHEN sf_prev.fee_advance_charge IS NOT NULL THEN
            -- Берем сумму только для первой строки с данным CONTRACT_ID и FINANCIAL_PERIOD
            -- Остальные строки получат 0
            CASE 
                WHEN ROW_NUMBER() OVER (
                    PARTITION BY 
                        RTRIM(cor.CONTRACT_ID),
                        CASE 
                            WHEN cor.BILL_MONTH IS NOT NULL AND LENGTH(TRIM(cor.BILL_MONTH)) >= 6 THEN
                                TO_CHAR(ADD_MONTHS(TO_DATE(SUBSTR(TRIM(cor.BILL_MONTH), 1, 6), 'YYYYMM'), -1), 'YYYY-MM')
                            ELSE NULL
                        END
                    ORDER BY cor.IMEI
                ) = 1 THEN sf_prev.fee_advance_charge
                ELSE 0
            END
        ELSE 0
    END AS FEE_ADVANCE_CHARGE_PREVIOUS_MONTH,
    NVL(sf.fee_credit, 0) AS FEE_CREDIT,
    NVL(sf.fee_credited, 0) AS FEE_CREDITED,
    NVL(sf.fee_prorated, 0) AS FEE_PRORATED,
    NVL(sf.fees_total, 0) AS FEES_TOTAL
FROM (
    -- Агрегируем данные для одного периода (IMEI + CONTRACT_ID + BILL_MONTH), чтобы избежать дубликатов fees
    -- Суммируем трафик и события, берем максимальные/первые значения для остальных полей
    -- ВАЖНО: Группируем по IMEI, CONTRACT_ID, BILL_MONTH, чтобы для одного IMEI и CONTRACT_ID
    -- с разными BILL_MONTH были отдельные строки (это правильно, так как это разные периоды)
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
        SUM(MAILBOX_EVENTS) AS MAILBOX_EVENTS,
        SUM(REGISTRATION_EVENTS) AS REGISTRATION_EVENTS,
        SUM(EVENTS_COUNT) AS EVENTS_COUNT,
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
-- JOIN для Advance Charge по периодам (используем transaction_month для правильного распределения)
-- FINANCIAL_PERIOD = BILL_MONTH - 1 месяц, поэтому transaction_month должен соответствовать FINANCIAL_PERIOD
-- ВАЖНО: Авансы могут быть в любом инвойсе (bill_month), но transaction_month должен соответствовать FINANCIAL_PERIOD
-- Это позволяет правильно показывать авансы за октябрь, даже если они в октябрьском инвойсе, а отчет за ноябрь
LEFT JOIN steccom_advance_charge_by_period sf_advance
    ON RTRIM(cor.CONTRACT_ID) = RTRIM(sf_advance.CONTRACT_ID)
    AND cor.IMEI = sf_advance.imei
    -- transaction_month должен соответствовать FINANCIAL_PERIOD (BILL_MONTH - 1)
    AND sf_advance.transaction_month = CASE 
           WHEN cor.BILL_MONTH IS NOT NULL AND LENGTH(TRIM(cor.BILL_MONTH)) >= 6 THEN
               TO_CHAR(ADD_MONTHS(TO_DATE(SUBSTR(TRIM(cor.BILL_MONTH), 1, 6), 'YYYYMM'), -1), 'YYYYMM')
           ELSE NULL
       END
-- Advance Charge за предыдущий месяц
-- cor.BILL_MONTH в формате YYYYMM (строка, например '202510')
-- FINANCIAL_PERIOD = BILL_MONTH - 1, поэтому предыдущий месяц = FINANCIAL_PERIOD - 1 = BILL_MONTH - 2
-- ВАЖНО: Аванс переходит по CONTRACT_ID, а не по IMEI
-- Это позволяет корректно обрабатывать свопы IMEI (замены IMEI на SUB)
-- IMEI для отображения берется из текущего периода (cor.IMEI)
-- Используем steccom_advance_charge_by_period_for_prev для правильного распределения по периодам
-- (агрегировано по CONTRACT_ID и transaction_month, чтобы избежать дубликатов при JOIN)
LEFT JOIN steccom_advance_charge_by_period_for_prev sf_prev
    ON RTRIM(cor.CONTRACT_ID) = RTRIM(sf_prev.CONTRACT_ID)
    -- transaction_month должен соответствовать предыдущему месяцу относительно FINANCIAL_PERIOD (BILL_MONTH - 2)
    AND sf_prev.transaction_month = CASE 
           WHEN cor.BILL_MONTH IS NOT NULL AND LENGTH(TRIM(cor.BILL_MONTH)) >= 6 THEN
               TO_CHAR(ADD_MONTHS(TO_DATE(SUBSTR(TRIM(cor.BILL_MONTH), 1, 6), 'YYYYMM'), -2), 'YYYYMM')
           ELSE
               NULL
       END
-- Дополнительные JOIN'ы для получения AGREEMENT_NUMBER по IMEI (для случаев swap IMEI)
-- JOIN по IMEI через SERVICES_EXT (когда IMEI хранится в SERVICES_EXT.VALUE)
-- ВАЖНО: Если для одного IMEI есть несколько SERVICE_ID, берем активный (DATE_END IS NULL)
LEFT JOIN (
    SELECT 
        se_ranked.VALUE AS IMEI,
        se_ranked.AGREEMENT_NUMBER
    FROM (
        SELECT 
            se.VALUE,
            a.DESCRIPTION AS AGREEMENT_NUMBER,
            ROW_NUMBER() OVER (
                PARTITION BY se.VALUE 
                ORDER BY 
                    CASE WHEN se.DATE_END IS NULL THEN 0 ELSE 1 END,  -- Приоритет активным
                    se.DATE_BEG DESC NULLS LAST,  -- Затем по дате начала (новее)
                    se.SERVICE_ID DESC  -- Затем по SERVICE_ID (больший = новее)
            ) AS rn
        FROM SERVICES_EXT se
        JOIN SERVICES s ON se.SERVICE_ID = s.SERVICE_ID
        JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
        WHERE se.VALUE IS NOT NULL
    ) se_ranked
    WHERE se_ranked.rn = 1  -- Берем только первый (самый приоритетный)
) imei_service_ext_info ON TRIM(imei_service_ext_info.IMEI) = TRIM(cor.IMEI)
    AND v.SERVICE_ID IS NULL  -- JOIN только если SERVICE_ID из v отсутствует (триггерная ситуация)
-- JOIN по IMEI через SERVICES.VSAT (когда IMEI хранится в SERVICES.VSAT)
LEFT JOIN (
    SELECT 
        s.VSAT AS IMEI,
        MAX(a.DESCRIPTION) AS AGREEMENT_NUMBER
    FROM SERVICES s
    JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
    WHERE s.VSAT IS NOT NULL
    GROUP BY s.VSAT
) imei_service_info ON TRIM(imei_service_info.IMEI) = TRIM(cor.IMEI)
    AND v.SERVICE_ID IS NULL  -- JOIN только если SERVICE_ID из v отсутствует (триггерная ситуация)
    AND imei_service_ext_info.AGREEMENT_NUMBER IS NULL  -- И если imei_service_ext_info тоже не дал результат
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
--    - Авансы за ноябрь (202511) → НЕ создают строки для FINANCIAL_PERIOD = 2025-12
--      Потому что FINANCIAL_PERIOD = 2025-12 еще не прошел (мы еще в декабре)
--    - Авансы за октябрь (202510) → FINANCIAL_PERIOD = 2025-11, BILL_MONTH = 202511
--      (создаются, потому что ноябрь уже прошел)
-- 4. В январе (текущий месяц = 202601):
--    - Авансы за ноябрь (202511) → FINANCIAL_PERIOD = 2025-12, BILL_MONTH = 202512
--      Теперь создаются, потому что декабрь уже прошел
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
    0 AS MAILBOX_EVENTS,
    0 AS REGISTRATION_EVENTS,
    0 AS EVENTS_COUNT,
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
    -- AGREEMENT_NUMBER: сначала из v (по CONTRACT_ID), потом по IMEI через SERVICES_EXT и SERVICES.VSAT
    COALESCE(
        v.AGREEMENT_NUMBER,
        imei_service_ext_info.AGREEMENT_NUMBER,
        imei_service_info.AGREEMENT_NUMBER
    ) AS AGREEMENT_NUMBER,
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
-- Дополнительные JOIN'ы для получения AGREEMENT_NUMBER по IMEI (для случаев swap IMEI) - для второго SELECT
-- ВАЖНО: Если для одного IMEI есть несколько SERVICE_ID, берем активный (DATE_END IS NULL)
LEFT JOIN (
    SELECT 
        se_ranked.VALUE AS IMEI,
        se_ranked.AGREEMENT_NUMBER
    FROM (
        SELECT 
            se.VALUE,
            a.DESCRIPTION AS AGREEMENT_NUMBER,
            ROW_NUMBER() OVER (
                PARTITION BY se.VALUE 
                ORDER BY 
                    CASE WHEN se.DATE_END IS NULL THEN 0 ELSE 1 END,  -- Приоритет активным
                    se.DATE_BEG DESC NULLS LAST,  -- Затем по дате начала (новее)
                    se.SERVICE_ID DESC  -- Затем по SERVICE_ID (больший = новее)
            ) AS rn
        FROM SERVICES_EXT se
        JOIN SERVICES s ON se.SERVICE_ID = s.SERVICE_ID
        JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
        WHERE se.VALUE IS NOT NULL
    ) se_ranked
    WHERE se_ranked.rn = 1  -- Берем только первый (самый приоритетный)
) imei_service_ext_info ON TRIM(imei_service_ext_info.IMEI) = TRIM(sf_prev.imei)
    AND v.SERVICE_ID IS NULL  -- JOIN только если SERVICE_ID из v отсутствует (триггерная ситуация)
LEFT JOIN (
    SELECT 
        s.VSAT AS IMEI,
        MAX(a.DESCRIPTION) AS AGREEMENT_NUMBER
    FROM SERVICES s
    JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
    WHERE s.VSAT IS NOT NULL
    GROUP BY s.VSAT
) imei_service_info ON TRIM(imei_service_info.IMEI) = TRIM(sf_prev.imei)
    AND v.SERVICE_ID IS NULL  -- JOIN только если SERVICE_ID из v отсутствует (триггерная ситуация)
    AND imei_service_ext_info.AGREEMENT_NUMBER IS NULL  -- И если imei_service_ext_info тоже не дал результат
-- Аванс за следующий месяц (для UNION ALL части)
-- ВАЖНО: Аванс переходит по CONTRACT_ID, а не по IMEI
LEFT JOIN steccom_fees sf_next
    ON sf_next.bill_month = TO_CHAR(ADD_MONTHS(TO_DATE(sf_prev.bill_month, 'YYYYMM'), 1), 'YYYYMM')
    AND RTRIM(sf_prev.CONTRACT_ID) = RTRIM(sf_next.CONTRACT_ID)
    -- УБРАНО: AND sf_prev.imei = sf_next.imei
    -- Причина: при свопе IMEI аванс должен переходить по CONTRACT_ID
-- КРИТИЧЕСКИ ВАЖНО: Добавляем строку ТОЛЬКО если:
-- 1. Есть аванс за предыдущий месяц
-- 2. НЕТ данных в cor для BILL_MONTH = bill_month + 1 (месяц после аванса)
-- 3. И НЕТ данных в основном SELECT (проверяем через NOT EXISTS с полным набором условий)
WHERE sf_prev.fee_advance_charge > 0
  -- Проверяем, что нет данных в основном SELECT (cor) для этого периода
  -- ВАЖНО: Проверяем по CONTRACT_ID, а не по IMEI, чтобы корректно обрабатывать свопы
  AND NOT EXISTS (
      SELECT 1
      FROM (
          SELECT IMEI, CONTRACT_ID, BILL_MONTH
          FROM V_CONSOLIDATED_OVERAGE_REPORT
          GROUP BY IMEI, CONTRACT_ID, BILL_MONTH
      ) cor_check
      WHERE RTRIM(cor_check.CONTRACT_ID) = RTRIM(sf_prev.CONTRACT_ID)
        -- УБРАНО: cor_check.IMEI = sf_prev.imei
        -- Причина: при свопе IMEI проверяем только CONTRACT_ID
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
  -- И ВАЖНО: не создаем строки для будущих финансовых периодов (месяц после аванса должен быть <= текущий месяц)
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
  -- Пример: если SYSDATE = 2025-12-01 (декабрь, но трафика еще нет):
  --   - Аванс за ноябрь (202511): bill_month < 2025-12-01 = TRUE, но FINANCIAL_PERIOD = 2025-12 > текущий месяц
  --   - НЕ создаем строки для будущих периодов, если трафика еще нет
  AND TO_DATE(sf_prev.bill_month, 'YYYYMM') < TRUNC(SYSDATE, 'MM')
  -- ДОПОЛНИТЕЛЬНО: не создаем строки для финансовых периодов, которые еще не наступили
  -- Финансовый период = месяц после аванса (bill_month + 1)
  -- Создаем строки только если финансовый период < текущий месяц
  -- Это предотвращает создание строк для декабря, когда мы еще в начале декабря и трафика за декабрь еще нет
  -- Пример: если SYSDATE = 2025-12-01, авансы за ноябрь (202511) создают FINANCIAL_PERIOD = 2025-12
  -- Но 2025-12-01 НЕ < 2025-12-01, поэтому строки НЕ создаются
  -- Когда наступит январь (2026-01), авансы за ноябрь создадут строки для FINANCIAL_PERIOD = 2025-12
  AND TO_CHAR(ADD_MONTHS(TO_DATE(sf_prev.bill_month, 'YYYYMM'), 1), 'YYYY-MM') < TO_CHAR(TRUNC(SYSDATE, 'MM'), 'YYYY-MM')
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
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.MAILBOX_EVENTS IS 'События SBD Mailbox Checks'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.REGISTRATION_EVENTS IS 'События SBD Registrations'
/
COMMENT ON COLUMN V_CONSOLIDATED_REPORT_WITH_BILLING.EVENTS_COUNT IS 'Общее количество событий (MAILBOX_EVENTS + REGISTRATION_EVENTS)'
/

SET DEFINE ON
