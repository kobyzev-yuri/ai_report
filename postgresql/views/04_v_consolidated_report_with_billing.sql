-- ============================================================================
-- V_CONSOLIDATED_REPORT_WITH_BILLING
-- Расширенный отчет с данными из биллинга (название клиента, договор, код 1С)
-- База данных: PostgreSQL (testing)
-- ============================================================================

DROP VIEW IF EXISTS V_CONSOLIDATED_REPORT_WITH_BILLING CASCADE;

CREATE OR REPLACE VIEW V_CONSOLIDATED_REPORT_WITH_BILLING AS
WITH -- Получаем маппинг tariff_id -> plan_name из существующих данных
tariff_plan_mapping AS (
    SELECT DISTINCT ON (v.tariff_id)
        v.tariff_id,
        cor.plan_name
    FROM v_iridium_services_info v
    JOIN v_consolidated_overage_report cor ON v.contract_id = cor.contract_id
    WHERE v.tariff_id IS NOT NULL 
      AND cor.plan_name IS NOT NULL
      AND cor.plan_name != ''
    GROUP BY v.tariff_id, cor.plan_name
    ORDER BY v.tariff_id, COUNT(*) DESC, cor.plan_name
),
-- Получаем маппинг IMEI -> plan_name (для IMEI, которые используются в других контрактах с планом)
imei_plan_mapping AS (
    SELECT DISTINCT ON (imei)
        imei,
        plan_name
    FROM (
        -- Планы из v_consolidated_overage_report
        SELECT cor.imei, cor.plan_name
        FROM v_consolidated_overage_report cor
        WHERE cor.imei IS NOT NULL
          AND cor.plan_name IS NOT NULL
          AND cor.plan_name != ''
        UNION ALL
        -- Планы напрямую из SPNET_TRAFFIC (даже если запись не попала в consolidated report)
        SELECT DISTINCT st.imei, st.plan_name
        FROM spnet_traffic st
        WHERE st.imei IS NOT NULL
          AND st.plan_name IS NOT NULL
          AND st.plan_name != ''
    ) all_imei_plans
    GROUP BY imei, plan_name
    ORDER BY imei, COUNT(*) DESC, plan_name
),
-- Получаем маппинг contract_id -> plan_name из SPNET_TRAFFIC напрямую
contract_plan_mapping AS (
    SELECT DISTINCT ON (contract_id)
        contract_id,
        plan_name
    FROM spnet_traffic
    WHERE contract_id IS NOT NULL
      AND plan_name IS NOT NULL
      AND plan_name != ''
    GROUP BY contract_id, plan_name
    ORDER BY contract_id, COUNT(*) DESC, plan_name
),
-- Агрегируем fees из STECCOM_EXPENSES по типам для каждого периода
-- ВАЖНО: Используем ту же логику определения периода, что и в V_CONSOLIDATED_OVERAGE_REPORT
-- Файл STECCOMLLCRussiaSBD.AccessFees.20250702.csv содержит счета за период с 2 июня по 1 июля
-- Дата в имени файла (20250702) - это дата окончания периода, поэтому вычитаем один месяц
steccom_fees AS (
    SELECT 
        -- Определяем bill_month по той же логике, что и в V_CONSOLIDATED_OVERAGE_REPORT
        CASE 
            WHEN se.SOURCE_FILE ~ '\.([0-9]{8})\.csv$' THEN
                CASE 
                    WHEN ((regexp_match(se.SOURCE_FILE, '\.([0-9]{8})\.csv$'))[1]::int) / 100 % 100 = 1 THEN
                        (((regexp_match(se.SOURCE_FILE, '\.([0-9]{8})\.csv$'))[1]::int) / 10000 - 1) * 100 + 12
                    ELSE
                        ((regexp_match(se.SOURCE_FILE, '\.([0-9]{8})\.csv$'))[1]::int) / 100 - 1
                END
            ELSE
                CASE 
                    WHEN EXTRACT(MONTH FROM se.INVOICE_DATE) = 1 THEN
                        (EXTRACT(YEAR FROM se.INVOICE_DATE) - 1) * 100 + 12
                    ELSE
                        EXTRACT(YEAR FROM se.INVOICE_DATE) * 100 + EXTRACT(MONTH FROM se.INVOICE_DATE) - 1
                END
        END AS bill_month,
        se.CONTRACT_ID,
        se.ICC_ID_IMEI AS imei,
        SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ACTIVATION%' OR UPPER(TRIM(se.DESCRIPTION)) = 'ACTIVATION FEE' THEN se.AMOUNT ELSE 0 END) AS fee_activation_fee,
        SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' OR UPPER(TRIM(se.DESCRIPTION)) = 'ADVANCE CHARGE' THEN se.AMOUNT ELSE 0 END) AS fee_advance_charge,
        SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%CREDIT%' AND UPPER(TRIM(se.DESCRIPTION)) NOT LIKE '%CREDITED%' THEN se.AMOUNT ELSE 0 END) AS fee_credit,
        SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%CREDITED%' THEN se.AMOUNT ELSE 0 END) AS fee_credited,
        SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%PRORATED%' OR UPPER(TRIM(se.DESCRIPTION)) = 'PRORATED' THEN se.AMOUNT ELSE 0 END) AS fee_prorated,
        SUM(se.AMOUNT) AS fees_total
    FROM steccom_expenses se
    WHERE se.CONTRACT_ID IS NOT NULL
      AND se.ICC_ID_IMEI IS NOT NULL
      AND se.INVOICE_DATE IS NOT NULL
      AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
    GROUP BY 
        -- Группируем по вычисленному bill_month
        CASE 
            WHEN se.SOURCE_FILE ~ '\.([0-9]{8})\.csv$' THEN
                CASE 
                    WHEN ((regexp_match(se.SOURCE_FILE, '\.([0-9]{8})\.csv$'))[1]::int) / 100 % 100 = 1 THEN
                        (((regexp_match(se.SOURCE_FILE, '\.([0-9]{8})\.csv$'))[1]::int) / 10000 - 1) * 100 + 12
                    ELSE
                        ((regexp_match(se.SOURCE_FILE, '\.([0-9]{8})\.csv$'))[1]::int) / 100 - 1
                END
            ELSE
                CASE 
                    WHEN EXTRACT(MONTH FROM se.INVOICE_DATE) = 1 THEN
                        (EXTRACT(YEAR FROM se.INVOICE_DATE) - 1) * 100 + 12
                    ELSE
                        EXTRACT(YEAR FROM se.INVOICE_DATE) * 100 + EXTRACT(MONTH FROM se.INVOICE_DATE) - 1
                END
        END,
        se.CONTRACT_ID, 
        se.ICC_ID_IMEI
)
SELECT 
    -- Bill Month в формате YYYY-MM для отображения
    CASE 
        WHEN cor.bill_month IS NOT NULL AND LENGTH(cor.bill_month) = 6 THEN
            SUBSTRING(cor.bill_month, 1, 4) || '-' || SUBSTRING(cor.bill_month, 5, 2)
        ELSE cor.bill_month
    END AS bill_month,
    -- Все поля из основного отчета (по каждому периоду отдельно!)
    cor.bill_month AS bill_month_yyyymm,  -- Сохраняем оригинальный формат для связи
    -- Отчетный Период (Financial Period) - месяц на 1 меньше, чем bill_month
    -- bill_month = 2025-11 (ноябрь) → Отчетный Период = 2025-10 (октябрь)
    -- cor.bill_month может быть числом (INTEGER) или строкой, преобразуем в строку для обработки
    CASE 
        WHEN cor.bill_month IS NOT NULL THEN
            CASE 
                WHEN CAST(SUBSTRING(LPAD(CAST(cor.bill_month AS TEXT), 6, '0'), 5, 2) AS INTEGER) = 1 THEN
                    -- Если январь (01), то отчетный период = декабрь предыдущего года
                    TO_CHAR(CAST(SUBSTRING(LPAD(CAST(cor.bill_month AS TEXT), 6, '0'), 1, 4) AS INTEGER) - 1) || '-12'
                ELSE
                    -- Иначе просто вычитаем 1 месяц
                    SUBSTRING(LPAD(CAST(cor.bill_month AS TEXT), 6, '0'), 1, 4) || '-' || LPAD(TO_CHAR(CAST(SUBSTRING(LPAD(CAST(cor.bill_month AS TEXT), 6, '0'), 5, 2) AS INTEGER) - 1), 2, '0')
            END
        ELSE NULL
    END AS financial_period,
    cor.imei,
    cor.contract_id,
    -- Используем plan_name из cor, если пустой - берем из маппинга по contract_id из SPNET_TRAFFIC,
    -- если пустой - из маппинга по tariff_id, если пустой - из маппинга по IMEI
    COALESCE(cor.plan_name, cpm.plan_name, tpm.plan_name, ipm.plan_name) AS plan_name,
    -- Разделение трафика и событий (по каждому периоду)
    cor.traffic_usage_bytes,
    cor.events_count,
    cor.data_usage_events,
    cor.mailbox_events,
    cor.registration_events,
    -- Суммы и превышения
    cor.included_kb,
    cor.total_usage_kb,
    cor.overage_kb,
    cor.calculated_overage,
    -- Две отдельные колонки для планов: основной и suspended
    cor.steccom_plan_name_monthly,
    cor.steccom_plan_name_suspended,
    -- Добавляем данные из биллинга (из импортированной таблицы)
    v.service_id,
    v.code_1c,
    v.organization_name,
    v.person_name,
    v.customer_name,
    -- Универсальное поле для отображения: организация или ФИО (используется в Streamlit)
    COALESCE(NULLIF(TRIM(v.organization_name), ''), NULLIF(TRIM(v.person_name), ''), '') AS display_name,
    v.agreement_number,
    v.order_number,
    v.status AS service_status,
    v.customer_id,
    v.account_id,
    v.tariff_id,
    -- Доп. поля: IMEI из биллинга (VSAT/IMEI) и номер сервиса при совпадении IMEI
    v.imei AS imei_vsat,
    CASE 
      WHEN v.contract_id LIKE 'SUB_%' AND v.imei IS NOT NULL AND cor.imei IS NOT NULL AND v.imei = cor.imei 
      THEN v.service_id 
      ELSE NULL 
    END AS service_id_vsat_match,
    -- Fees из STECCOM_EXPENSES
    COALESCE(sf.fee_activation_fee, 0) AS fee_activation_fee,
    COALESCE(sf.fee_advance_charge, 0) AS fee_advance_charge,
    COALESCE(sf_prev.fee_advance_charge, 0) AS fee_advance_charge_previous_month,
    COALESCE(sf.fee_credit, 0) AS fee_credit,
    COALESCE(sf.fee_credited, 0) AS fee_credited,
    COALESCE(sf.fee_prorated, 0) AS fee_prorated,
    COALESCE(sf.fees_total, 0) AS fees_total
FROM (
    -- Агрегируем данные для одного периода (imei + contract_id + bill_month), чтобы избежать дубликатов fees
    -- Суммируем трафик и события, берем максимальные/первые значения для остальных полей
    SELECT 
        imei,
        contract_id,
        bill_month,
        -- PLAN_NAME: берем первый непустой из всех записей периода
        MAX(CASE WHEN plan_name IS NOT NULL AND TRIM(plan_name) != '' THEN plan_name END) AS plan_name,
        -- Суммируем трафик и события для всех записей периода
        SUM(traffic_usage_bytes) AS traffic_usage_bytes,
        SUM(events_count) AS events_count,
        SUM(data_usage_events) AS data_usage_events,
        SUM(mailbox_events) AS mailbox_events,
        SUM(registration_events) AS registration_events,
        -- Суммируем SPNet суммы
        MAX(included_kb) AS included_kb,
        SUM(total_usage_kb) AS total_usage_kb,
        SUM(overage_kb) AS overage_kb,
        SUM(calculated_overage) AS calculated_overage,
        -- STECCOM данные: берем максимальные/первые значения
        MAX(steccom_plan_name_monthly) AS steccom_plan_name_monthly,
        MAX(steccom_plan_name_suspended) AS steccom_plan_name_suspended
    FROM v_consolidated_overage_report
    GROUP BY imei, contract_id, bill_month
) cor
-- Используем DISTINCT ON для v_iridium_services_info, чтобы избежать дубликатов fees
-- Берем одну запись на contract_id (с максимальным service_id, если несколько)
LEFT JOIN LATERAL (
    SELECT DISTINCT ON (contract_id) *
    FROM v_iridium_services_info
    WHERE contract_id = cor.contract_id
    ORDER BY contract_id, service_id DESC NULLS LAST
) v ON true
LEFT JOIN contract_plan_mapping cpm
    ON cor.contract_id = cpm.contract_id
LEFT JOIN tariff_plan_mapping tpm
    ON v.tariff_id = tpm.tariff_id
LEFT JOIN imei_plan_mapping ipm
    ON cor.imei = ipm.imei
LEFT JOIN steccom_fees sf
    ON CAST(cor.bill_month AS INTEGER) = sf.bill_month
    AND cor.contract_id = sf.contract_id
    AND cor.imei = sf.imei
-- Advance Charge за предыдущий месяц
LEFT JOIN steccom_fees sf_prev
    ON -- Вычисляем предыдущий месяц: если месяц = 01, то предыдущий = 12 предыдущего года, иначе просто вычитаем 1
       CASE 
           WHEN CAST(SUBSTRING(cor.bill_month, 5, 2) AS INTEGER) = 1 THEN
               (CAST(SUBSTRING(cor.bill_month, 1, 4) AS INTEGER) - 1) * 100 + 12
           ELSE
               CAST(cor.bill_month AS INTEGER) - 1
       END = sf_prev.bill_month
    AND cor.contract_id = sf_prev.contract_id
    AND cor.imei = sf_prev.imei
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
SELECT 
    -- BILL_MONTH = месяц через 1 после аванса (bill_month + 1)
    CASE 
        WHEN sf_prev.bill_month % 100 = 12 THEN
            SUBSTRING(CAST(sf_prev.bill_month + 89 AS TEXT), 1, 4) || '-' || '01'
        ELSE
            SUBSTRING(CAST(sf_prev.bill_month + 1 AS TEXT), 1, 4) || '-' || LPAD(CAST((sf_prev.bill_month % 100) + 1 AS TEXT), 2, '0')
    END AS bill_month,
    -- BILL_MONTH_YYYMM для связи
    CASE 
        WHEN sf_prev.bill_month % 100 = 12 THEN
            (CAST(SUBSTRING(CAST(sf_prev.bill_month AS TEXT), 1, 4) AS INTEGER) + 1) * 100 + 1
        ELSE
            sf_prev.bill_month + 1
    END AS bill_month_yyyymm,
    -- FINANCIAL_PERIOD = месяц через 1 после аванса (bill_month + 1)
    -- ВАЖНО: Аванс за месяц X должен отображаться в отчете за месяц X+1
    CASE 
        WHEN sf_prev.bill_month % 100 = 12 THEN
            SUBSTRING(CAST(sf_prev.bill_month + 89 AS TEXT), 1, 4) || '-' || '01'
        ELSE
            SUBSTRING(CAST(sf_prev.bill_month + 1 AS TEXT), 1, 4) || '-' || LPAD(CAST((sf_prev.bill_month % 100) + 1 AS TEXT), 2, '0')
    END AS financial_period,
    sf_prev.imei,
    sf_prev.contract_id,
    NULL AS plan_name,
    0 AS traffic_usage_bytes,
    0 AS events_count,
    0 AS data_usage_events,
    0 AS mailbox_events,
    0 AS registration_events,
    0 AS included_kb,
    0 AS total_usage_kb,
    0 AS overage_kb,
    0 AS calculated_overage,
    NULL AS steccom_plan_name_monthly,
    NULL AS steccom_plan_name_suspended,
    v.service_id,
    v.code_1c,
    v.organization_name,
    v.person_name,
    v.customer_name,
    COALESCE(NULLIF(TRIM(v.organization_name), ''), NULLIF(TRIM(v.person_name), ''), '') AS display_name,
    v.agreement_number,
    v.order_number,
    v.status AS service_status,
    v.customer_id,
    v.account_id,
    v.tariff_id,
    v.imei AS imei_vsat,
    NULL AS service_id_vsat_match,
    COALESCE(sf_next.fee_activation_fee, 0) AS fee_activation_fee,
    COALESCE(sf_next.fee_advance_charge, 0) AS fee_advance_charge,
    COALESCE(sf_prev.fee_advance_charge, 0) AS fee_advance_charge_previous_month,
    COALESCE(sf_next.fee_credit, 0) AS fee_credit,
    COALESCE(sf_next.fee_credited, 0) AS fee_credited,
    COALESCE(sf_next.fee_prorated, 0) AS fee_prorated,
    COALESCE(sf_next.fees_total, 0) AS fees_total
FROM steccom_fees sf_prev
LEFT JOIN LATERAL (
    SELECT DISTINCT ON (contract_id) *
    FROM v_iridium_services_info
    WHERE contract_id = sf_prev.contract_id
    ORDER BY contract_id, service_id DESC NULLS LAST
) v ON true
LEFT JOIN steccom_fees sf_next
    ON CASE 
           WHEN sf_prev.bill_month % 100 = 12 THEN
               (CAST(SUBSTRING(CAST(sf_prev.bill_month AS TEXT), 1, 4) AS INTEGER) + 1) * 100 + 1
           ELSE
               sf_prev.bill_month + 1
       END = sf_next.bill_month
    AND sf_prev.contract_id = sf_next.contract_id
    AND sf_prev.imei = sf_next.imei
WHERE sf_prev.fee_advance_charge > 0
  AND NOT EXISTS (
      SELECT 1
      FROM v_consolidated_overage_report cor_check
      WHERE cor_check.imei = sf_prev.imei
        AND cor_check.contract_id = sf_prev.contract_id
        AND cor_check.bill_month = CASE 
            WHEN sf_prev.bill_month % 100 = 12 THEN
                (CAST(SUBSTRING(CAST(sf_prev.bill_month AS TEXT), 1, 4) AS INTEGER) + 1) * 100 + 1
            ELSE
                sf_prev.bill_month + 1
        END
  )
  -- Дополнительная проверка: не добавляем строки для слишком старых авансов (более 3 месяцев назад)
  AND sf_prev.bill_month >= CAST(TO_CHAR(CURRENT_DATE - INTERVAL '3 months', 'YYYYMM') AS INTEGER)
  -- ВАЖНО: не создаем строки для будущих финансовых периодов
  -- Проверяем, что bill_month < текущий месяц (аванс должен быть за прошедший месяц)
  AND sf_prev.bill_month < CAST(TO_CHAR(DATE_TRUNC('month', CURRENT_DATE), 'YYYYMM') AS INTEGER);

-- Комментарии
COMMENT ON VIEW v_consolidated_report_with_billing IS 
'Консолидированный отчет по Iridium с данными клиентов из биллинга.
Использует данные из IRIDIUM_SERVICES_INFO (импорт из Oracle).';

COMMENT ON COLUMN v_consolidated_report_with_billing.service_id IS 'ID сервиса из биллинга';
COMMENT ON COLUMN v_consolidated_report_with_billing.code_1c IS 'Код клиента из 1С';
COMMENT ON COLUMN v_consolidated_report_with_billing.organization_name IS 'Название организации (для юр.лиц)';
COMMENT ON COLUMN v_consolidated_report_with_billing.person_name IS 'ФИО физического лица';
COMMENT ON COLUMN v_consolidated_report_with_billing.customer_name IS 'Название организации или ФИО клиента (универсальное поле)';
COMMENT ON COLUMN v_consolidated_report_with_billing.display_name IS 'Отображаемое имя: организация или ФИО (для UI)';
COMMENT ON COLUMN v_consolidated_report_with_billing.agreement_number IS 'Номер договора в СТЭККОМ';
COMMENT ON COLUMN v_consolidated_report_with_billing.order_number IS 'Номер заказа/приложения';
COMMENT ON COLUMN v_consolidated_report_with_billing.imei_vsat IS 'IMEI из биллинга (VSAT/IMEI из IRIDIUM_SERVICES_INFO)';
COMMENT ON COLUMN v_consolidated_report_with_billing.service_id_vsat_match IS 'SERVICE_ID если login LIKE SUB_% и IMEI (VSAT) совпадает с отчетным IMEI';
COMMENT ON COLUMN v_consolidated_report_with_billing.bill_month IS 'Период в формате YYYY-MM для отображения';
COMMENT ON COLUMN v_consolidated_report_with_billing.bill_month_yyyymm IS 'Период в формате YYYYMM для связи с другими таблицами';
COMMENT ON COLUMN v_consolidated_report_with_billing.financial_period IS 'Отчетный Период (Financial Period) - месяц на 1 меньше, чем bill_month. bill_month = 2025-11 (ноябрь) → Отчетный Период = 2025-10 (октябрь). Используется для отображения финансистам.';
COMMENT ON COLUMN v_consolidated_report_with_billing.fee_activation_fee IS 'Fee: Activation Fee из STECCOM_EXPENSES';
COMMENT ON COLUMN v_consolidated_report_with_billing.fee_advance_charge IS 'Fee: Advance Charge из STECCOM_EXPENSES';
COMMENT ON COLUMN v_consolidated_report_with_billing.fee_advance_charge_previous_month IS 'Fee: Advance Charge за предыдущий месяц из STECCOM_EXPENSES';
COMMENT ON COLUMN v_consolidated_report_with_billing.fee_credit IS 'Fee: Credit из STECCOM_EXPENSES';
COMMENT ON COLUMN v_consolidated_report_with_billing.fee_credited IS 'Fee: Credited из STECCOM_EXPENSES';
COMMENT ON COLUMN v_consolidated_report_with_billing.fee_prorated IS 'Fee: Prorated из STECCOM_EXPENSES';
COMMENT ON COLUMN v_consolidated_report_with_billing.fees_total IS 'Fees Total ($) - сумма всех fees';

\echo 'View V_CONSOLIDATED_REPORT_WITH_BILLING created successfully!'
\echo ''
\echo 'Now includes: SERVICE_ID, CODE_1C, ORGANIZATION_NAME, CUSTOMER_NAME'
\echo ''



