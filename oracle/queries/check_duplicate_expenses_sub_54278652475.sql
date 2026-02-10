-- ============================================================================
-- Проверка задвоения в отчете по расходам
-- Контракт: SUB-54278652475, IMEI: 300234069001680
-- Проблема: задвоение строк из-за неправильного JOIN во VIEW
-- ============================================================================

SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING ON
SET LINESIZE 2000

PROMPT ============================================================================
PROMPT Проверка задвоения для CONTRACT_ID = SUB-54278652475, IMEI = 300234069001680
PROMPT ============================================================================
PROMPT 

-- 1. Проверка данных в исходной таблице STECCOM_EXPENSES
PROMPT 1. Данные в STECCOM_EXPENSES:
SELECT 
    ID,
    TO_CHAR(INVOICE_DATE, 'YYYY-MM-DD') AS invoice_date,
    TO_CHAR(INVOICE_DATE, 'YYYYMM') AS bill_month,
    CONTRACT_ID,
    ICC_ID_IMEI AS imei,
    DESCRIPTION,
    AMOUNT,
    TO_CHAR(TRANSACTION_DATE, 'YYYY-MM-DD') AS transaction_date,
    SOURCE_FILE,
    TO_CHAR(LOAD_DATE, 'YYYY-MM-DD HH24:MI:SS') AS load_date
FROM STECCOM_EXPENSES
WHERE CONTRACT_ID = 'SUB-54278652475'
  AND ICC_ID_IMEI = '300234069001680'
  AND (SERVICE IS NULL OR UPPER(TRIM(SERVICE)) != 'BROADBAND')
ORDER BY INVOICE_DATE DESC, ID DESC;

PROMPT 
PROMPT 2. Агрегация fees по периодам (как в CTE steccom_fees):
WITH unique_steccom_expenses AS (
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
    WHERE se.CONTRACT_ID = 'SUB-54278652475'
      AND se.ICC_ID_IMEI = '300234069001680'
      AND se.CONTRACT_ID IS NOT NULL
      AND se.ICC_ID_IMEI IS NOT NULL
      AND se.INVOICE_DATE IS NOT NULL
      AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
),
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
    WHERE se.rn = 1
    GROUP BY 
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
        se.CONTRACT_ID, 
        se.ICC_ID_IMEI
)
SELECT 
    bill_month,
    CONTRACT_ID,
    imei,
    fee_activation_fee,
    fee_advance_charge,
    fee_credit,
    fee_credited,
    fee_prorated,
    fees_total
FROM steccom_fees
ORDER BY bill_month DESC;

PROMPT 
PROMPT 3. Данные в V_CONSOLIDATED_OVERAGE_REPORT (базовая таблица для JOIN):
SELECT 
    IMEI,
    CONTRACT_ID,
    BILL_MONTH,
    COUNT(*) AS row_count
FROM V_CONSOLIDATED_OVERAGE_REPORT
WHERE CONTRACT_ID = 'SUB-54278652475'
  AND IMEI = '300234069001680'
GROUP BY IMEI, CONTRACT_ID, BILL_MONTH
ORDER BY BILL_MONTH DESC;

PROMPT 
PROMPT 4. Проверка JOIN с V_IRIDIUM_SERVICES_INFO (может создавать дубликаты):
SELECT 
    CONTRACT_ID,
    COUNT(*) AS service_count,
    LISTAGG(SERVICE_ID, ', ') WITHIN GROUP (ORDER BY SERVICE_ID DESC) AS service_ids,
    LISTAGG(AGREEMENT_NUMBER, ', ') WITHIN GROUP (ORDER BY SERVICE_ID DESC) AS agreement_numbers
FROM V_IRIDIUM_SERVICES_INFO
WHERE CONTRACT_ID = 'SUB-54278652475'
GROUP BY CONTRACT_ID;

PROMPT 
PROMPT 5. Проверка JOIN с SERVICES_EXT по IMEI (может создавать дубликаты):
SELECT 
    se.VALUE AS IMEI,
    COUNT(*) AS record_count,
    LISTAGG(se.SERVICE_ID, ', ') WITHIN GROUP (ORDER BY se.SERVICE_ID DESC) AS service_ids,
    LISTAGG(a.DESCRIPTION, ', ') WITHIN GROUP (ORDER BY se.SERVICE_ID DESC) AS agreement_numbers
FROM SERVICES_EXT se
JOIN SERVICES s ON se.SERVICE_ID = s.SERVICE_ID
JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
WHERE se.VALUE = '300234069001680'
GROUP BY se.VALUE;

PROMPT 
PROMPT 6. Проверка JOIN с SERVICES.VSAT по IMEI (может создавать дубликаты):
SELECT 
    s.VSAT AS IMEI,
    COUNT(*) AS record_count,
    LISTAGG(s.SERVICE_ID, ', ') WITHIN GROUP (ORDER BY s.SERVICE_ID DESC) AS service_ids,
    LISTAGG(a.DESCRIPTION, ', ') WITHIN GROUP (ORDER BY s.SERVICE_ID DESC) AS agreement_numbers
FROM SERVICES s
JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
WHERE s.VSAT = '300234069001680'
GROUP BY s.VSAT;

PROMPT 
PROMPT 7. РЕЗУЛЬТАТ: Сколько строк в финальном VIEW (должна быть одна на период):
SELECT 
    BILL_MONTH,
    FINANCIAL_PERIOD,
    IMEI,
    CONTRACT_ID,
    AGREEMENT_NUMBER,
    SERVICE_ID,
    COUNT(*) AS duplicate_count,
    SUM(FEE_ADVANCE_CHARGE) AS total_advance_charge,
    SUM(FEE_ADVANCE_CHARGE_PREVIOUS_MONTH) AS total_advance_charge_prev,
    SUM(FEE_ACTIVATION_FEE) AS total_activation_fee,
    SUM(FEES_TOTAL) AS total_fees
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE CONTRACT_ID = 'SUB-54278652475'
  AND IMEI = '300234069001680'
GROUP BY BILL_MONTH, FINANCIAL_PERIOD, IMEI, CONTRACT_ID, AGREEMENT_NUMBER, SERVICE_ID
HAVING COUNT(*) > 1
ORDER BY BILL_MONTH DESC;

PROMPT 
PROMPT 8. Все строки из VIEW (детально):
SELECT 
    BILL_MONTH,
    FINANCIAL_PERIOD,
    IMEI,
    CONTRACT_ID,
    AGREEMENT_NUMBER,
    SERVICE_ID,
    CODE_1C,
    CUSTOMER_NAME,
    FEE_ADVANCE_CHARGE,
    FEE_ADVANCE_CHARGE_PREVIOUS_MONTH,
    FEE_ACTIVATION_FEE,
    FEES_TOTAL
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE CONTRACT_ID = 'SUB-54278652475'
  AND IMEI = '300234069001680'
ORDER BY BILL_MONTH DESC, FINANCIAL_PERIOD DESC;

EXIT;





















