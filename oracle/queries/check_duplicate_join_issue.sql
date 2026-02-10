-- ============================================================================
-- Проверка проблемы с задвоением в отчете по расходам
-- Контракт: SUB-54278652475, IMEI: 300234069001680
-- Проблема: задвоение строк из-за неправильного JOIN во VIEW
-- ============================================================================

SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING ON
SET LINESIZE 2000

PROMPT ============================================================================
PROMPT 1. Проверка данных в V_CONSOLIDATED_OVERAGE_REPORT для этого контракта и IMEI
PROMPT ============================================================================

SELECT 
    'V_CONSOLIDATED_OVERAGE_REPORT' AS source,
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
PROMPT ============================================================================
PROMPT 2. Проверка данных в steccom_fees (CTE из VIEW) для этого контракта и IMEI
PROMPT ============================================================================

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
    WHERE se.CONTRACT_ID IS NOT NULL
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
    'steccom_fees' AS source,
    bill_month,
    CONTRACT_ID,
    imei,
    COUNT(*) AS row_count,
    SUM(fee_advance_charge) AS total_advance_charge
FROM steccom_fees
WHERE CONTRACT_ID = 'SUB-54278652475'
  AND imei = '300234069001680'
GROUP BY bill_month, CONTRACT_ID, imei
ORDER BY bill_month DESC;

PROMPT 
PROMPT ============================================================================
PROMPT 3. Проверка JOIN с V_IRIDIUM_SERVICES_INFO (может быть несколько записей)
PROMPT ============================================================================

SELECT 
    'V_IRIDIUM_SERVICES_INFO' AS source,
    CONTRACT_ID,
    COUNT(*) AS service_count,
    LISTAGG(SERVICE_ID, ', ') WITHIN GROUP (ORDER BY SERVICE_ID DESC) AS service_ids
FROM V_IRIDIUM_SERVICES_INFO
WHERE CONTRACT_ID = 'SUB-54278652475'
GROUP BY CONTRACT_ID;

PROMPT 
PROMPT ============================================================================
PROMPT 4. Проверка JOIN с SERVICES_EXT по IMEI (может быть несколько записей)
PROMPT ============================================================================

SELECT 
    'SERVICES_EXT' AS source,
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
PROMPT ============================================================================
PROMPT 5. Проверка JOIN с SERVICES.VSAT по IMEI (может быть несколько записей)
PROMPT ============================================================================

SELECT 
    'SERVICES.VSAT' AS source,
    s.VSAT AS IMEI,
    COUNT(*) AS record_count,
    LISTAGG(s.SERVICE_ID, ', ') WITHIN GROUP (ORDER BY s.SERVICE_ID DESC) AS service_ids,
    LISTAGG(a.DESCRIPTION, ', ') WITHIN GROUP (ORDER BY s.SERVICE_ID DESC) AS agreement_numbers
FROM SERVICES s
JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
WHERE s.VSAT = '300234069001680'
GROUP BY s.VSAT;

PROMPT 
PROMPT ============================================================================
PROMPT 6. Проверка финального VIEW - сколько строк получается
PROMPT ============================================================================

SELECT 
    'V_CONSOLIDATED_REPORT_WITH_BILLING' AS source,
    BILL_MONTH,
    FINANCIAL_PERIOD,
    IMEI,
    CONTRACT_ID,
    COUNT(*) AS row_count,
    SUM(FEE_ADVANCE_CHARGE) AS total_advance_charge,
    SUM(FEE_ADVANCE_CHARGE_PREVIOUS_MONTH) AS total_advance_charge_prev,
    LISTAGG(AGREEMENT_NUMBER, ', ') WITHIN GROUP (ORDER BY AGREEMENT_NUMBER) AS agreement_numbers
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE CONTRACT_ID = 'SUB-54278652475'
  AND IMEI = '300234069001680'
GROUP BY BILL_MONTH, FINANCIAL_PERIOD, IMEI, CONTRACT_ID
ORDER BY BILL_MONTH DESC;

PROMPT 
PROMPT ============================================================================
PROMPT 7. Детальная проверка - все строки из VIEW
PROMPT ============================================================================

SELECT 
    BILL_MONTH,
    FINANCIAL_PERIOD,
    IMEI,
    CONTRACT_ID,
    AGREEMENT_NUMBER,
    SERVICE_ID,
    FEE_ADVANCE_CHARGE,
    FEE_ADVANCE_CHARGE_PREVIOUS_MONTH,
    FEE_ACTIVATION_FEE,
    FEES_TOTAL
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE CONTRACT_ID = 'SUB-54278652475'
  AND IMEI = '300234069001680'
ORDER BY BILL_MONTH DESC, FINANCIAL_PERIOD DESC;

EXIT;





















