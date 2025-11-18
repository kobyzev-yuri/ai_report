-- Проверка конкретной записи
SET PAGESIZE 1000
SET LINESIZE 200

PROMPT ========================================
PROMPT Проверка данных для конкретной записи
PROMPT ========================================
SELECT 
    BILL_MONTH,
    IMEI,
    CONTRACT_ID,
    FEE_ACTIVATION_FEE,
    FEE_ADVANCE_CHARGE,
    FEE_CREDIT,
    FEE_CREDITED,
    FEE_PRORATED,
    FEES_TOTAL
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE IMEI = '300234069308010'
  AND CONTRACT_ID = 'SUB-26089990228'
  AND BILL_MONTH = '2025-09';

PROMPT ========================================
PROMPT Проверка данных в steccom_fees для этой записи
PROMPT ========================================
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
        CASE 
            WHEN REGEXP_LIKE(se.SOURCE_FILE, '\.([0-9]{8})\.csv$') THEN
                CASE 
                    WHEN MOD(TO_NUMBER(REGEXP_SUBSTR(se.SOURCE_FILE, '\.([0-9]{8})\.csv$', 1, 1, NULL, 1)) / 100, 100) = 1 THEN
                        TO_CHAR((TO_NUMBER(REGEXP_SUBSTR(se.SOURCE_FILE, '\.([0-9]{8})\.csv$', 1, 1, NULL, 1)) / 10000 - 1) * 100 + 12)
                    ELSE
                        TO_CHAR(TO_NUMBER(REGEXP_SUBSTR(se.SOURCE_FILE, '\.([0-9]{8})\.csv$', 1, 1, NULL, 1)) / 100 - 1)
                END
            ELSE
                CASE 
                    WHEN EXTRACT(MONTH FROM se.INVOICE_DATE) = 1 THEN
                        TO_CHAR(EXTRACT(YEAR FROM se.INVOICE_DATE) - 1) || '12'
                    ELSE
                        TO_CHAR(EXTRACT(YEAR FROM se.INVOICE_DATE)) || LPAD(TO_CHAR(EXTRACT(MONTH FROM se.INVOICE_DATE) - 1), 2, '0')
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
    FROM unique_steccom_expenses se
    WHERE se.rn = 1
    GROUP BY 
        CASE 
            WHEN REGEXP_LIKE(se.SOURCE_FILE, '\.([0-9]{8})\.csv$') THEN
                CASE 
                    WHEN MOD(TO_NUMBER(REGEXP_SUBSTR(se.SOURCE_FILE, '\.([0-9]{8})\.csv$', 1, 1, NULL, 1)) / 100, 100) = 1 THEN
                        TO_CHAR((TO_NUMBER(REGEXP_SUBSTR(se.SOURCE_FILE, '\.([0-9]{8})\.csv$', 1, 1, NULL, 1)) / 10000 - 1) * 100 + 12)
                    ELSE
                        TO_CHAR(TO_NUMBER(REGEXP_SUBSTR(se.SOURCE_FILE, '\.([0-9]{8})\.csv$', 1, 1, NULL, 1)) / 100 - 1)
                END
            ELSE
                CASE 
                    WHEN EXTRACT(MONTH FROM se.INVOICE_DATE) = 1 THEN
                        TO_CHAR(EXTRACT(YEAR FROM se.INVOICE_DATE) - 1) || '12'
                    ELSE
                        TO_CHAR(EXTRACT(YEAR FROM se.INVOICE_DATE)) || LPAD(TO_CHAR(EXTRACT(MONTH FROM se.INVOICE_DATE) - 1), 2, '0')
                END
        END,
        se.CONTRACT_ID, 
        se.ICC_ID_IMEI
)
SELECT 
    bill_month,
    SUBSTR(bill_month, 1, 6) AS bill_month_first_6,
    contract_id,
    imei,
    fee_activation_fee,
    fee_advance_charge,
    fee_credit,
    fee_credited,
    fee_prorated,
    fees_total
FROM steccom_fees
WHERE contract_id = 'SUB-26089990228'
  AND imei = '300234069308010'
  AND SUBSTR(bill_month, 1, 6) = '202509';

PROMPT ========================================
PROMPT Проверка исходных данных в STECCOM_EXPENSES
PROMPT ========================================
SELECT 
    SOURCE_FILE,
    CONTRACT_ID,
    ICC_ID_IMEI,
    DESCRIPTION,
    AMOUNT,
    INVOICE_DATE
FROM STECCOM_EXPENSES
WHERE CONTRACT_ID = 'SUB-26089990228'
  AND ICC_ID_IMEI = '300234069308010'
  AND (UPPER(TRIM(DESCRIPTION)) LIKE '%ADVANCE CHARGE%' 
       OR UPPER(TRIM(DESCRIPTION)) LIKE '%ACTIVATION%'
       OR UPPER(TRIM(DESCRIPTION)) LIKE '%CREDIT%'
       OR UPPER(TRIM(DESCRIPTION)) LIKE '%PRORATED%')
ORDER BY INVOICE_DATE DESC;

PROMPT ========================================
PROMPT Проверка завершена
PROMPT ========================================

