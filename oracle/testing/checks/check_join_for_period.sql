-- Проверка JOIN для периода 2025-11
SET PAGESIZE 1000
SET LINESIZE 200

PROMPT ========================================
PROMPT Проверка данных в cor (V_CONSOLIDATED_OVERAGE_REPORT) для периода 2025-11
PROMPT ========================================
SELECT 
    BILL_MONTH,
    IMEI,
    CONTRACT_ID,
    LENGTH(CONTRACT_ID) AS contract_id_len,
    SUBSTR(BILL_MONTH, 1, 6) AS bill_month_first_6
FROM V_CONSOLIDATED_OVERAGE_REPORT
WHERE IMEI = '300234069308010'
  AND CONTRACT_ID = 'SUB-26089990228'
  AND BILL_MONTH = '202511';

PROMPT ========================================
PROMPT Проверка данных в steccom_fees для периода 2025-11
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
    LENGTH(contract_id) AS contract_id_len,
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
  AND SUBSTR(bill_month, 1, 6) IN ('202510', '202511');

PROMPT ========================================
PROMPT Проверка JOIN вручную для периода 2025-11
PROMPT ========================================
WITH cor_data AS (
    SELECT 
        BILL_MONTH,
        IMEI,
        CONTRACT_ID,
        SUBSTR(BILL_MONTH, 1, 6) AS bill_month_first_6
    FROM V_CONSOLIDATED_OVERAGE_REPORT
    WHERE IMEI = '300234069308010'
      AND CONTRACT_ID = 'SUB-26089990228'
      AND BILL_MONTH = '202511'
),
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
    cor.BILL_MONTH AS cor_bill_month,
    cor.bill_month_first_6 AS cor_first_6,
    sf.bill_month AS sf_bill_month,
    SUBSTR(sf.bill_month, 1, 6) AS sf_first_6,
    cor.bill_month_first_6 = SUBSTR(sf.bill_month, 1, 6) AS month_match,
    cor.CONTRACT_ID AS cor_contract_id,
    sf.contract_id AS sf_contract_id,
    RTRIM(cor.CONTRACT_ID) = RTRIM(sf.contract_id) AS contract_match,
    cor.IMEI AS cor_imei,
    sf.imei AS sf_imei,
    cor.IMEI = sf.imei AS imei_match,
    sf.fee_activation_fee,
    sf.fee_advance_charge,
    sf.fee_credit,
    sf.fee_credited,
    sf.fee_prorated
FROM cor_data cor
LEFT JOIN steccom_fees sf
    ON cor.bill_month_first_6 = SUBSTR(sf.bill_month, 1, 6)
    AND RTRIM(cor.CONTRACT_ID) = RTRIM(sf.contract_id)
    AND cor.IMEI = sf.imei;

PROMPT ========================================
PROMPT Проверка завершена
PROMPT ========================================

