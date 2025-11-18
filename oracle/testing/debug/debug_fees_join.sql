-- Отладка JOIN для Fees
SET PAGESIZE 1000
SET LINESIZE 200

PROMPT ========================================
PROMPT Проверка JOIN для Fees - текущий месяц
PROMPT ========================================
SELECT 
    cor.BILL_MONTH AS cor_bill_month,
    SUBSTR(cor.BILL_MONTH, 1, 6) AS cor_first_6,
    sf.bill_month AS sf_bill_month,
    SUBSTR(sf.bill_month, 1, 6) AS sf_first_6,
    SUBSTR(cor.BILL_MONTH, 1, 6) = SUBSTR(sf.bill_month, 1, 6) AS month_match,
    cor.CONTRACT_ID AS cor_contract_id,
    sf.CONTRACT_ID AS sf_contract_id,
    cor.CONTRACT_ID = sf.CONTRACT_ID AS contract_match,
    cor.IMEI AS cor_imei,
    sf.imei AS sf_imei,
    cor.IMEI = sf.imei AS imei_match,
    sf.fee_advance_charge,
    sf.fee_activation_fee
FROM (
    SELECT DISTINCT 
        BILL_MONTH,
        CONTRACT_ID,
        IMEI
    FROM V_CONSOLIDATED_OVERAGE_REPORT
    WHERE ROWNUM <= 10
) cor
LEFT JOIN (
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
            SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' OR UPPER(TRIM(se.DESCRIPTION)) = 'ADVANCE CHARGE' THEN se.AMOUNT ELSE 0 END) AS fee_advance_charge,
            SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ACTIVATION%' OR UPPER(TRIM(se.DESCRIPTION)) = 'ACTIVATION FEE' THEN se.AMOUNT ELSE 0 END) AS fee_activation_fee
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
    SELECT * FROM steccom_fees
) sf
    ON SUBSTR(cor.BILL_MONTH, 1, 6) = SUBSTR(sf.bill_month, 1, 6)
    AND cor.CONTRACT_ID = sf.CONTRACT_ID
    AND cor.IMEI = sf.imei
WHERE sf.fee_advance_charge IS NOT NULL
  AND sf.fee_advance_charge != 0;

PROMPT ========================================
PROMPT Проверка данных в представлении для конкретного примера
PROMPT ========================================
SELECT 
    BILL_MONTH,
    FINANCIAL_PERIOD,
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
PROMPT Проверка данных в steccom_fees для этого же примера
PROMPT ========================================
SELECT 
    bill_month,
    SUBSTR(bill_month, 1, 6) AS first_6,
    contract_id,
    imei,
    fee_activation_fee,
    fee_advance_charge,
    fee_credit,
    fee_credited,
    fee_prorated,
    fees_total
FROM (
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
    SELECT * FROM steccom_fees
)
WHERE contract_id = 'SUB-26089990228'
  AND imei = '300234069308010'
  AND SUBSTR(bill_month, 1, 6) = '202509';

PROMPT ========================================
PROMPT Отладка завершена
PROMPT ========================================

