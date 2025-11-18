-- Проверка данных для всех периодов
SET PAGESIZE 1000
SET LINESIZE 200

PROMPT ========================================
PROMPT Проверка данных в представлении для всех периодов с ненулевыми Fees
PROMPT ========================================
SELECT 
    BILL_MONTH,
    COUNT(*) AS record_count,
    SUM(CASE WHEN FEE_ACTIVATION_FEE IS NOT NULL AND FEE_ACTIVATION_FEE != 0 THEN 1 ELSE 0 END) AS has_activation_fee,
    SUM(CASE WHEN FEE_ADVANCE_CHARGE IS NOT NULL AND FEE_ADVANCE_CHARGE != 0 THEN 1 ELSE 0 END) AS has_advance_charge,
    SUM(CASE WHEN FEE_CREDIT IS NOT NULL AND FEE_CREDIT != 0 THEN 1 ELSE 0 END) AS has_credit,
    SUM(CASE WHEN FEE_CREDITED IS NOT NULL AND FEE_CREDITED != 0 THEN 1 ELSE 0 END) AS has_credited,
    SUM(CASE WHEN FEE_PRORATED IS NOT NULL AND FEE_PRORATED != 0 THEN 1 ELSE 0 END) AS has_prorated
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE IMEI = '300234069308010'
  AND CONTRACT_ID LIKE 'SUB-26089990228%'
GROUP BY BILL_MONTH
ORDER BY BILL_MONTH DESC;

PROMPT ========================================
PROMPT Проверка данных в steccom_fees для всех периодов
PROMPT ========================================
WITH unique_steccom_expenses AS (
    SELECT se.*,
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
    WHERE se.CONTRACT_ID = 'SUB-26089990228'
      AND se.ICC_ID_IMEI = '300234069308010'
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
        SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%PRORATED%' OR UPPER(TRIM(se.DESCRIPTION)) = 'PRORATED' THEN se.AMOUNT ELSE 0 END) AS fee_prorated
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
    SUBSTR(bill_month, 1, 6) AS bill_month_first_6,
    COUNT(*) AS record_count,
    SUM(CASE WHEN fee_activation_fee != 0 THEN 1 ELSE 0 END) AS has_activation_fee,
    SUM(CASE WHEN fee_advance_charge != 0 THEN 1 ELSE 0 END) AS has_advance_charge,
    SUM(CASE WHEN fee_credit != 0 THEN 1 ELSE 0 END) AS has_credit,
    SUM(CASE WHEN fee_credited != 0 THEN 1 ELSE 0 END) AS has_credited,
    SUM(CASE WHEN fee_prorated != 0 THEN 1 ELSE 0 END) AS has_prorated
FROM steccom_fees
GROUP BY SUBSTR(bill_month, 1, 6)
ORDER BY SUBSTR(bill_month, 1, 6) DESC;

PROMPT ========================================
PROMPT Проверка завершена
PROMPT ========================================

