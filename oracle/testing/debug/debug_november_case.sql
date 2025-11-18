-- Отладка случая с ноябрем 2025
SET PAGESIZE 1000
SET LINESIZE 200

PROMPT ========================================
PROMPT Проверка данных за ноябрь 2025
PROMPT Контракт: SUB-62005190142, IMEI: 300534067885480
PROMPT ========================================

-- 1. Проверяем формат BILL_MONTH для ноября
SELECT 
    BILL_MONTH,
    BILL_MONTH_YYYMM,
    LENGTH(BILL_MONTH_YYYMM) AS len,
    SUBSTR(BILL_MONTH_YYYMM, 1, 6) AS first_6_chars,
    IMEI,
    CONTRACT_ID,
    FEE_ADVANCE_CHARGE AS current_month_advance,
    FEE_ADVANCE_CHARGE_PREVIOUS_MONTH AS previous_month_advance
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE BILL_MONTH_YYYMM LIKE '202511%'
  AND CONTRACT_ID = 'SUB-62005190142'
  AND IMEI = '300534067885480';

PROMPT ========================================
PROMPT 2. Проверяем вычисление предыдущего месяца для 202511
PROMPT ========================================
SELECT 
    '202511' AS current_month,
    CASE 
        WHEN '202511' IS NOT NULL 
         AND LENGTH('202511') >= 6 
         AND TO_NUMBER(SUBSTR('202511', 5, 2)) = 1 THEN
            TO_CHAR(TO_NUMBER(SUBSTR('202511', 1, 4)) - 1) || '12'
        WHEN '202511' IS NOT NULL 
         AND LENGTH('202511') >= 6 THEN
            LPAD(TO_CHAR(TO_NUMBER(SUBSTR('202511', 1, 6)) - 1), 6, '0')
        ELSE
            NULL
    END AS calculated_previous_month
FROM DUAL;

PROMPT ========================================
PROMPT 3. Проверяем данные в steccom_fees за октябрь 2025
PROMPT ========================================
SELECT 
    bill_month,
    SUBSTR(bill_month, 1, 6) AS first_6_chars,
    contract_id,
    imei,
    fee_advance_charge
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
            SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' OR UPPER(TRIM(se.DESCRIPTION)) = 'ADVANCE CHARGE' THEN se.AMOUNT ELSE 0 END) AS fee_advance_charge
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
WHERE SUBSTR(bill_month, 1, 6) = '202510'
  AND contract_id = 'SUB-62005190142'
  AND imei = '300534067885480';

PROMPT ========================================
PROMPT 4. Ручная проверка JOIN для этого случая
PROMPT ========================================
SELECT 
    cor.BILL_MONTH AS current_bill_month,
    SUBSTR(cor.BILL_MONTH, 1, 6) AS current_first_6,
    CASE 
        WHEN cor.BILL_MONTH IS NOT NULL 
         AND LENGTH(cor.BILL_MONTH) >= 6 
         AND TO_NUMBER(SUBSTR(cor.BILL_MONTH, 5, 2)) = 1 THEN
            TO_CHAR(TO_NUMBER(SUBSTR(cor.BILL_MONTH, 1, 4)) - 1) || '12'
        WHEN cor.BILL_MONTH IS NOT NULL 
         AND LENGTH(cor.BILL_MONTH) >= 6 THEN
            LPAD(TO_CHAR(TO_NUMBER(SUBSTR(cor.BILL_MONTH, 1, 6)) - 1), 6, '0')
        ELSE
            NULL
    END AS calculated_prev_month,
    sf_prev.bill_month AS prev_bill_month_in_table,
    SUBSTR(sf_prev.bill_month, 1, 6) AS prev_first_6,
    CASE 
        WHEN cor.BILL_MONTH IS NOT NULL 
         AND LENGTH(cor.BILL_MONTH) >= 6 
         AND TO_NUMBER(SUBSTR(cor.BILL_MONTH, 5, 2)) = 1 THEN
            TO_CHAR(TO_NUMBER(SUBSTR(cor.BILL_MONTH, 1, 4)) - 1) || '12'
        WHEN cor.BILL_MONTH IS NOT NULL 
         AND LENGTH(cor.BILL_MONTH) >= 6 THEN
            LPAD(TO_CHAR(TO_NUMBER(SUBSTR(cor.BILL_MONTH, 1, 6)) - 1), 6, '0')
        ELSE
            NULL
    END = SUBSTR(sf_prev.bill_month, 1, 6) AS month_match,
    cor.CONTRACT_ID,
    cor.IMEI,
    sf_prev.fee_advance_charge AS prev_fee
FROM (
    SELECT DISTINCT 
        BILL_MONTH,
        CONTRACT_ID,
        IMEI
    FROM V_CONSOLIDATED_OVERAGE_REPORT
    WHERE BILL_MONTH LIKE '202511%'
      AND CONTRACT_ID = 'SUB-62005190142'
      AND IMEI = '300534067885480'
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
            SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' OR UPPER(TRIM(se.DESCRIPTION)) = 'ADVANCE CHARGE' THEN se.AMOUNT ELSE 0 END) AS fee_advance_charge
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
) sf_prev
    ON SUBSTR(sf_prev.bill_month, 1, 6) = CASE 
           WHEN cor.BILL_MONTH IS NOT NULL 
            AND LENGTH(cor.BILL_MONTH) >= 6 
            AND TO_NUMBER(SUBSTR(cor.BILL_MONTH, 5, 2)) = 1 THEN
               TO_CHAR(TO_NUMBER(SUBSTR(cor.BILL_MONTH, 1, 4)) - 1) || '12'
           WHEN cor.BILL_MONTH IS NOT NULL 
            AND LENGTH(cor.BILL_MONTH) >= 6 THEN
               LPAD(TO_CHAR(TO_NUMBER(SUBSTR(cor.BILL_MONTH, 1, 6)) - 1), 6, '0')
           ELSE
               NULL
       END
    AND cor.CONTRACT_ID = sf_prev.CONTRACT_ID
    AND cor.IMEI = sf_prev.imei;

PROMPT ========================================
PROMPT Отладка завершена
PROMPT ========================================

