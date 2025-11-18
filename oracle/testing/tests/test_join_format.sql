-- Тест формата JOIN для текущего и предыдущего месяца
SET PAGESIZE 1000
SET LINESIZE 200

PROMPT ========================================
PROMPT 1. Проверка формата BILL_MONTH в cor
PROMPT ========================================
SELECT DISTINCT 
    cor.BILL_MONTH,
    LENGTH(cor.BILL_MONTH) AS len,
    SUBSTR(cor.BILL_MONTH, 1, 6) AS first_6_chars,
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
    END AS calculated_prev_month
FROM (
    SELECT DISTINCT BILL_MONTH
    FROM V_CONSOLIDATED_OVERAGE_REPORT
    WHERE ROWNUM <= 20
) cor
ORDER BY cor.BILL_MONTH DESC;

PROMPT ========================================
PROMPT 2. Проверка JOIN с текущим месяцем (sf)
PROMPT ========================================
SELECT 
    cor.BILL_MONTH,
    sf.bill_month AS sf_bill_month,
    cor.BILL_MONTH = sf.bill_month AS match_current,
    cor.CONTRACT_ID,
    cor.IMEI,
    sf.fee_advance_charge
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
) sf
    ON cor.BILL_MONTH = sf.bill_month
    AND cor.CONTRACT_ID = sf.CONTRACT_ID
    AND cor.IMEI = sf.imei
WHERE sf.fee_advance_charge IS NOT NULL
  AND sf.fee_advance_charge != 0
  AND ROWNUM <= 10;

PROMPT ========================================
PROMPT 3. Проверка JOIN с предыдущим месяцем (sf_prev) - вручную
PROMPT ========================================
SELECT 
    cor.BILL_MONTH,
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
    sf_prev.bill_month AS sf_prev_bill_month,
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
    END = sf_prev.bill_month AS match_prev,
    cor.CONTRACT_ID,
    cor.IMEI,
    sf_prev.fee_advance_charge AS prev_fee
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
    ON CASE 
           WHEN cor.BILL_MONTH IS NOT NULL 
            AND LENGTH(cor.BILL_MONTH) >= 6 
            AND TO_NUMBER(SUBSTR(cor.BILL_MONTH, 5, 2)) = 1 THEN
               TO_CHAR(TO_NUMBER(SUBSTR(cor.BILL_MONTH, 1, 4)) - 1) || '12'
           WHEN cor.BILL_MONTH IS NOT NULL 
            AND LENGTH(cor.BILL_MONTH) >= 6 THEN
               LPAD(TO_CHAR(TO_NUMBER(SUBSTR(cor.BILL_MONTH, 1, 6)) - 1), 6, '0')
           ELSE
               NULL
       END = sf_prev.bill_month
    AND cor.CONTRACT_ID = sf_prev.CONTRACT_ID
    AND cor.IMEI = sf_prev.imei
WHERE sf_prev.fee_advance_charge IS NOT NULL
  AND sf_prev.fee_advance_charge != 0
  AND ROWNUM <= 10;

PROMPT ========================================
PROMPT Тест завершен
PROMPT ========================================

