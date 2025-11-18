-- Отладка JOIN для предыдущего месяца
SET PAGESIZE 1000
SET LINESIZE 200

PROMPT ========================================
PROMPT 1. Проверка данных в представлении (первые 10 строк)
PROMPT ========================================
SELECT 
    BILL_MONTH,
    BILL_MONTH_YYYMM,
    IMEI,
    CONTRACT_ID,
    FEE_ADVANCE_CHARGE,
    FEE_ADVANCE_CHARGE_PREVIOUS_MONTH
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE ROWNUM <= 10
ORDER BY BILL_MONTH DESC;

PROMPT ========================================
PROMPT 2. Проверка вычисления предыдущего месяца
PROMPT ========================================
SELECT 
    cor.BILL_MONTH AS current_bill_month,
    CASE 
        WHEN cor.BILL_MONTH IS NOT NULL 
         AND LENGTH(cor.BILL_MONTH) = 6 
         AND TO_NUMBER(SUBSTR(cor.BILL_MONTH, 5, 2)) = 1 THEN
            TO_CHAR(TO_NUMBER(SUBSTR(cor.BILL_MONTH, 1, 4)) - 1) || '12'
        WHEN cor.BILL_MONTH IS NOT NULL 
         AND LENGTH(cor.BILL_MONTH) = 6 THEN
            LPAD(TO_CHAR(TO_NUMBER(cor.BILL_MONTH) - 1), 6, '0')
        ELSE
            NULL
    END AS calculated_previous_month,
    cor.IMEI,
    cor.CONTRACT_ID
FROM (
    SELECT DISTINCT 
        BILL_MONTH,
        IMEI,
        CONTRACT_ID
    FROM V_CONSOLIDATED_OVERAGE_REPORT
    WHERE ROWNUM <= 10
) cor
ORDER BY cor.BILL_MONTH DESC;

PROMPT ========================================
PROMPT 3. Проверка данных в steccom_fees CTE
PROMPT ========================================
SELECT 
    bill_month,
    contract_id,
    imei,
    fee_advance_charge,
    COUNT(*) AS cnt
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
    WHERE fee_advance_charge != 0
)
WHERE ROWNUM <= 20
ORDER BY bill_month DESC;

PROMPT ========================================
PROMPT 4. Проверка JOIN вручную для одного примера
PROMPT ========================================
SELECT 
    cor.BILL_MONTH,
    cor.IMEI,
    cor.CONTRACT_ID,
    sf_current.fee_advance_charge AS current_month_fee,
    sf_prev.bill_month AS prev_bill_month,
    sf_prev.fee_advance_charge AS previous_month_fee
FROM (
    SELECT DISTINCT 
        BILL_MONTH,
        IMEI,
        CONTRACT_ID
    FROM V_CONSOLIDATED_OVERAGE_REPORT
    WHERE ROWNUM <= 5
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
) sf_current
    ON cor.BILL_MONTH = sf_current.bill_month
    AND cor.CONTRACT_ID = sf_current.CONTRACT_ID
    AND cor.IMEI = sf_current.imei
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
            AND LENGTH(cor.BILL_MONTH) = 6 
            AND TO_NUMBER(SUBSTR(cor.BILL_MONTH, 5, 2)) = 1 THEN
               TO_CHAR(TO_NUMBER(SUBSTR(cor.BILL_MONTH, 1, 4)) - 1) || '12'
           WHEN cor.BILL_MONTH IS NOT NULL 
            AND LENGTH(cor.BILL_MONTH) = 6 THEN
               LPAD(TO_CHAR(TO_NUMBER(cor.BILL_MONTH) - 1), 6, '0')
           ELSE
               NULL
       END = sf_prev.bill_month
    AND cor.CONTRACT_ID = sf_prev.CONTRACT_ID
    AND cor.IMEI = sf_prev.imei;

PROMPT ========================================
PROMPT Отладка завершена
PROMPT ========================================

