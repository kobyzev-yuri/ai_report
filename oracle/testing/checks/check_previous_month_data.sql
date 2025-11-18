-- Проверка данных за предыдущий месяц
SET PAGESIZE 1000
SET LINESIZE 200

PROMPT ========================================
PROMPT 1. Проверка: есть ли данные в текущем месяце?
PROMPT ========================================
SELECT 
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN FEE_ADVANCE_CHARGE IS NOT NULL AND FEE_ADVANCE_CHARGE != 0 THEN 1 END) AS rows_with_current_fee,
    COUNT(CASE WHEN FEE_ADVANCE_CHARGE_PREVIOUS_MONTH IS NOT NULL THEN 1 END) AS rows_with_prev_fee_not_null,
    COUNT(CASE WHEN FEE_ADVANCE_CHARGE_PREVIOUS_MONTH IS NOT NULL AND FEE_ADVANCE_CHARGE_PREVIOUS_MONTH != 0 THEN 1 END) AS rows_with_prev_fee_non_zero
FROM V_CONSOLIDATED_REPORT_WITH_BILLING;

PROMPT ========================================
PROMPT 2. Примеры данных с текущим месяцем (первые 5 строк)
PROMPT ========================================
SELECT 
    BILL_MONTH,
    BILL_MONTH_YYYMM,
    SUBSTR(BILL_MONTH_YYYMM, 1, 6) AS first_6_chars,
    IMEI,
    CONTRACT_ID,
    FEE_ADVANCE_CHARGE AS current_fee,
    FEE_ADVANCE_CHARGE_PREVIOUS_MONTH AS prev_fee
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE FEE_ADVANCE_CHARGE IS NOT NULL 
  AND FEE_ADVANCE_CHARGE != 0
  AND ROWNUM <= 5
ORDER BY BILL_MONTH DESC;

PROMPT ========================================
PROMPT 3. Проверка вычисления предыдущего месяца для реальных данных
PROMPT ========================================
SELECT 
    cor.BILL_MONTH AS current_bill_month,
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
    END AS calculated_previous_month,
    cor.IMEI,
    cor.CONTRACT_ID
FROM (
    SELECT DISTINCT 
        BILL_MONTH,
        IMEI,
        CONTRACT_ID
    FROM V_CONSOLIDATED_OVERAGE_REPORT
    WHERE FEE_ADVANCE_CHARGE IS NOT NULL
      AND FEE_ADVANCE_CHARGE != 0
      AND ROWNUM <= 10
) cor
ORDER BY cor.BILL_MONTH DESC;

PROMPT ========================================
PROMPT 4. Проверка: есть ли данные в steccom_fees за предыдущие месяцы?
PROMPT ========================================
SELECT 
    bill_month,
    COUNT(*) AS record_count,
    COUNT(DISTINCT contract_id) AS unique_contracts,
    COUNT(DISTINCT imei) AS unique_imeis,
    SUM(fee_advance_charge) AS total_advance_charge
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
GROUP BY bill_month
ORDER BY bill_month DESC;

PROMPT ========================================
PROMPT 5. Ручная проверка JOIN для одного примера
PROMPT ========================================
SELECT 
    cor.BILL_MONTH AS current_month,
    SUBSTR(cor.BILL_MONTH, 1, 6) AS current_month_6chars,
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
    sf_current.fee_advance_charge AS current_fee,
    sf_prev.bill_month AS prev_bill_month_in_table,
    sf_prev.fee_advance_charge AS prev_fee,
    cor.CONTRACT_ID,
    cor.IMEI
FROM (
    SELECT DISTINCT 
        BILL_MONTH,
        CONTRACT_ID,
        IMEI
    FROM V_CONSOLIDATED_OVERAGE_REPORT
    WHERE BILL_MONTH IN ('202511', '202510', '202510.02', '202509', '202509.02')
      AND ROWNUM <= 5
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
    ON SUBSTR(cor.BILL_MONTH, 1, 6) = sf_current.bill_month
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
WHERE sf_current.fee_advance_charge IS NOT NULL
  AND sf_current.fee_advance_charge != 0;

PROMPT ========================================
PROMPT Проверка завершена
PROMPT ========================================

