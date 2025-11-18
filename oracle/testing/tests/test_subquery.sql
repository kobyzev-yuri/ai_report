-- Тестовый скрипт для проверки подзапроса FEE_ADVANCE_CHARGE_PREVIOUS_MONTH
SET PAGESIZE 1000
SET LINESIZE 200

-- Проверяем, работает ли подзапрос для вычисления предыдущего месяца
PROMPT ========================================
PROMPT Тест вычисления предыдущего месяца
PROMPT ========================================

-- Тест 1: Январь (202501 -> 202412)
SELECT 
    '202501' AS current_month,
    CASE 
        WHEN '202501' IS NOT NULL 
         AND LENGTH('202501') = 6 
         AND TO_NUMBER(SUBSTR('202501', 5, 2)) = 1 THEN
            TO_CHAR(TO_NUMBER(SUBSTR('202501', 1, 4)) - 1) || '12'
        WHEN '202501' IS NOT NULL 
         AND LENGTH('202501') = 6 THEN
            LPAD(TO_CHAR(TO_NUMBER('202501') - 1), 6, '0')
        ELSE
            NULL
    END AS previous_month
FROM DUAL;

-- Тест 2: Октябрь (202510 -> 202509)
SELECT 
    '202510' AS current_month,
    CASE 
        WHEN '202510' IS NOT NULL 
         AND LENGTH('202510') = 6 
         AND TO_NUMBER(SUBSTR('202510', 5, 2)) = 1 THEN
            TO_CHAR(TO_NUMBER(SUBSTR('202510', 1, 4)) - 1) || '12'
        WHEN '202510' IS NOT NULL 
         AND LENGTH('202510') = 6 THEN
            LPAD(TO_CHAR(TO_NUMBER('202510') - 1), 6, '0')
        ELSE
            NULL
    END AS previous_month
FROM DUAL;

-- Тест 3: Проверяем, что подзапрос работает с реальными данными
PROMPT ========================================
PROMPT Тест подзапроса с реальными данными
PROMPT ========================================
SELECT 
    cor.BILL_MONTH,
    cor.CONTRACT_ID,
    cor.IMEI,
    (
        SELECT sf_prev.fee_advance_charge
        FROM (
            WITH unique_steccom_expenses AS (
                SELECT se.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY se.CONTRACT_ID, se.ICC_ID_IMEI, se.SOURCE_FILE, se.DESCRIPTION, se.AMOUNT
                           ORDER BY se.ID
                       ) AS rn
                FROM STECCOM_EXPENSES se
                WHERE se.CONTRACT_ID IS NOT NULL
                  AND se.ICC_ID_IMEI IS NOT NULL
            ),
            steccom_fees AS (
                SELECT 
                    CASE 
                        WHEN REGEXP_LIKE(se.SOURCE_FILE, '\.([0-9]{8})\.csv$') THEN
                            CASE 
                                WHEN TO_NUMBER(REGEXP_SUBSTR(se.SOURCE_FILE, '\.([0-9]{8})\.csv$', 1, 1, NULL, 1)) / 100 MOD 100 = 1 THEN
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
                                WHEN TO_NUMBER(REGEXP_SUBSTR(se.SOURCE_FILE, '\.([0-9]{8})\.csv$', 1, 1, NULL, 1)) / 100 MOD 100 = 1 THEN
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
        WHERE CASE 
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
        AND cor.IMEI = sf_prev.imei
        AND ROWNUM = 1
    ) AS fee_advance_charge_previous_month
FROM (
    SELECT DISTINCT 
        BILL_MONTH,
        CONTRACT_ID,
        IMEI
    FROM V_CONSOLIDATED_OVERAGE_REPORT
    WHERE ROWNUM <= 10
) cor;

PROMPT ========================================
PROMPT Тест завершен
PROMPT ========================================

