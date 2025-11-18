-- Быстрая проверка: почему данные не присоединяются для периода 2025-11
SET PAGESIZE 1000
SET LINESIZE 200

-- Проверка: есть ли данные в steccom_fees для периода 2025-11 (bill_month должен быть 202510, так как файл 20251102.csv относится к октябрю)
SELECT 
    'steccom_fees' AS source,
    bill_month,
    SUBSTR(bill_month, 1, 6) AS bill_month_first_6,
    contract_id,
    imei,
    fee_advance_charge
FROM (
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
          AND se.SOURCE_FILE LIKE '%20251102%'
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
            SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' THEN se.AMOUNT ELSE 0 END) AS fee_advance_charge
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
UNION ALL
SELECT 
    'cor' AS source,
    BILL_MONTH AS bill_month,
    SUBSTR(BILL_MONTH, 1, 6) AS bill_month_first_6,
    CONTRACT_ID,
    IMEI,
    NULL AS fee_advance_charge
FROM V_CONSOLIDATED_OVERAGE_REPORT
WHERE IMEI = '300234069308010'
  AND CONTRACT_ID = 'SUB-26089990228'
  AND BILL_MONTH = '202511';

