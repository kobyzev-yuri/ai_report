-- Проверка: может ли быть проблема с повторной загрузкой файла
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING ON
SET LINESIZE 2000

-- Проверка: есть ли записи с одинаковыми ключевыми полями, но разными ID
-- Это может указывать на повторную загрузку файла
SELECT 
    'Duplicate records (same key fields, different ID)' AS check_type,
    COUNT(*) AS duplicate_groups,
    SUM(record_count - 1) AS extra_records,
    ROUND(SUM(total_amount - max_amount), 2) AS extra_amount
FROM (
    SELECT 
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS invoice_month,
        se.CONTRACT_ID,
        se.ICC_ID_IMEI,
        UPPER(TRIM(se.DESCRIPTION)) AS description,
        se.AMOUNT,
        se.TRANSACTION_DATE,
        COUNT(*) AS record_count,
        SUM(se.AMOUNT) AS total_amount,
        MAX(se.AMOUNT) AS max_amount
    FROM STECCOM_EXPENSES se
    WHERE se.CONTRACT_ID IS NOT NULL
      AND se.ICC_ID_IMEI IS NOT NULL
      AND se.INVOICE_DATE IS NOT NULL
      AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
      AND UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%'
      AND TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') = '202510'
    GROUP BY 
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
        se.CONTRACT_ID,
        se.ICC_ID_IMEI,
        UPPER(TRIM(se.DESCRIPTION)),
        se.AMOUNT,
        se.TRANSACTION_DATE
    HAVING COUNT(*) > 1
);

-- Проверка: примеры дубликатов
SELECT 
    'Examples of duplicates' AS check_type,
    se.CONTRACT_ID,
    se.ICC_ID_IMEI,
    TO_CHAR(se.INVOICE_DATE, 'YYYY-MM-DD') AS invoice_date,
    TO_CHAR(se.TRANSACTION_DATE, 'YYYY-MM-DD') AS transaction_date,
    se.AMOUNT,
    COUNT(*) AS record_count,
    LISTAGG(TO_CHAR(se.ID), ', ') WITHIN GROUP (ORDER BY se.ID) AS ids
FROM STECCOM_EXPENSES se
WHERE se.CONTRACT_ID IS NOT NULL
  AND se.ICC_ID_IMEI IS NOT NULL
  AND se.INVOICE_DATE IS NOT NULL
  AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
  AND UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%'
  AND TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') = '202510'
GROUP BY 
    se.CONTRACT_ID,
    se.ICC_ID_IMEI,
    TO_CHAR(se.INVOICE_DATE, 'YYYY-MM-DD'),
    TO_CHAR(se.TRANSACTION_DATE, 'YYYY-MM-DD'),
    se.AMOUNT
HAVING COUNT(*) > 1
ORDER BY record_count DESC, se.ICC_ID_IMEI
FETCH FIRST 10 ROWS ONLY;

-- Проверка: сумма после дедупликации (только rn=1)
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
        ) AS rn,
        CASE 
            WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' OR UPPER(TRIM(se.DESCRIPTION)) = 'ADVANCE CHARGE' THEN 1
            ELSE 0
        END AS is_advance_charge
    FROM STECCOM_EXPENSES se
    WHERE se.CONTRACT_ID IS NOT NULL
      AND se.ICC_ID_IMEI IS NOT NULL
      AND se.INVOICE_DATE IS NOT NULL
      AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
)
SELECT 
    'After deduplication (rn=1)' AS check_type,
    COUNT(*) AS record_count,
    ROUND(SUM(se.AMOUNT), 2) AS total_sum
FROM unique_steccom_expenses se
WHERE se.is_advance_charge = 1
  AND se.rn = 1
  AND TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') = '202510';

EXIT;





