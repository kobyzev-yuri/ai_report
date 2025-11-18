-- Поиск октябрьской записи в исходной таблице STECCOM_EXPENSES
SET PAGESIZE 1000
SET LINESIZE 200

PROMPT ========================================
PROMPT Поиск записи за октябрь 2025 с ненулевыми Fees в STECCOM_EXPENSES
PROMPT ========================================
SELECT 
    se.SOURCE_FILE,
    se.CONTRACT_ID,
    se.ICC_ID_IMEI AS IMEI,
    se.DESCRIPTION,
    se.AMOUNT,
    se.INVOICE_DATE,
    -- Определяем bill_month по той же логике, что в представлении
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
    END AS calculated_bill_month,
    SUBSTR(CASE 
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
    END, 1, 6) AS bill_month_first_6
FROM STECCOM_EXPENSES se
WHERE se.CONTRACT_ID IS NOT NULL
  AND se.ICC_ID_IMEI IS NOT NULL
  AND se.INVOICE_DATE IS NOT NULL
  AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
  AND (
      -- Файл 20251102.csv относится к октябрю (bill_month = 202510)
      se.SOURCE_FILE LIKE '%20251102%'
      -- Или INVOICE_DATE в октябре 2025
      OR (EXTRACT(YEAR FROM se.INVOICE_DATE) = 2025 AND EXTRACT(MONTH FROM se.INVOICE_DATE) = 10)
  )
  AND se.AMOUNT != 0
  AND (
      UPPER(TRIM(se.DESCRIPTION)) LIKE '%ACTIVATION%'
      OR UPPER(TRIM(se.DESCRIPTION)) = 'ACTIVATION FEE'
      OR UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%'
      OR UPPER(TRIM(se.DESCRIPTION)) = 'ADVANCE CHARGE'
      OR (UPPER(TRIM(se.DESCRIPTION)) LIKE '%CREDIT%' AND UPPER(TRIM(se.DESCRIPTION)) NOT LIKE '%CREDITED%')
      OR UPPER(TRIM(se.DESCRIPTION)) LIKE '%CREDITED%'
      OR UPPER(TRIM(se.DESCRIPTION)) LIKE '%PRORATED%'
      OR UPPER(TRIM(se.DESCRIPTION)) = 'PRORATED'
  )
  AND ROWNUM = 1;

