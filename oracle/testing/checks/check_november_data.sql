-- Проверка данных за ноябрь 2025
SET PAGESIZE 1000
SET LINESIZE 200

PROMPT ========================================
PROMPT Проверка исходных данных в STECCOM_EXPENSES для ноября 2025
PROMPT ========================================
SELECT 
    SOURCE_FILE,
    CONTRACT_ID,
    ICC_ID_IMEI,
    DESCRIPTION,
    AMOUNT,
    INVOICE_DATE
FROM STECCOM_EXPENSES
WHERE CONTRACT_ID = 'SUB-26089990228'
  AND ICC_ID_IMEI = '300234069308010'
  AND (SOURCE_FILE LIKE '%202512%' OR INVOICE_DATE >= DATE '2025-11-01')
ORDER BY INVOICE_DATE DESC;

PROMPT ========================================
PROMPT Проверка: какой bill_month получается для файла 20251202.csv
PROMPT ========================================
SELECT 
    '20251202.csv' AS source_file,
    CASE 
        WHEN MOD(20251202 / 100, 100) = 1 THEN
            TO_CHAR((20251202 / 10000 - 1) * 100 + 12)
        ELSE
            TO_CHAR(20251202 / 100 - 1)
    END AS calculated_bill_month,
    SUBSTR(CASE 
        WHEN MOD(20251202 / 100, 100) = 1 THEN
            TO_CHAR((20251202 / 10000 - 1) * 100 + 12)
        ELSE
            TO_CHAR(20251202 / 100 - 1)
    END, 1, 6) AS bill_month_first_6
FROM DUAL;

PROMPT ========================================
PROMPT Проверка завершена
PROMPT ========================================

