-- ============================================================================
-- Проверка дубликатов для контракта SUB-54278652475 и IMEI 300234069001680
-- Назначение: Найти дубликаты записей, которые вызывают задвоение сумм
-- ============================================================================

SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING ON
SET LINESIZE 2000

PROMPT ============================================================================
PROMPT 1. Проверка дубликатов по ключевым полям для контракта SUB-54278652475
PROMPT ============================================================================

SELECT 
    'Duplicate records found' AS check_type,
    TO_CHAR(INVOICE_DATE, 'YYYY-MM-DD') AS invoice_date,
    CONTRACT_ID,
    ICC_ID_IMEI AS imei,
    DESCRIPTION,
    AMOUNT,
    TO_CHAR(TRANSACTION_DATE, 'YYYY-MM-DD') AS transaction_date,
    COUNT(*) AS duplicate_count,
    LISTAGG(ID, ', ') WITHIN GROUP (ORDER BY ID) AS ids,
    LISTAGG(SOURCE_FILE, ', ') WITHIN GROUP (ORDER BY SOURCE_FILE) AS source_files,
    LISTAGG(TO_CHAR(LOAD_DATE, 'YYYY-MM-DD HH24:MI:SS'), ', ') WITHIN GROUP (ORDER BY LOAD_DATE) AS load_dates
FROM STECCOM_EXPENSES
WHERE CONTRACT_ID = 'SUB-54278652475'
  AND ICC_ID_IMEI = '300234069001680'
  AND CONTRACT_ID IS NOT NULL
  AND ICC_ID_IMEI IS NOT NULL
  AND INVOICE_DATE IS NOT NULL
  AND (SERVICE IS NULL OR UPPER(TRIM(SERVICE)) != 'BROADBAND')
GROUP BY 
    INVOICE_DATE,
    CONTRACT_ID,
    ICC_ID_IMEI,
    DESCRIPTION,
    AMOUNT,
    TRANSACTION_DATE
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, INVOICE_DATE DESC;

PROMPT 
PROMPT ============================================================================
PROMPT 2. Детальная информация о всех записях для этого контракта и IMEI
PROMPT ============================================================================

SELECT 
    ID,
    TO_CHAR(INVOICE_DATE, 'YYYY-MM-DD') AS invoice_date,
    CONTRACT_ID,
    ICC_ID_IMEI AS imei,
    DESCRIPTION,
    AMOUNT,
    TO_CHAR(TRANSACTION_DATE, 'YYYY-MM-DD') AS transaction_date,
    SERVICE,
    RATE_TYPE,
    PLAN_DISCOUNT,
    SOURCE_FILE,
    TO_CHAR(LOAD_DATE, 'YYYY-MM-DD HH24:MI:SS') AS load_date,
    CREATED_BY
FROM STECCOM_EXPENSES
WHERE CONTRACT_ID = 'SUB-54278652475'
  AND ICC_ID_IMEI = '300234069001680'
  AND (SERVICE IS NULL OR UPPER(TRIM(SERVICE)) != 'BROADBAND')
ORDER BY INVOICE_DATE DESC, TRANSACTION_DATE DESC, ID DESC;

PROMPT 
PROMPT ============================================================================
PROMPT 3. Проверка сумм по периодам (до и после дедупликации)
PROMPT ============================================================================

-- До дедупликации
SELECT 
    'Before deduplication' AS check_type,
    TO_CHAR(INVOICE_DATE, 'YYYYMM') AS bill_month,
    CONTRACT_ID,
    ICC_ID_IMEI AS imei,
    DESCRIPTION,
    COUNT(*) AS record_count,
    ROUND(SUM(AMOUNT), 2) AS total_amount,
    LISTAGG(AMOUNT, ' + ') WITHIN GROUP (ORDER BY ID) AS amounts
FROM STECCOM_EXPENSES
WHERE CONTRACT_ID = 'SUB-54278652475'
  AND ICC_ID_IMEI = '300234069001680'
  AND (SERVICE IS NULL OR UPPER(TRIM(SERVICE)) != 'BROADBAND')
GROUP BY TO_CHAR(INVOICE_DATE, 'YYYYMM'), CONTRACT_ID, ICC_ID_IMEI, DESCRIPTION
ORDER BY bill_month DESC, DESCRIPTION;

-- После дедупликации
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
    WHERE se.CONTRACT_ID = 'SUB-54278652475'
      AND se.ICC_ID_IMEI = '300234069001680'
      AND se.CONTRACT_ID IS NOT NULL
      AND se.ICC_ID_IMEI IS NOT NULL
      AND se.INVOICE_DATE IS NOT NULL
      AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
)
SELECT 
    'After deduplication (rn=1)' AS check_type,
    TO_CHAR(INVOICE_DATE, 'YYYYMM') AS bill_month,
    CONTRACT_ID,
    ICC_ID_IMEI AS imei,
    DESCRIPTION,
    COUNT(*) AS record_count,
    ROUND(SUM(AMOUNT), 2) AS total_amount
FROM unique_steccom_expenses
WHERE rn = 1
GROUP BY TO_CHAR(INVOICE_DATE, 'YYYYMM'), CONTRACT_ID, ICC_ID_IMEI, DESCRIPTION
ORDER BY bill_month DESC, DESCRIPTION;

PROMPT 
PROMPT ============================================================================
PROMPT 4. Статистика по файлам-источникам
PROMPT ============================================================================

SELECT 
    SOURCE_FILE,
    COUNT(*) AS record_count,
    ROUND(SUM(AMOUNT), 2) AS total_amount,
    MIN(TO_CHAR(LOAD_DATE, 'YYYY-MM-DD HH24:MI:SS')) AS first_load,
    MAX(TO_CHAR(LOAD_DATE, 'YYYY-MM-DD HH24:MI:SS')) AS last_load
FROM STECCOM_EXPENSES
WHERE CONTRACT_ID = 'SUB-54278652475'
  AND ICC_ID_IMEI = '300234069001680'
  AND (SERVICE IS NULL OR UPPER(TRIM(SERVICE)) != 'BROADBAND')
GROUP BY SOURCE_FILE
ORDER BY last_load DESC;

EXIT;





















