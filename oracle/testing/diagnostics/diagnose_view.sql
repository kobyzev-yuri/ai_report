-- Диагностический скрипт для проверки представления V_CONSOLIDATED_REPORT_WITH_BILLING
SET PAGESIZE 1000
SET LINESIZE 200

-- 1. Проверяем, существует ли представление
PROMPT ========================================
PROMPT 1. Проверка существования представления
PROMPT ========================================
SELECT 
    VIEW_NAME,
    TEXT_LENGTH,
    TEXT
FROM USER_VIEWS 
WHERE VIEW_NAME = 'V_CONSOLIDATED_REPORT_WITH_BILLING';

-- 2. Проверяем колонки в представлении
PROMPT ========================================
PROMPT 2. Список колонок в представлении
PROMPT ========================================
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    DATA_LENGTH,
    DATA_PRECISION,
    DATA_SCALE,
    NULLABLE
FROM USER_TAB_COLUMNS 
WHERE TABLE_NAME = 'V_CONSOLIDATED_REPORT_WITH_BILLING'
ORDER BY COLUMN_ID;

-- 3. Проверяем, есть ли колонка FEE_ADVANCE_CHARGE_PREVIOUS_MONTH
PROMPT ========================================
PROMPT 3. Поиск колонки FEE_ADVANCE_CHARGE_PREVIOUS_MONTH
PROMPT ========================================
SELECT 
    COLUMN_NAME,
    DATA_TYPE
FROM USER_TAB_COLUMNS 
WHERE TABLE_NAME = 'V_CONSOLIDATED_REPORT_WITH_BILLING'
  AND UPPER(COLUMN_NAME) LIKE '%ADVANCE%PREVIOUS%'
ORDER BY COLUMN_ID;

-- 4. Пробуем выполнить простой SELECT из представления (первые 5 строк)
PROMPT ========================================
PROMPT 4. Тестовый SELECT из представления (первые 5 строк)
PROMPT ========================================
SELECT 
    BILL_MONTH,
    IMEI,
    CONTRACT_ID,
    FEE_ADVANCE_CHARGE,
    FEE_ADVANCE_CHARGE_PREVIOUS_MONTH
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE ROWNUM <= 5;

-- 5. Проверяем структуру CTE steccom_fees
PROMPT ========================================
PROMPT 5. Проверка структуры steccom_fees (первые 5 строк)
PROMPT ========================================
SELECT 
    bill_month,
    contract_id,
    imei,
    fee_advance_charge
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
)
WHERE ROWNUM <= 5;

PROMPT ========================================
PROMPT Диагностика завершена
PROMPT ========================================

