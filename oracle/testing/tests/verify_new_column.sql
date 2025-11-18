-- Проверка новой колонки FEE_ADVANCE_CHARGE_PREVIOUS_MONTH
SET PAGESIZE 1000
SET LINESIZE 200

PROMPT ========================================
PROMPT Проверка колонки FEE_ADVANCE_CHARGE_PREVIOUS_MONTH
PROMPT ========================================

-- Проверяем, что колонка существует
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    DATA_LENGTH,
    NULLABLE
FROM USER_TAB_COLUMNS 
WHERE TABLE_NAME = 'V_CONSOLIDATED_REPORT_WITH_BILLING'
  AND UPPER(COLUMN_NAME) = 'FEE_ADVANCE_CHARGE_PREVIOUS_MONTH';

PROMPT ========================================
PROMPT Пример данных из представления (первые 10 строк с непустыми значениями)
PROMPT ========================================
SELECT 
    BILL_MONTH,
    IMEI,
    CONTRACT_ID,
    FEE_ADVANCE_CHARGE AS current_month_advance,
    FEE_ADVANCE_CHARGE_PREVIOUS_MONTH AS previous_month_advance
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE FEE_ADVANCE_CHARGE_PREVIOUS_MONTH IS NOT NULL
  AND FEE_ADVANCE_CHARGE_PREVIOUS_MONTH != 0
  AND ROWNUM <= 10
ORDER BY BILL_MONTH DESC;

PROMPT ========================================
PROMPT Статистика по колонке
PROMPT ========================================
SELECT 
    COUNT(*) AS total_rows,
    COUNT(FEE_ADVANCE_CHARGE_PREVIOUS_MONTH) AS non_null_rows,
    COUNT(CASE WHEN FEE_ADVANCE_CHARGE_PREVIOUS_MONTH != 0 THEN 1 END) AS non_zero_rows,
    SUM(FEE_ADVANCE_CHARGE_PREVIOUS_MONTH) AS total_sum,
    AVG(FEE_ADVANCE_CHARGE_PREVIOUS_MONTH) AS avg_value,
    MIN(FEE_ADVANCE_CHARGE_PREVIOUS_MONTH) AS min_value,
    MAX(FEE_ADVANCE_CHARGE_PREVIOUS_MONTH) AS max_value
FROM V_CONSOLIDATED_REPORT_WITH_BILLING;

PROMPT ========================================
PROMPT Проверка завершена
PROMPT ========================================

