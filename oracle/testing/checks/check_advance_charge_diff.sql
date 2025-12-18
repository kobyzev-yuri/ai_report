-- ============================================================================
-- check_advance_charge_diff.sql
-- Финальная сверка Advance Charge за октябрь 2025
-- Цель: найти, откуда берутся лишние $34 относительно инвойса (23 834.50)
-- ============================================================================

SET PAGESIZE 200
SET FEEDBACK ON
SET HEADING ON
SET LINESIZE 2000

PROMPT === 1. Общая сумма в отчете (ожидаем 23 834.50) ===
SELECT 
    ROUND(SUM(v.FEE_ADVANCE_CHARGE), 2) AS total_advance_charge,
    23834.5 AS expected_amount,
    ROUND(SUM(v.FEE_ADVANCE_CHARGE) - 23834.5, 2) AS difference
FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
WHERE v.FINANCIAL_PERIOD = '2025-10'
  AND v.FEE_ADVANCE_CHARGE > 0;

PROMPT
PROMPT === 2. Сырые строки Advance Charge за октябрь из STECCOM_EXPENSES ===
PROMPT (при необходимости можно сохранить в файл и сверить с детализацией Iridium)

SPOOL steccom_advance_2025_10.txt

SELECT 
    se.SOURCE_FILE,
    TO_CHAR(se.INVOICE_DATE, 'YYYY-MM-DD')     AS invoice_date,
    TO_CHAR(se.TRANSACTION_DATE, 'YYYY-MM-DD') AS transaction_date,
    se.CONTRACT_ID,
    se.ICC_ID_IMEI                             AS imei,
    se.DESCRIPTION,
    se.AMOUNT
FROM STECCOM_EXPENSES se
WHERE se.CONTRACT_ID IS NOT NULL
  AND se.ICC_ID_IMEI IS NOT NULL
  AND se.INVOICE_DATE IS NOT NULL
  AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
  AND UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%'
  AND TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') = '202510'
ORDER BY se.SOURCE_FILE, se.CONTRACT_ID, se.ICC_ID_IMEI, se.TRANSACTION_DATE, se.AMOUNT;

SPOOL OFF;

PROMPT
PROMPT === 3. Суммы Advance Charge по CONTRACT_ID+IMEI в отчете ===

WITH view_agg AS (
  SELECT 
      v.CONTRACT_ID,
      v.IMEI,
      ROUND(SUM(v.FEE_ADVANCE_CHARGE), 2) AS total_advance
  FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
  WHERE v.FINANCIAL_PERIOD = '2025-10'
    AND v.FEE_ADVANCE_CHARGE > 0
  GROUP BY v.CONTRACT_ID, v.IMEI
)
SELECT *
FROM view_agg
ORDER BY total_advance DESC;

PROMPT
PROMPT === 4. Суммы Advance Charge по CONTRACT_ID+IMEI в сырых данных и в отчете (diff) ===
PROMPT (НУЖНО ПРИСЛАТЬ ВЫВОД ИМЕННО ЭТОГО БЛОКА)

WITH steccom_raw AS (
  SELECT 
      se.CONTRACT_ID,
      se.ICC_ID_IMEI AS imei,
      ROUND(SUM(se.AMOUNT), 2) AS total_advance
  FROM STECCOM_EXPENSES se
  WHERE se.CONTRACT_ID IS NOT NULL
    AND se.ICC_ID_IMEI IS NOT NULL
    AND se.INVOICE_DATE IS NOT NULL
    AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
    AND UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%'
    AND TO_CHAR(se.TRANSACTION_DATE, 'YYYYMM') = '202510'
  GROUP BY se.CONTRACT_ID, se.ICC_ID_IMEI
),
view_agg AS (
  SELECT 
      v.CONTRACT_ID,
      v.IMEI,
      ROUND(SUM(v.FEE_ADVANCE_CHARGE), 2) AS total_advance
  FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
  WHERE v.FINANCIAL_PERIOD = '2025-10'
    AND v.FEE_ADVANCE_CHARGE > 0
  GROUP BY v.CONTRACT_ID, v.IMEI
)
SELECT 
    COALESCE(s.CONTRACT_ID, v.CONTRACT_ID)        AS CONTRACT_ID,
    COALESCE(s.imei, v.IMEI)                      AS IMEI,
    s.total_advance                               AS total_from_steccom,
    v.total_advance                               AS total_from_view,
    NVL(v.total_advance,0) - NVL(s.total_advance,0) AS diff_view_minus_steccom
FROM steccom_raw s
FULL OUTER JOIN view_agg v
  ON s.CONTRACT_ID = v.CONTRACT_ID
 AND s.imei        = v.IMEI
WHERE ABS(NVL(v.total_advance,0) - NVL(s.total_advance,0)) > 0.001
ORDER BY ABS(NVL(v.total_advance,0) - NVL(s.total_advance,0)) DESC;

EXIT;





