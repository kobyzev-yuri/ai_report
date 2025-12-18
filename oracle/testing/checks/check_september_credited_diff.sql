-- ============================================================================
-- check_september_credited_diff.sql
-- Сентябрь 2025: разбор расхождений по полю FEE_CREDITED (Credited)
-- ============================================================================

SET PAGESIZE 200
SET FEEDBACK ON
SET HEADING ON
SET LINESIZE 2000

PROMPT === 1. Сводная сумма Credited за сентябрь 2025 (отчетный период 2025-09) ===

SELECT 
    ROUND(SUM(v.FEE_CREDITED), 2) AS total_credited,
    -567.52 AS expected_credited,
    ROUND(SUM(v.FEE_CREDITED) - (-567.52), 2) AS diff_credited
FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
WHERE v.FINANCIAL_PERIOD = '2025-09';

PROMPT
PROMPT === 2. Credited по CONTRACT_ID+IMEI в отчете ===

WITH view_credited AS (
  SELECT 
      v.CONTRACT_ID,
      v.IMEI,
      ROUND(SUM(v.FEE_CREDITED), 2) AS total_credited
  FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
  WHERE v.FINANCIAL_PERIOD = '2025-09'
    AND v.FEE_CREDITED <> 0
  GROUP BY v.CONTRACT_ID, v.IMEI
)
SELECT *
FROM view_credited
ORDER BY total_credited;

PROMPT
PROMPT === 3. Credited по CONTRACT_ID+IMEI в сырых данных STECCOM_EXPENSES ===

WITH steccom_credited_raw AS (
  SELECT 
      se.CONTRACT_ID,
      se.ICC_ID_IMEI AS IMEI,
      ROUND(SUM(se.AMOUNT), 2) AS total_credited
  FROM STECCOM_EXPENSES se
  WHERE se.CONTRACT_ID IS NOT NULL
    AND se.ICC_ID_IMEI IS NOT NULL
    AND se.INVOICE_DATE IS NOT NULL
    AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
    AND UPPER(TRIM(se.DESCRIPTION)) LIKE '%CREDITED%'
    -- Сентябрьский период: берем записи, которые относятся к авансам за сентябрь
    -- (по аналогии с FEE_CREDITED в view, если там используется INVOICE_DATE-1 месяц,
    -- при необходимости это условие можно скорректировать)
    AND TO_CHAR(ADD_MONTHS(se.INVOICE_DATE, -1), 'YYYYMM') = '202509'
  GROUP BY se.CONTRACT_ID, se.ICC_ID_IMEI
)
SELECT *
FROM steccom_credited_raw
ORDER BY total_credited;

PROMPT
PROMPT === 4. Diff по Credited: STECCOM_EXPENSES vs отчет (НУЖНО ПРИСЛАТЬ ЭТОТ БЛОК) ===

WITH steccom_credited_raw AS (
  SELECT 
      se.CONTRACT_ID,
      se.ICC_ID_IMEI AS IMEI,
      ROUND(SUM(se.AMOUNT), 2) AS total_credited
  FROM STECCOM_EXPENSES se
  WHERE se.CONTRACT_ID IS NOT NULL
    AND se.ICC_ID_IMEI IS NOT NULL
    AND se.INVOICE_DATE IS NOT NULL
    AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
    AND UPPER(TRIM(se.DESCRIPTION)) LIKE '%CREDITED%'
    AND TO_CHAR(ADD_MONTHS(se.INVOICE_DATE, -1), 'YYYYMM') = '202509'
  GROUP BY se.CONTRACT_ID, se.ICC_ID_IMEI
),
view_credited AS (
  SELECT 
      v.CONTRACT_ID,
      v.IMEI,
      ROUND(SUM(v.FEE_CREDITED), 2) AS total_credited
  FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
  WHERE v.FINANCIAL_PERIOD = '2025-09'
    AND v.FEE_CREDITED <> 0
  GROUP BY v.CONTRACT_ID, v.IMEI
)
SELECT 
    COALESCE(s.CONTRACT_ID, v.CONTRACT_ID) AS CONTRACT_ID,
    COALESCE(s.IMEI, v.IMEI)               AS IMEI,
    s.total_credited                       AS credited_from_steccom,
    v.total_credited                       AS credited_from_view,
    NVL(v.total_credited,0) - NVL(s.total_credited,0) AS diff_view_minus_steccom
FROM steccom_credited_raw s
FULL OUTER JOIN view_credited v
  ON s.CONTRACT_ID = v.CONTRACT_ID
 AND s.IMEI        = v.IMEI
WHERE ABS(NVL(v.total_credited,0) - NVL(s.total_credited,0)) > 0.001
ORDER BY ABS(NVL(v.total_credited,0) - NVL(s.total_credited,0)) DESC;

EXIT;





