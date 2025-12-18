-- ============================================================================
-- check_september_overage_diff.sql
-- Сентябрь 2025: разбор расхождений по Calculated Overage и Total Amount
-- ============================================================================

SET PAGESIZE 200
SET FEEDBACK ON
SET HEADING ON
SET LINESIZE 2000

PROMPT === 1. Сводные суммы за сентябрь 2025 (отчетный период 2025-09) ===

SELECT 
    ROUND(SUM(v.CALCULATED_OVERAGE), 2) AS total_calculated_overage,
    5069.40 AS expected_calculated_overage,
    ROUND(SUM(v.CALCULATED_OVERAGE) - 5069.40, 2) AS diff_calculated_overage
FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
WHERE v.FINANCIAL_PERIOD = '2025-09';

SELECT 
    ROUND(SUM(v.SPNET_TOTAL_AMOUNT), 2) AS total_spnet_amount,
    650.00 AS expected_spnet_amount,
    ROUND(SUM(v.SPNET_TOTAL_AMOUNT) - 650.00, 2) AS diff_spnet_amount
FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
WHERE v.FINANCIAL_PERIOD = '2025-09';

PROMPT
PROMPT === 2. Calculated Overage по CONTRACT_ID+IMEI в отчете ===

WITH view_overage AS (
  SELECT 
      v.CONTRACT_ID,
      v.IMEI,
      ROUND(SUM(v.CALCULATED_OVERAGE), 2) AS total_overage
  FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
  WHERE v.FINANCIAL_PERIOD = '2025-09'
  GROUP BY v.CONTRACT_ID, v.IMEI
)
SELECT *
FROM view_overage
ORDER BY total_overage DESC;

PROMPT
PROMPT === 3. Calculated Overage по CONTRACT_ID+IMEI в исходных данных SPNet ===
PROMPT (используем V_CONSOLIDATED_OVERAGE_REPORT как источник "сырых" сумм) 

WITH spnet_overage AS (
  SELECT 
      cor.CONTRACT_ID,
      cor.IMEI,
      ROUND(SUM(cor.CALCULATED_OVERAGE), 2) AS total_overage
  FROM V_CONSOLIDATED_OVERAGE_REPORT cor
  -- В этой вьюшке есть только BILL_MONTH, поэтому для сентябрьского
  -- отчетного периода (2025-09) берем BILL_MONTH = '202509'
  WHERE cor.BILL_MONTH = '202509'
  GROUP BY cor.CONTRACT_ID, cor.IMEI
)
SELECT *
FROM spnet_overage
ORDER BY total_overage DESC;

PROMPT
PROMPT === 4. Diff по Calculated Overage: SPNet vs отчет (НУЖНО ПРИСЛАТЬ ЭТОТ БЛОК) ===

WITH spnet_overage AS (
  SELECT 
      cor.CONTRACT_ID,
      cor.IMEI,
      ROUND(SUM(cor.CALCULATED_OVERAGE), 2) AS total_overage
  FROM V_CONSOLIDATED_OVERAGE_REPORT cor
  WHERE cor.BILL_MONTH = '202509'
  GROUP BY cor.CONTRACT_ID, cor.IMEI
),
view_overage AS (
  SELECT 
      v.CONTRACT_ID,
      v.IMEI,
      ROUND(SUM(v.CALCULATED_OVERAGE), 2) AS total_overage
  FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
  WHERE v.FINANCIAL_PERIOD = '2025-09'
  GROUP BY v.CONTRACT_ID, v.IMEI
)
SELECT 
    COALESCE(s.CONTRACT_ID, v.CONTRACT_ID) AS CONTRACT_ID,
    COALESCE(s.IMEI, v.IMEI)               AS IMEI,
    s.total_overage                        AS overage_from_spnet,
    v.total_overage                        AS overage_from_view,
    NVL(v.total_overage,0) - NVL(s.total_overage,0) AS diff_view_minus_spnet
FROM spnet_overage s
FULL OUTER JOIN view_overage v
  ON s.CONTRACT_ID = v.CONTRACT_ID
 AND s.IMEI        = v.IMEI
WHERE ABS(NVL(v.total_overage,0) - NVL(s.total_overage,0)) > 0.001
ORDER BY ABS(NVL(v.total_overage,0) - NVL(s.total_overage,0)) DESC;

PROMPT
PROMPT === 5. Total Amount (SPNET_TOTAL_AMOUNT) по CONTRACT_ID+IMEI: SPNet vs отчет ===
PROMPT (по аналогии, чтобы понять источники расхождения 1300 vs 650) 

WITH spnet_amount AS (
  SELECT 
      cor.CONTRACT_ID,
      cor.IMEI,
      ROUND(SUM(cor.SPNET_TOTAL_AMOUNT), 2) AS total_amount
  FROM V_CONSOLIDATED_OVERAGE_REPORT cor
  WHERE cor.BILL_MONTH = '202509'
  GROUP BY cor.CONTRACT_ID, cor.IMEI
),
view_amount AS (
  SELECT 
      v.CONTRACT_ID,
      v.IMEI,
      ROUND(SUM(v.SPNET_TOTAL_AMOUNT), 2) AS total_amount
  FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
  WHERE v.FINANCIAL_PERIOD = '2025-09'
  GROUP BY v.CONTRACT_ID, v.IMEI
)
SELECT 
    COALESCE(s.CONTRACT_ID, v.CONTRACT_ID) AS CONTRACT_ID,
    COALESCE(s.IMEI, v.IMEI)               AS IMEI,
    s.total_amount                         AS amount_from_spnet,
    v.total_amount                         AS amount_from_view,
    NVL(v.total_amount,0) - NVL(s.total_amount,0) AS diff_view_minus_spnet
FROM spnet_amount s
FULL OUTER JOIN view_amount v
  ON s.CONTRACT_ID = v.CONTRACT_ID
 AND s.IMEI        = v.IMEI
WHERE ABS(NVL(v.total_amount,0) - NVL(s.total_amount,0)) > 0.001
ORDER BY ABS(NVL(v.total_amount,0) - NVL(s.total_amount,0)) DESC;

EXIT;


