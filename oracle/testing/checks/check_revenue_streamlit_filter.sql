-- Проверка фильтра отчёта «Доходы» (как в Streamlit get_revenue_report) vs сырой VIEW.
-- Запуск через туннель:
--   sqlplus billing7/...@//127.0.0.1:1521/bm7 @oracle/testing/checks/check_revenue_streamlit_filter.sql
--
-- Параметры: измените DEFINE ниже при необходимости.

SET PAGESIZE 200
SET LINESIZE 200
SET VERIFY OFF

DEFINE imei = '300234069207960'
DEFINE period_yyyymm = '2026-02'

PROMPT === 1) Сырой VIEW (должна быть строка с REVENUE_SUSPEND_ABON при только приостановке) ===
SELECT SERVICE_ID, CONTRACT_ID, IMEI, CUSTOMER_NAME, PERIOD_YYYYMM,
       REVENUE_SBD_ABON, REVENUE_SUSPEND_ABON, REVENUE_MSG_ABON, REVENUE_TOTAL
FROM V_REVENUE_FROM_INVOICES
WHERE IMEI = '&imei' AND PERIOD_YYYYMM = '&period_yyyymm';

PROMPT === 2) Старый фильтр (INNER JOIN по v.SERVICE_ID) — мог «съесть» строку, если главная 9002 закрыта ===
SELECT COUNT(*) AS cnt_old_filter FROM V_REVENUE_FROM_INVOICES v
JOIN SERVICES s ON v.SERVICE_ID = s.SERVICE_ID
  AND (s.CLOSE_DATE IS NULL OR s.CLOSE_DATE > LAST_DAY(TO_DATE(v.PERIOD_YYYYMM||'-01','YYYY-MM-DD')))
WHERE v.IMEI = '&imei' AND v.PERIOD_YYYYMM = '&period_yyyymm';

PROMPT === 3) Новый фильтр (LEFT JOIN + EXISTS по любой активной услуге с начислениями в периоде) ===
SELECT v.SERVICE_ID, v.CONTRACT_ID, v.IMEI, v.PERIOD_YYYYMM, v.REVENUE_SUSPEND_ABON, v.REVENUE_TOTAL
FROM V_REVENUE_FROM_INVOICES v
LEFT JOIN SERVICES s ON v.SERVICE_ID = s.SERVICE_ID
  AND (s.CLOSE_DATE IS NULL OR s.CLOSE_DATE > LAST_DAY(TO_DATE(v.PERIOD_YYYYMM||'-01','YYYY-MM-DD')))
WHERE v.IMEI = '&imei' AND v.PERIOD_YYYYMM = '&period_yyyymm'
  AND (
    s.SERVICE_ID IS NOT NULL
    OR EXISTS (
      SELECT 1 FROM BM_INVOICE_ITEM ii2
      JOIN SERVICES s2 ON ii2.SERVICE_ID = s2.SERVICE_ID
      JOIN BM_PERIOD p ON ii2.PERIOD_ID = p.PERIOD_ID
      WHERE s2.VSAT = v.IMEI
        AND TO_CHAR(p.START_DATE,'YYYY-MM') = v.PERIOD_YYYYMM
        AND (s2.CLOSE_DATE IS NULL OR s2.CLOSE_DATE > LAST_DAY(TO_DATE(v.PERIOD_YYYYMM||'-01','YYYY-MM-DD')))
    )
  );

PROMPT === Готово ===
