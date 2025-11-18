-- Тестовый запрос, аналогичный тому, что использует Streamlit
SET PAGESIZE 1000
SET LINESIZE 200

PROMPT ========================================
PROMPT Тестовый запрос Streamlit для периода 2025-09
PROMPT ========================================
SELECT 
    v.FINANCIAL_PERIOD AS "Отчетный Период",
    v.BILL_MONTH AS "Bill Month",
    v.IMEI AS "IMEI",
    v.CONTRACT_ID AS "Contract ID",
    COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '') AS "Organization/Person",
    v.CODE_1C AS "Code 1C",
    v.SERVICE_ID AS "Service ID",
    v.AGREEMENT_NUMBER AS "Agreement #",
    COALESCE(v.PLAN_NAME, '') AS "Plan Name",
    COALESCE(v.STECCOM_PLAN_NAME_MONTHLY, '') AS "Plan Monthly",
    COALESCE(v.STECCOM_PLAN_NAME_SUSPENDED, '') AS "Plan Suspended",
    ROUND(v.TRAFFIC_USAGE_BYTES / 1000, 2) AS "Traffic Usage (KB)",
    v.EVENTS_COUNT AS "Events (Count)",
    v.DATA_USAGE_EVENTS AS "Data Events",
    v.MAILBOX_EVENTS AS "Mailbox Events",
    v.REGISTRATION_EVENTS AS "Registration Events",
    CASE 
        WHEN UPPER(COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '')) LIKE '%СТЭК.КОМ%' 
             OR UPPER(COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '')) LIKE '%СТЭККОМ%'
             OR UPPER(COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '')) LIKE '%STECCOM%'
        THEN 0
        ELSE v.OVERAGE_KB
    END AS "Overage (KB)",
    CASE 
        WHEN UPPER(COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '')) LIKE '%СТЭК.КОМ%' 
             OR UPPER(COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '')) LIKE '%СТЭККОМ%'
             OR UPPER(COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '')) LIKE '%STECCOM%'
        THEN 0
        ELSE v.CALCULATED_OVERAGE
    END AS "Calculated Overage ($)",
    NVL(v.FEE_ACTIVATION_FEE, 0) AS "Activation Fee",
    NVL(v.FEE_ADVANCE_CHARGE, 0) AS "Advance Charge",
    NVL(v.FEE_ADVANCE_CHARGE_PREVIOUS_MONTH, 0) AS "Advance Charge Previous Month",
    NVL(v.FEE_CREDIT, 0) AS "Credit",
    NVL(v.FEE_CREDITED, 0) AS "Credited",
    NVL(v.FEE_PRORATED, 0) AS "Prorated",
    NVL(v.FEES_TOTAL, 0) AS "Fees Total ($)"
FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
WHERE v.BILL_MONTH = '2025-09'
  AND v.IMEI = '300234069308010'
  AND v.CONTRACT_ID = 'SUB-26089990228'
ORDER BY v.BILL_MONTH DESC, "Calculated Overage ($)" DESC NULLS LAST;

PROMPT ========================================
PROMPT Проверка всех записей с ненулевыми Fees для периода 2025-09
PROMPT ========================================
SELECT 
    v.BILL_MONTH,
    v.IMEI,
    v.CONTRACT_ID,
    NVL(v.FEE_ACTIVATION_FEE, 0) AS "Activation Fee",
    NVL(v.FEE_ADVANCE_CHARGE, 0) AS "Advance Charge",
    NVL(v.FEE_CREDIT, 0) AS "Credit",
    NVL(v.FEE_CREDITED, 0) AS "Credited",
    NVL(v.FEE_PRORATED, 0) AS "Prorated",
    NVL(v.FEES_TOTAL, 0) AS "Fees Total"
FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
WHERE v.BILL_MONTH = '2025-09'
  AND (NVL(v.FEE_ACTIVATION_FEE, 0) != 0 
       OR NVL(v.FEE_ADVANCE_CHARGE, 0) != 0 
       OR NVL(v.FEE_CREDIT, 0) != 0 
       OR NVL(v.FEE_CREDITED, 0) != 0 
       OR NVL(v.FEE_PRORATED, 0) != 0
       OR NVL(v.FEES_TOTAL, 0) != 0)
ORDER BY v.BILL_MONTH DESC, NVL(v.FEES_TOTAL, 0) DESC;

PROMPT ========================================
PROMPT Проверка завершена
PROMPT ========================================

