-- ============================================================================
-- Тест маппинга тарифных планов для проблемной записи
-- Проверка: SUB-38494828046, IMEI: 300215061804050, период: 202510
-- ============================================================================

SET PAGESIZE 1000
SET LINESIZE 200

PROMPT ============================================================================
PROMPT 1. Проверка данных в SPNET_TRAFFIC для текущего периода (202510)
PROMPT ============================================================================
SELECT 
    IMEI,
    CONTRACT_ID,
    BILL_MONTH,
    PLAN_NAME,
    COUNT(*) AS records_count
FROM SPNET_TRAFFIC
WHERE CONTRACT_ID = 'SUB-38494828046'
  AND IMEI = '300215061804050'
  AND BILL_MONTH = 202510
GROUP BY IMEI, CONTRACT_ID, BILL_MONTH, PLAN_NAME;

PROMPT
PROMPT ============================================================================
PROMPT 2. Проверка планов в других периодах для этого CONTRACT_ID
PROMPT ============================================================================
SELECT 
    CONTRACT_ID,
    PLAN_NAME,
    COUNT(*) AS records_count,
    LISTAGG(DISTINCT TO_CHAR(BILL_MONTH), ', ') WITHIN GROUP (ORDER BY BILL_MONTH) AS periods
FROM SPNET_TRAFFIC
WHERE CONTRACT_ID = 'SUB-38494828046'
  AND PLAN_NAME IS NOT NULL
  AND PLAN_NAME != ''
GROUP BY CONTRACT_ID, PLAN_NAME
ORDER BY records_count DESC;

PROMPT
PROMPT ============================================================================
PROMPT 3. Проверка планов в других периодах для этого IMEI
PROMPT ============================================================================
SELECT 
    IMEI,
    PLAN_NAME,
    COUNT(*) AS records_count,
    LISTAGG(DISTINCT TO_CHAR(BILL_MONTH), ', ') WITHIN GROUP (ORDER BY BILL_MONTH) AS periods
FROM SPNET_TRAFFIC
WHERE IMEI = '300215061804050'
  AND PLAN_NAME IS NOT NULL
  AND PLAN_NAME != ''
GROUP BY IMEI, PLAN_NAME
ORDER BY records_count DESC;

PROMPT
PROMPT ============================================================================
PROMPT 4. Тест маппинга contract_plan_mapping (как в view)
PROMPT ============================================================================
WITH contract_plan_mapping AS (
    SELECT 
        CONTRACT_ID,
        PLAN_NAME
    FROM (
        SELECT 
            st.CONTRACT_ID,
            st.PLAN_NAME,
            COUNT(*) AS plan_count,
            ROW_NUMBER() OVER (PARTITION BY st.CONTRACT_ID ORDER BY COUNT(*) DESC, st.PLAN_NAME) AS rn
        FROM SPNET_TRAFFIC st
        WHERE st.CONTRACT_ID IS NOT NULL
          AND st.PLAN_NAME IS NOT NULL
          AND st.PLAN_NAME != ''
        GROUP BY st.CONTRACT_ID, st.PLAN_NAME
    )
    WHERE rn = 1
)
SELECT 
    cpm.CONTRACT_ID,
    cpm.PLAN_NAME
FROM contract_plan_mapping cpm
WHERE cpm.CONTRACT_ID = 'SUB-38494828046';

PROMPT
PROMPT ============================================================================
PROMPT 5. Тест маппинга imei_plan_mapping (как в view)
PROMPT ============================================================================
WITH imei_plan_mapping AS (
    SELECT 
        IMEI,
        PLAN_NAME
    FROM (
        SELECT 
            st.IMEI,
            st.PLAN_NAME,
            COUNT(*) AS plan_count,
            ROW_NUMBER() OVER (PARTITION BY st.IMEI ORDER BY COUNT(*) DESC, st.PLAN_NAME) AS rn
        FROM SPNET_TRAFFIC st
        WHERE st.IMEI IS NOT NULL
          AND st.PLAN_NAME IS NOT NULL
          AND st.PLAN_NAME != ''
        GROUP BY st.IMEI, st.PLAN_NAME
    )
    WHERE rn = 1
)
SELECT 
    ipm.IMEI,
    ipm.PLAN_NAME
FROM imei_plan_mapping ipm
WHERE ipm.IMEI = '300215061804050';

PROMPT
PROMPT ============================================================================
PROMPT 6. Проверка результата в V_CONSOLIDATED_OVERAGE_REPORT
PROMPT ============================================================================
SELECT 
    IMEI,
    CONTRACT_ID,
    BILL_MONTH,
    PLAN_NAME,
    SPNET_TOTAL_AMOUNT,
    STECCOM_TOTAL_AMOUNT
FROM V_CONSOLIDATED_OVERAGE_REPORT
WHERE CONTRACT_ID = 'SUB-38494828046'
  AND IMEI = '300215061804050'
  AND BILL_MONTH = '202510';

PROMPT
PROMPT ============================================================================
PROMPT 7. Проверка результата в V_CONSOLIDATED_OVERAGE_REPORT
PROMPT ============================================================================
SELECT 
    IMEI,
    CONTRACT_ID,
    BILL_MONTH,
    PLAN_NAME,
    SPNET_TOTAL_AMOUNT,
    STECCOM_TOTAL_AMOUNT
FROM V_CONSOLIDATED_OVERAGE_REPORT
WHERE CONTRACT_ID = 'SUB-38494828046'
  AND IMEI = '300215061804050'
  AND BILL_MONTH = '202510';

PROMPT
PROMPT ============================================================================
PROMPT 8. Тест полной логики маппинга (как в view)
PROMPT ============================================================================
WITH contract_plan_mapping AS (
    SELECT 
        CONTRACT_ID,
        PLAN_NAME
    FROM (
        SELECT 
            st.CONTRACT_ID,
            st.PLAN_NAME,
            COUNT(*) AS plan_count,
            ROW_NUMBER() OVER (PARTITION BY st.CONTRACT_ID ORDER BY COUNT(*) DESC, st.PLAN_NAME) AS rn
        FROM SPNET_TRAFFIC st
        WHERE st.CONTRACT_ID IS NOT NULL
          AND st.PLAN_NAME IS NOT NULL
          AND st.PLAN_NAME != ''
        GROUP BY st.CONTRACT_ID, st.PLAN_NAME
    )
    WHERE rn = 1
),
imei_plan_mapping AS (
    SELECT 
        IMEI,
        PLAN_NAME
    FROM (
        SELECT 
            st.IMEI,
            st.PLAN_NAME,
            COUNT(*) AS plan_count,
            ROW_NUMBER() OVER (PARTITION BY st.IMEI ORDER BY COUNT(*) DESC, st.PLAN_NAME) AS rn
        FROM SPNET_TRAFFIC st
        WHERE st.IMEI IS NOT NULL
          AND st.PLAN_NAME IS NOT NULL
          AND st.PLAN_NAME != ''
        GROUP BY st.IMEI, st.PLAN_NAME
    )
    WHERE rn = 1
),
spnet_data AS (
    SELECT 
        ov.IMEI,
        ov.CONTRACT_ID,
        LPAD(TO_CHAR(MOD(ov.BILL_MONTH, 10000)), 4, '0') || LPAD(TO_CHAR(TRUNC(ov.BILL_MONTH / 10000)), 2, '0') AS BILL_MONTH,
        ov.PLAN_NAME
    FROM V_SPNET_OVERAGE_ANALYSIS ov
    WHERE ov.CONTRACT_ID = 'SUB-38494828046'
      AND ov.IMEI = '300215061804050'
      AND LPAD(TO_CHAR(MOD(ov.BILL_MONTH, 10000)), 4, '0') || LPAD(TO_CHAR(TRUNC(ov.BILL_MONTH / 10000)), 2, '0') = '202510'
    GROUP BY 
        ov.IMEI,
        ov.CONTRACT_ID,
        LPAD(TO_CHAR(MOD(ov.BILL_MONTH, 10000)), 4, '0') || LPAD(TO_CHAR(TRUNC(ov.BILL_MONTH / 10000)), 2, '0'),
        ov.PLAN_NAME
)
SELECT 
    sp.IMEI,
    sp.CONTRACT_ID,
    sp.BILL_MONTH,
    sp.PLAN_NAME AS spnet_plan_name,
    cpm.PLAN_NAME AS contract_mapping_plan,
    ipm.PLAN_NAME AS imei_mapping_plan,
    NVL(
        NULLIF(TRIM(sp.PLAN_NAME), ''),
        NVL(
            NULLIF(TRIM(cpm.PLAN_NAME), ''),
            NULLIF(TRIM(ipm.PLAN_NAME), '')
        )
    ) AS final_plan_name
FROM spnet_data sp
LEFT JOIN contract_plan_mapping cpm
    ON sp.CONTRACT_ID = cpm.CONTRACT_ID
LEFT JOIN imei_plan_mapping ipm
    ON sp.IMEI = ipm.IMEI;

PROMPT
PROMPT ============================================================================
PROMPT 9. Проверка структуры view (DDL) - первые 2000 символов
PROMPT ============================================================================
SELECT SUBSTR(DBMS_METADATA.GET_DDL('VIEW', 'V_CONSOLIDATED_OVERAGE_REPORT'), 1, 2000) AS view_ddl FROM DUAL;

