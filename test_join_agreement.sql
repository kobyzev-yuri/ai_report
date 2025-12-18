-- ============================================================================
-- Тест JOIN'ов для AGREEMENT_NUMBER в view
-- ============================================================================

SET LINESIZE 200
SET PAGESIZE 100

PROMPT ============================================================================
PROMPT Тест: Проверка JOIN imei_service_ext_info с данными из cor
PROMPT ============================================================================

WITH cor AS (
    SELECT 
        IMEI,
        CONTRACT_ID,
        BILL_MONTH
    FROM V_CONSOLIDATED_OVERAGE_REPORT
    WHERE IMEI = '300434069401140'
      AND ROWNUM <= 1
),
imei_service_ext_info AS (
    SELECT 
        se_ranked.VALUE AS IMEI,
        se_ranked.AGREEMENT_NUMBER
    FROM (
        SELECT 
            se.VALUE,
            a.DESCRIPTION AS AGREEMENT_NUMBER,
            ROW_NUMBER() OVER (
                PARTITION BY se.VALUE 
                ORDER BY 
                    CASE WHEN se.DATE_END IS NULL THEN 0 ELSE 1 END,
                    se.DATE_BEG DESC NULLS LAST,
                    se.SERVICE_ID DESC
            ) AS rn
        FROM SERVICES_EXT se
        JOIN SERVICES s ON se.SERVICE_ID = s.SERVICE_ID
        JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
        WHERE se.VALUE IS NOT NULL
    ) se_ranked
    WHERE se_ranked.rn = 1
)
SELECT 
    cor.IMEI AS cor_imei,
    LENGTH(cor.IMEI) AS cor_imei_len,
    imei_service_ext_info.IMEI AS join_imei,
    LENGTH(imei_service_ext_info.IMEI) AS join_imei_len,
    TRIM(cor.IMEI) AS cor_imei_trim,
    TRIM(imei_service_ext_info.IMEI) AS join_imei_trim,
    CASE WHEN TRIM(cor.IMEI) = TRIM(imei_service_ext_info.IMEI) THEN 'MATCH' ELSE 'NO MATCH' END AS match_status,
    imei_service_ext_info.AGREEMENT_NUMBER
FROM cor
LEFT JOIN imei_service_ext_info ON TRIM(imei_service_ext_info.IMEI) = TRIM(cor.IMEI);

PROMPT
PROMPT ============================================================================
PROMPT Тест: Проверка COALESCE для AGREEMENT_NUMBER
PROMPT ============================================================================

SELECT 
    cor.IMEI,
    cor.CONTRACT_ID,
    v.AGREEMENT_NUMBER AS v_agreement,
    imei_service_ext_info.AGREEMENT_NUMBER AS imei_ext_agreement,
    imei_service_info.AGREEMENT_NUMBER AS imei_vsat_agreement,
    COALESCE(
        v.AGREEMENT_NUMBER,
        imei_service_ext_info.AGREEMENT_NUMBER,
        imei_service_info.AGREEMENT_NUMBER
    ) AS final_agreement
FROM (
    SELECT 
        IMEI,
        CONTRACT_ID,
        BILL_MONTH
    FROM V_CONSOLIDATED_OVERAGE_REPORT
    WHERE IMEI = '300434069401140'
      AND ROWNUM <= 1
) cor
LEFT JOIN (
    SELECT v1.SERVICE_ID, v1.CONTRACT_ID, v1.IMEI, v1.AGREEMENT_NUMBER
    FROM (
        SELECT v0.*,
               ROW_NUMBER() OVER (PARTITION BY v0.CONTRACT_ID ORDER BY v0.SERVICE_ID DESC NULLS LAST) AS rn
        FROM V_IRIDIUM_SERVICES_INFO v0
    ) v1
    WHERE v1.rn = 1
) v ON cor.CONTRACT_ID = v.CONTRACT_ID
LEFT JOIN (
    SELECT 
        se_ranked.VALUE AS IMEI,
        se_ranked.AGREEMENT_NUMBER
    FROM (
        SELECT 
            se.VALUE,
            a.DESCRIPTION AS AGREEMENT_NUMBER,
            ROW_NUMBER() OVER (
                PARTITION BY se.VALUE 
                ORDER BY 
                    CASE WHEN se.DATE_END IS NULL THEN 0 ELSE 1 END,
                    se.DATE_BEG DESC NULLS LAST,
                    se.SERVICE_ID DESC
            ) AS rn
        FROM SERVICES_EXT se
        JOIN SERVICES s ON se.SERVICE_ID = s.SERVICE_ID
        JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
        WHERE se.VALUE IS NOT NULL
    ) se_ranked
    WHERE se_ranked.rn = 1
) imei_service_ext_info ON TRIM(imei_service_ext_info.IMEI) = TRIM(cor.IMEI)
LEFT JOIN (
    SELECT 
        s.VSAT AS IMEI,
        MAX(a.DESCRIPTION) AS AGREEMENT_NUMBER
    FROM SERVICES s
    JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
    WHERE s.VSAT IS NOT NULL
    GROUP BY s.VSAT
) imei_service_info ON TRIM(imei_service_info.IMEI) = TRIM(cor.IMEI);

