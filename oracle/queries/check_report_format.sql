-- Запрос в формате отчета с правильной агрегацией fees
SELECT 
    -- Bill Month в формате YYYY-MM
    CASE 
        WHEN st.BILL_MONTH >= 200000 THEN
            SUBSTR(TO_CHAR(st.BILL_MONTH), 1, 4) || '-' || SUBSTR(TO_CHAR(st.BILL_MONTH), 5, 2)
        ELSE
            LPAD(TO_CHAR(MOD(st.BILL_MONTH, 10000)), 4, '0') || '-' || LPAD(TO_CHAR(TRUNC(st.BILL_MONTH / 10000)), 2, '0')
    END AS "Bill Month",
    st.IMEI AS "IMEI",
    st.CONTRACT_ID AS "Contract ID",
    -- Трафик и события
    ROUND(SUM(CASE WHEN st.USAGE_TYPE = 'SBD Data Usage' THEN st.USAGE_BYTES ELSE 0 END) / 1000, 2) AS "Traffic Usage (KB)",
    -- События: для событий (USAGE_UNIT = 'EVENT') используем значение USAGE_BYTES,
    -- для остальных типов используем CALL_SESSION_COUNT или COUNT записей
    SUM(
        CASE 
            WHEN UPPER(TRIM(st.USAGE_UNIT)) = 'EVENT' THEN 
                COALESCE(st.USAGE_BYTES, st.ACTUAL_USAGE, 0)
            WHEN st.CALL_SESSION_COUNT IS NOT NULL THEN 
                st.CALL_SESSION_COUNT
            ELSE 1
        END
    ) AS "Events (Count)",
    SUM(
        CASE 
            WHEN st.USAGE_TYPE = 'SBD Data Usage' THEN
                CASE 
                    WHEN UPPER(TRIM(st.USAGE_UNIT)) = 'EVENT' THEN 
                        COALESCE(st.USAGE_BYTES, st.ACTUAL_USAGE, 0)
                    WHEN st.CALL_SESSION_COUNT IS NOT NULL THEN 
                        st.CALL_SESSION_COUNT
                    ELSE 1
                END
            ELSE 0
        END
    ) AS "Data Events",
    SUM(
        CASE 
            WHEN st.USAGE_TYPE = 'SBD Mailbox Checks' THEN
                CASE 
                    WHEN UPPER(TRIM(st.USAGE_UNIT)) = 'EVENT' THEN 
                        COALESCE(st.USAGE_BYTES, st.ACTUAL_USAGE, 0)
                    WHEN st.CALL_SESSION_COUNT IS NOT NULL THEN 
                        st.CALL_SESSION_COUNT
                    ELSE 1
                END
            ELSE 0
        END
    ) AS "Mailbox Events",
    SUM(
        CASE 
            WHEN st.USAGE_TYPE = 'SBD Registrations' THEN
                CASE 
                    WHEN UPPER(TRIM(st.USAGE_UNIT)) = 'EVENT' THEN 
                        COALESCE(st.USAGE_BYTES, st.ACTUAL_USAGE, 0)
                    WHEN st.CALL_SESSION_COUNT IS NOT NULL THEN 
                        st.CALL_SESSION_COUNT
                    ELSE 1
                END
            ELSE 0
        END
    ) AS "Registration Events",
    -- Fees из STECCOM_EXPENSES (агрегированные отдельно, без дубликатов)
    NVL(fees.fee_activation_fee, 0) AS "Activation Fee",
    NVL(fees.fee_advance_charge, 0) AS "Advance Charge",
    NVL(fees.fee_prorated, 0) AS "Prorated"
FROM SPNET_TRAFFIC st
LEFT JOIN (
    -- Агрегируем fees отдельно, исключая дубликаты
    SELECT 
        se.CONTRACT_ID,
        se.ICC_ID_IMEI AS imei,
        TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS bill_month,
        SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ACTIVATION%' OR UPPER(TRIM(se.DESCRIPTION)) = 'ACTIVATION FEE' THEN se.AMOUNT ELSE 0 END) AS fee_activation_fee,
        SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' OR UPPER(TRIM(se.DESCRIPTION)) = 'ADVANCE CHARGE' THEN se.AMOUNT ELSE 0 END) AS fee_advance_charge,
        SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%PRORATED%' OR UPPER(TRIM(se.DESCRIPTION)) = 'PRORATED' THEN se.AMOUNT ELSE 0 END) AS fee_prorated
    FROM (
        SELECT se.*,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
                    se.CONTRACT_ID,
                    se.ICC_ID_IMEI,
                    se.DESCRIPTION,
                    se.AMOUNT,
                    se.FEE_TYPE,
                    se.TRANSACTION_DATE
                ORDER BY se.ID
            ) AS rn
        FROM STECCOM_EXPENSES se
        WHERE se.CONTRACT_ID IS NOT NULL
          AND se.ICC_ID_IMEI IS NOT NULL
          AND se.INVOICE_DATE IS NOT NULL
          AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
    ) se
    WHERE se.rn = 1
    GROUP BY se.CONTRACT_ID, se.ICC_ID_IMEI, TO_CHAR(se.INVOICE_DATE, 'YYYYMM')
) fees
    ON st.CONTRACT_ID = fees.CONTRACT_ID
    AND st.IMEI = fees.imei
    AND (
        CASE 
            WHEN st.BILL_MONTH >= 200000 THEN
                TO_CHAR(st.BILL_MONTH)
            ELSE
                LPAD(TO_CHAR(MOD(st.BILL_MONTH, 10000)), 4, '0') || LPAD(TO_CHAR(TRUNC(st.BILL_MONTH / 10000)), 2, '0')
        END = fees.bill_month
    )
WHERE st.CONTRACT_ID = 'SUB-61922000117'
GROUP BY 
    CASE 
        WHEN st.BILL_MONTH >= 200000 THEN
            SUBSTR(TO_CHAR(st.BILL_MONTH), 1, 4) || '-' || SUBSTR(TO_CHAR(st.BILL_MONTH), 5, 2)
        ELSE
            LPAD(TO_CHAR(MOD(st.BILL_MONTH, 10000)), 4, '0') || '-' || LPAD(TO_CHAR(TRUNC(st.BILL_MONTH / 10000)), 2, '0')
    END,
    st.IMEI,
    st.CONTRACT_ID,
    fees.fee_activation_fee,
    fees.fee_advance_charge,
    fees.fee_prorated
ORDER BY "Bill Month" DESC;




