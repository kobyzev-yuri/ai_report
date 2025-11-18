-- Проверка пропавшей колонки Total Amount из SPNet
-- Эта колонка должна показывать стоимость трафика из SPNET_TRAFFIC.TOTAL_AMOUNT

-- 1. Проверка наличия SPNET_TOTAL_AMOUNT в V_SPNET_OVERAGE_ANALYSIS
SELECT 
    'V_SPNET_OVERAGE_ANALYSIS' AS source,
    ov.IMEI,
    ov.CONTRACT_ID,
    ov.BILL_MONTH,
    ov.SPNET_TOTAL_AMOUNT,
    ov.CALCULATED_OVERAGE_CHARGE
FROM V_SPNET_OVERAGE_ANALYSIS ov
WHERE ROWNUM <= 10
ORDER BY ov.BILL_MONTH DESC;

-- 2. Проверка наличия SPNET_TOTAL_AMOUNT в V_CONSOLIDATED_OVERAGE_REPORT
-- Должна быть колонка, но может отсутствовать
SELECT 
    'V_CONSOLIDATED_OVERAGE_REPORT columns' AS info
FROM USER_TAB_COLUMNS
WHERE TABLE_NAME = 'V_CONSOLIDATED_OVERAGE_REPORT'
  AND UPPER(COLUMN_NAME) LIKE '%TOTAL%'
ORDER BY COLUMN_NAME;

-- 3. Проверка данных в SPNET_TRAFFIC (исходная таблица)
SELECT 
    'SPNET_TRAFFIC' AS source,
    st.IMEI,
    st.CONTRACT_ID,
    st.BILL_MONTH,
    SUM(st.TOTAL_AMOUNT) AS total_amount_sum,
    COUNT(*) AS record_count
FROM SPNET_TRAFFIC st
WHERE ROWNUM <= 100
GROUP BY st.IMEI, st.CONTRACT_ID, st.BILL_MONTH
ORDER BY st.BILL_MONTH DESC;

-- 4. Проверка, передается ли SPNET_TOTAL_AMOUNT из spnet_data в V_CONSOLIDATED_OVERAGE_REPORT
-- Нужно проверить структуру представления

