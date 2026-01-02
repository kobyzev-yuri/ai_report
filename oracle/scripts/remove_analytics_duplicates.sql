-- ============================================================================
-- Удаление дубликатов из ANALYTICS
-- Оставляет только одну запись с максимальным AID для каждой группы дубликатов
-- ============================================================================
-- 
-- ВАЖНО: Дубликаты определяются как записи, где ВСЕ поля совпадают, кроме AID
-- 
-- Использование:
-- 1. Сначала проверьте дубликаты через функцию get_analytics_duplicates
-- 2. Выполните этот скрипт для удаления дубликатов
-- 3. Удаляются все записи кроме той, у которой максимальный AID в группе
--
-- ============================================================================

SET SQLBLANKLINES ON
SET DEFINE OFF

-- Удаление дубликатов: оставляем только запись с максимальным AID
-- Используем ROW_NUMBER для более надежного определения дубликатов
DELETE FROM ANALYTICS
WHERE AID IN (
    SELECT AID
    FROM (
        SELECT 
            AID,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    PERIOD_ID,
                    NVL(SERVICE_ID, -999999),
                    NVL(CUSTOMER_ID, -999999),
                    NVL(ACCOUNT_ID, -999999),
                    NVL(TYPE_ID, -999999),
                    NVL(TARIFF_ID, -999999),
                    NVL(TARIFFEL_ID, -999999),
                    NVL(VSAT, 'NULL'),
                    NVL(MONEY, -999999),
                    NVL(PRICE, -999999),
                    NVL(TRAF, -999999),
                    NVL(TOTAL_TRAF, -999999),
                    NVL(CBYTE, -999999),
                    NVL(INVOICE_ITEM_ID, -999999),
                    NVL(FLAG, -999999),
                    NVL(RESOURCE_TYPE_ID, -999999),
                    NVL(CLASS_ID, -999999),
                    NVL(CLASS_NAME, 'NULL'),
                    NVL(BLANK, 'NULL'),
                    NVL(COUNTER_ID, -999999),
                    NVL(COUNTER_CF, -999999),
                    NVL(ZONE_ID, -999999),
                    NVL(THRESHOLD, -999999),
                    NVL(SUB_TYPE_ID, -999999),
                    NVL(SUB_PERIOD_ID, -999999),
                    NVL(PMONEY, -999999),
                    NVL(PARTNER_PERCENT, -999999)
                ORDER BY AID DESC
            ) AS rn
        FROM ANALYTICS
    )
    WHERE rn > 1  -- Удаляем все кроме первой записи (с максимальным AID)
)
/


SET DEFINE ON



