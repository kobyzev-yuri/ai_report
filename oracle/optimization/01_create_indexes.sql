-- ============================================================================
-- Этап 1: Создание индексов для оптимизации производительности
-- Приоритет: Высокий (быстрый эффект, низкий риск)
-- Ожидаемый эффект: 30-50% ускорение JOIN операций
-- ============================================================================

SET SQLBLANKLINES ON
SET DEFINE OFF

PROMPT ============================================================================
PROMPT Создание составных индексов для STECCOM_EXPENSES
PROMPT ============================================================================

-- Индекс для частых JOIN по (CONTRACT_ID, IMEI, INVOICE_DATE)
-- Используется в steccom_fees CTE и JOIN с cor
PROMPT Создание IDX_STECCOM_COMPOSITE_JOIN...
CREATE INDEX IDX_STECCOM_COMPOSITE_JOIN ON STECCOM_EXPENSES(
    CONTRACT_ID, 
    ICC_ID_IMEI, 
    INVOICE_DATE
);

-- Функциональный индекс для BILL_MONTH (YYYYMM) - используется в JOIN
-- Это позволит использовать индекс для JOIN по bill_month
PROMPT Создание IDX_STECCOM_BILL_MONTH...
CREATE INDEX IDX_STECCOM_BILL_MONTH ON STECCOM_EXPENSES(
    TO_CHAR(INVOICE_DATE, 'YYYYMM'),
    CONTRACT_ID,
    ICC_ID_IMEI
);

-- Индекс для фильтрации по SERVICE (используется в WHERE)
-- Улучшит производительность фильтрации BROADBAND
PROMPT Создание IDX_STECCOM_SERVICE_FILTER...
CREATE INDEX IDX_STECCOM_SERVICE_FILTER ON STECCOM_EXPENSES(
    SERVICE,
    CONTRACT_ID,
    ICC_ID_IMEI
);

-- Индекс для DESCRIPTION (используется в CASE WHEN LIKE)
-- Улучшит производительность агрегации fees
PROMPT Создание IDX_STECCOM_DESCRIPTION...
CREATE INDEX IDX_STECCOM_DESCRIPTION ON STECCOM_EXPENSES(
    UPPER(TRIM(DESCRIPTION)),
    CONTRACT_ID,
    ICC_ID_IMEI
);

PROMPT ============================================================================
PROMPT Создание составных индексов для SPNET_TRAFFIC
PROMPT ============================================================================

-- Индекс для маппинга планов по CONTRACT_ID
-- Используется в contract_plan_mapping CTE
PROMPT Создание IDX_SPNET_PLAN_MAPPING...
CREATE INDEX IDX_SPNET_PLAN_MAPPING ON SPNET_TRAFFIC(
    CONTRACT_ID, 
    PLAN_NAME
);

-- Индекс для маппинга планов по IMEI
-- Используется в imei_plan_mapping CTE
PROMPT Создание IDX_SPNET_IMEI_PLAN_MAPPING...
CREATE INDEX IDX_SPNET_IMEI_PLAN_MAPPING ON SPNET_TRAFFIC(
    IMEI, 
    PLAN_NAME
);

-- Составной индекс для частых запросов по (IMEI, CONTRACT_ID, BILL_MONTH)
-- Используется в spnet_data CTE и JOIN
PROMPT Создание IDX_SPNET_COMPOSITE_JOIN...
CREATE INDEX IDX_SPNET_COMPOSITE_JOIN ON SPNET_TRAFFIC(
    IMEI,
    CONTRACT_ID,
    BILL_MONTH
);

PROMPT ============================================================================
PROMPT Индексы созданы успешно!
PROMPT ============================================================================
PROMPT 
PROMPT Следующие шаги:
PROMPT 1. Проверить использование индексов: SELECT * FROM USER_INDEXES WHERE INDEX_NAME LIKE 'IDX_%';
PROMPT 2. Собрать статистику: EXEC DBMS_STATS.GATHER_TABLE_STATS('BILLING7', 'STECCOM_EXPENSES');
PROMPT 3. Протестировать производительность запросов
PROMPT 4. Мониторить размер индексов и время обновления
PROMPT ============================================================================

SET DEFINE ON

