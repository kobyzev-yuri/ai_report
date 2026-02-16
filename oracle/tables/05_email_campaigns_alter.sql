-- ============================================================================
-- ALTER TABLE EMAIL_CAMPAIGNS - Добавление полей для тестовой рассылки
-- Назначение: Добавить поддержку тестовой рассылки в существующую таблицу
-- База данных: Oracle (production)
-- ============================================================================

-- Добавляем поле TEST_MODE (флаг тестовой рассылки)
ALTER TABLE EMAIL_CAMPAIGNS ADD (
    TEST_MODE NUMBER DEFAULT 0,
    TEST_EMAILS CLOB
);

-- Обновляем CHECK constraint для STATUS, добавляя TEST_SENT
ALTER TABLE EMAIL_CAMPAIGNS DROP CONSTRAINT SYS_C0012345;  -- Замените на реальное имя constraint если нужно
-- Или проще: удалить и создать заново
ALTER TABLE EMAIL_CAMPAIGNS DROP CONSTRAINT IF EXISTS EMAIL_CAMPAIGNS_STATUS_CK;
ALTER TABLE EMAIL_CAMPAIGNS ADD CONSTRAINT EMAIL_CAMPAIGNS_STATUS_CK 
    CHECK (STATUS IN ('DRAFT', 'SENT', 'FAILED', 'PARTIAL', 'TEST_SENT'));

-- Комментарии для новых полей
COMMENT ON COLUMN EMAIL_CAMPAIGNS.TEST_MODE IS 'Флаг тестовой рассылки: 0 - обычная рассылка, 1 - тестовая рассылка по контрольным email';
COMMENT ON COLUMN EMAIL_CAMPAIGNS.TEST_EMAILS IS 'Список контрольных email для тестовой рассылки (разделитель - запятая)';

-- Обновляем комментарий для STATUS
COMMENT ON COLUMN EMAIL_CAMPAIGNS.STATUS IS 'Статус кампании: DRAFT - черновик, SENT - отправлено, FAILED - ошибка, PARTIAL - частично отправлено, TEST_SENT - тестовая рассылка отправлена';

PROMPT Таблица EMAIL_CAMPAIGNS успешно обновлена для поддержки тестовой рассылки!

