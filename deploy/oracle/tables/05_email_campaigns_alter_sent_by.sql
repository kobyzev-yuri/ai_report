-- ============================================================================
-- ALTER TABLE EMAIL_CAMPAIGNS - Добавление поля SENT_BY для логирования отправителя
-- Назначение: Добавить поле для записи кто отправил кампанию
-- База данных: Oracle (production)
-- ============================================================================

-- Добавляем поле SENT_BY (пользователь отправивший кампанию)
ALTER TABLE EMAIL_CAMPAIGNS ADD (
    SENT_BY VARCHAR2(50)
);

-- Комментарий для нового поля
COMMENT ON COLUMN EMAIL_CAMPAIGNS.SENT_BY IS 'Пользователь отправивший кампанию (кто нажал кнопку отправки)';

-- Индекс для быстрого поиска по отправителю
CREATE INDEX IDX_CAMPAIGN_SENT_BY ON EMAIL_CAMPAIGNS(SENT_BY);
CREATE INDEX IDX_CAMPAIGN_SENT_AT ON EMAIL_CAMPAIGNS(SENT_AT);

PROMPT Таблица EMAIL_CAMPAIGNS успешно обновлена для логирования отправителя!

