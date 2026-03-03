-- ============================================================================
-- ALTER EMAIL_CAMPAIGNS - Увеличение размера поля GREETING
-- Проблема: GREETING VARCHAR2(1000) недостаточно для длинных текстов писем
-- Решение: Изменить на CLOB для поддержки больших текстов
-- ============================================================================

-- Oracle не позволяет напрямую изменить VARCHAR2 на CLOB, поэтому:
-- 1. Создаем временную колонку CLOB
ALTER TABLE EMAIL_CAMPAIGNS ADD GREETING_NEW CLOB;

-- 2. Копируем данные из старой колонки в новую
UPDATE EMAIL_CAMPAIGNS SET GREETING_NEW = GREETING WHERE GREETING IS NOT NULL;

-- 3. Удаляем старую колонку
ALTER TABLE EMAIL_CAMPAIGNS DROP COLUMN GREETING;

-- 4. Переименовываем новую колонку
ALTER TABLE EMAIL_CAMPAIGNS RENAME COLUMN GREETING_NEW TO GREETING;

PROMPT Поле GREETING успешно изменено на CLOB!
