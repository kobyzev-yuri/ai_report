-- ============================================================================
-- LOAD_LOGS - Журнал загрузок данных
-- Назначение: Отслеживание загрузок для тестирования
-- База данных: PostgreSQL (testing)
-- ============================================================================

-- Удаление если существует
DROP TABLE IF EXISTS LOAD_LOGS CASCADE;

CREATE TABLE LOAD_LOGS (
    ID SERIAL PRIMARY KEY,
    TABLE_NAME VARCHAR(50) NOT NULL,
    FILE_NAME VARCHAR(200) NOT NULL,
    RECORDS_LOADED INTEGER,
    LOAD_STATUS VARCHAR(20) CHECK (LOAD_STATUS IN ('SUCCESS', 'FAILED', 'PARTIAL')),
    ERROR_MESSAGE TEXT,
    LOAD_START_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    LOAD_END_TIME TIMESTAMP,
    LOADED_BY VARCHAR(50) DEFAULT CURRENT_USER
);

-- Индексы
CREATE INDEX idx_load_table ON LOAD_LOGS(TABLE_NAME);
CREATE INDEX idx_load_file ON LOAD_LOGS(FILE_NAME);
CREATE INDEX idx_load_date ON LOAD_LOGS(LOAD_START_TIME);
CREATE INDEX idx_load_status ON LOAD_LOGS(LOAD_STATUS);

-- Комментарии
COMMENT ON TABLE LOAD_LOGS IS 'Журнал загрузки данных из CSV файлов (тестовая)';
COMMENT ON COLUMN LOAD_LOGS.TABLE_NAME IS 'Имя таблицы в которую загружались данные';
COMMENT ON COLUMN LOAD_LOGS.FILE_NAME IS 'Имя загруженного файла';
COMMENT ON COLUMN LOAD_LOGS.RECORDS_LOADED IS 'Количество загруженных записей';
COMMENT ON COLUMN LOAD_LOGS.LOAD_STATUS IS 'Статус загрузки (SUCCESS/FAILED/PARTIAL)';

\echo 'Таблица LOAD_LOGS создана успешно!'


