#!/bin/bash
# ============================================================================
# Импорт V_IRIDIUM_SERVICES_INFO.csv в PostgreSQL
# Использование: ./import_iridium_csv.sh [путь_к_файлу.csv]
# ============================================================================

set -e

# Параметры подключения к PostgreSQL (можно задать через переменные окружения)
PG_HOST="${PGHOST:-localhost}"
PG_PORT="${PGPORT:-5432}"
PG_DB="${PGDATABASE:-billing}"
PG_USER="${PGUSER:-postgres}"
PG_PASSWORD="${PGPASSWORD:-}"

# Путь к файлу
if [ -n "$1" ]; then
    CSV_FILE="$1"
else
    # По умолчанию ищем в oracle/test
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    CSV_FILE="${SCRIPT_DIR}/../../oracle/test/V_IRIDIUM_SERVICES_INFO.csv"
fi

# Проверка наличия файла
if [ ! -f "${CSV_FILE}" ]; then
    echo "ERROR: Файл не найден: ${CSV_FILE}"
    echo ""
    echo "Использование: $0 [путь_к_файлу.csv]"
    echo "  или поместите файл в: oracle/test/V_IRIDIUM_SERVICES_INFO.csv"
    exit 1
fi

echo "============================================================================"
echo "Импорт V_IRIDIUM_SERVICES_INFO.csv в PostgreSQL"
echo "============================================================================"
echo ""
echo "Файл: ${CSV_FILE}"
echo "База данных: ${PG_USER}@${PG_HOST}:${PG_PORT}/${PG_DB}"
echo ""

# Проверка количества строк
RECORD_COUNT=$(wc -l < "${CSV_FILE}")
echo "Строк в файле: ${RECORD_COUNT}"
echo ""

# Проверка формата (должно быть 18 колонок, разделитель |)
FIRST_LINE=$(head -n 1 "${CSV_FILE}")
COL_COUNT=$(echo "${FIRST_LINE}" | tr '|' '\n' | wc -l)
echo "Колонок в первой строке: ${COL_COUNT} (ожидается 18)"
if [ "${COL_COUNT}" -ne 18 ]; then
    echo "WARNING: Неожиданное количество колонок. Продолжаем импорт..."
fi
echo ""

# Экспортируем пароль для psql
if [ -n "${PG_PASSWORD}" ]; then
    export PGPASSWORD="${PG_PASSWORD}"
fi

# Импорт данных
echo "Начинаем импорт..."
echo ""

# Сначала очищаем таблицу (опционально, можно закомментировать)
# psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}" -c "TRUNCATE TABLE iridium_services_info;"

# Импорт с использованием COPY
psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}" << EOF
-- Импорт данных (разделитель |, без заголовка)
\copy iridium_services_info (
    service_id,
    contract_id,
    imei,
    tariff_id,
    agreement_number,
    order_number,
    status,
    actual_status,
    customer_id,
    organization_name,
    person_name,
    customer_name,
    create_date,
    start_date,
    stop_date,
    account_id,
    is_suspended,
    code_1c
) FROM '${CSV_FILE}' WITH (FORMAT csv, DELIMITER '|', NULL '');

-- Проверка загруженных данных
\echo ''
\echo '============================================================================'
\echo 'Проверка импорта:'
\echo '============================================================================'

SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT service_id) AS unique_services,
    COUNT(*) FILTER (WHERE contract_id IS NOT NULL) AS with_contract_id,
    COUNT(*) FILTER (WHERE imei IS NOT NULL) AS with_imei,
    COUNT(*) FILTER (WHERE code_1c IS NOT NULL) AS with_code_1c,
    COUNT(*) FILTER (WHERE status = 10) AS active_status,
    COUNT(*) FILTER (WHERE is_suspended = 'Y') AS suspended
FROM iridium_services_info;

\echo ''
\echo 'Примеры записей:'
SELECT 
    service_id, 
    contract_id, 
    LEFT(imei, 20) AS imei, 
    tariff_id, 
    status, 
    is_suspended,
    code_1c
FROM iridium_services_info 
LIMIT 5;

EOF

IMPORT_EXIT_CODE=$?

if [ ${IMPORT_EXIT_CODE} -eq 0 ]; then
    echo ""
    echo "============================================================================"
    echo "✓ Импорт завершен успешно!"
    echo "============================================================================"
else
    echo ""
    echo "============================================================================"
    echo "ERROR: Импорт завершился с ошибкой (код: ${IMPORT_EXIT_CODE})"
    echo "============================================================================"
    exit ${IMPORT_EXIT_CODE}
fi

