#!/bin/bash
# ============================================================================
# Import V_IRIDIUM_SERVICES_INFO.txt from Oracle dump to PostgreSQL
# Last successful version using import_iridium.py
# ============================================================================

set -e

# Конфигурация
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUMP_FILE="${SCRIPT_DIR}/V_IRIDIUM_SERVICES_INFO.txt"
IMPORT_SCRIPT="${SCRIPT_DIR}/import_iridium.py"

# Конфигурация PostgreSQL
# ВАЖНО: Установите PGPASSWORD в переменных окружения!
PG_HOST="${PGHOST:-localhost}"
PG_PORT="${PGPORT:-5432}"
PG_DB="${PGDATABASE:-billing}"
PG_USER="${PGUSER:-postgres}"
PG_PASSWORD="${PGPASSWORD:-your-password-here}"

# Целевая таблица
TARGET_TABLE="${TARGET_TABLE:-iridium_services_info}"

echo "============================================================================"
echo "Import Oracle dump to PostgreSQL"
echo "============================================================================"
echo ""
echo "Source file: ${DUMP_FILE}"
echo "Target DB: ${PG_USER}@${PG_HOST}:${PG_PORT}/${PG_DB}"
echo "Target table: ${TARGET_TABLE}"
echo ""

# Проверка наличия файла дампа
if [ ! -f "${DUMP_FILE}" ]; then
    echo "ERROR: Dump file not found: ${DUMP_FILE}"
    echo ""
    echo "Please export data first using:"
    echo "  sqlplus -s billing7/billing@bm7 @export_v_iridium_services_info.sql"
    exit 1
fi

# Проверка количества строк
RECORD_COUNT=$(wc -l < "${DUMP_FILE}")
echo "Records in dump file: ${RECORD_COUNT}"
echo ""

# Проверка наличия скрипта импорта
if [ ! -f "${IMPORT_SCRIPT}" ]; then
    echo "ERROR: Import script not found: ${IMPORT_SCRIPT}"
    exit 1
fi

# Формируем DSN для PostgreSQL
PG_DSN="host=${PG_HOST} dbname=${PG_DB} user=${PG_USER} password=${PG_PASSWORD}"

# Проверка пароля
if [ "${PG_PASSWORD}" = "your-password-here" ]; then
    echo "ERROR: Please set PGPASSWORD environment variable!"
    echo "   export PGPASSWORD=your-actual-password"
    exit 1
fi

# Запускаем импорт
echo "Starting import..."
echo ""

# Используем PGPASSWORD для аутентификации
export PGPASSWORD="${PG_PASSWORD}"

python3 "${IMPORT_SCRIPT}" \
    --input "${DUMP_FILE}" \
    --dsn "${PG_DSN}" \
    --table "${TARGET_TABLE}" \
    --truncate \
    --batch 1000

IMPORT_EXIT_CODE=$?

if [ ${IMPORT_EXIT_CODE} -eq 0 ]; then
    echo ""
    echo "============================================================================"
    echo "Import completed successfully!"
    echo "============================================================================"
    
    # Проверка загруженных данных
    echo ""
    echo "Verifying import..."
    psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}" -c "
        SELECT 
            COUNT(*) AS total_records,
            COUNT(DISTINCT service_id) AS unique_services,
            COUNT(*) FILTER (WHERE contract_id IS NOT NULL) AS with_contract_id,
            COUNT(*) FILTER (WHERE imei IS NOT NULL) AS with_imei
        FROM ${TARGET_TABLE};
    "
    
    echo ""
    echo "Sample records:"
    psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}" -c "
        SELECT 
            service_id, 
            contract_id, 
            LEFT(imei, 20) AS imei, 
            tariff_id, 
            status, 
            account_id 
        FROM ${TARGET_TABLE} 
        LIMIT 5;
    "
else
    echo ""
    echo "============================================================================"
    echo "ERROR: Import failed with exit code ${IMPORT_EXIT_CODE}"
    echo "============================================================================"
    exit ${IMPORT_EXIT_CODE}
fi

