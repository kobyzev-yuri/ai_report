#!/bin/bash
# Скрипт для сбора статистики таблиц, используемых в финансовом анализе
# Ускоряет выполнение стандартных запросов финансового анализа

set -e

# Цвета для вывода
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "=========================================="
echo "Сбор статистики для финансового анализа"
echo "=========================================="
echo ""

# Загружаем конфигурацию Oracle
if [ -f config.env ]; then
    source config.env
else
    echo -e "${RED}❌ Файл config.env не найден${NC}"
    exit 1
fi

# Проверяем наличие переменных Oracle
if [ -z "$ORACLE_USER" ] || [ -z "$ORACLE_PASSWORD" ] || [ -z "$ORACLE_HOST" ]; then
    echo -e "${RED}❌ Не заданы переменные Oracle в config.env${NC}"
    exit 1
fi

# Формируем строку подключения
if [ -n "$ORACLE_SID" ]; then
    ORACLE_DSN="${ORACLE_HOST}:${ORACLE_PORT:-1521}/${ORACLE_SID}"
else
    ORACLE_DSN="${ORACLE_HOST}:${ORACLE_PORT:-1521}/${ORACLE_SERVICE}"
fi

echo -e "${YELLOW}Подключение к Oracle: ${ORACLE_USER}@${ORACLE_DSN}${NC}"
echo ""

# Список таблиц для сбора статистики (в порядке важности)
# Эти таблицы используются в VIEW для финансового анализа (V_PROFITABILITY_BY_PERIOD, V_PROFITABILITY_TREND, V_UNPROFITABLE_CUSTOMERS)
TABLES=(
    "BM_INVOICE_ITEM"      # Используется напрямую в V_PROFITABILITY_BY_PERIOD для курса валют
    "BM_PERIOD"            # Используется напрямую в V_PROFITABILITY_BY_PERIOD
    "STECCOM_EXPENSES"     # Используется через V_CONSOLIDATED_REPORT_WITH_BILLING
    "SPNET_TRAFFIC"        # Используется через V_CONSOLIDATED_REPORT_WITH_BILLING
    "BM_INVOICE"           # Используется через V_REVENUE_FROM_INVOICES
    "BM_CURRENCY_RATE"     # Может использоваться как запасной вариант для курса
    "SERVICES"             # Используется через V_REVENUE_FROM_INVOICES и V_CONSOLIDATED_REPORT_WITH_BILLING
    "BM_RESOURCE_TYPE"     # Используется через V_REVENUE_FROM_INVOICES
)

# Список представлений (для информации)
VIEWS=(
    "V_CONSOLIDATED_REPORT_WITH_BILLING"
    "V_REVENUE_FROM_INVOICES"
    "V_CONSOLIDATED_OVERAGE_REPORT"
)

SCHEMA="${ORACLE_USER}"

echo -e "${GREEN}Сбор статистики для таблиц:${NC}"
echo ""

# Функция для сбора статистики таблицы
gather_table_stats() {
    local table_name=$1
    echo -e "${YELLOW}📊 Сбор статистики для ${SCHEMA}.${table_name}...${NC}"
    
    sqlplus -S "${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_DSN}" <<EOF
SET PAGESIZE 0
SET FEEDBACK OFF
SET VERIFY OFF
SET HEADING OFF

BEGIN
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => '${SCHEMA}',
        tabname => '${table_name}',
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
        method_opt => 'FOR ALL COLUMNS SIZE AUTO',
        cascade => TRUE,
        degree => DBMS_STATS.AUTO_DEGREE,
        granularity => 'ALL',
        no_invalidate => FALSE
    );
    DBMS_OUTPUT.PUT_LINE('✅ Статистика для ${table_name} собрана');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('❌ Ошибка для ${table_name}: ' || SQLERRM);
END;
/
EXIT;
EOF

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ ${table_name} - статистика собрана${NC}"
    else
        echo -e "${RED}❌ ${table_name} - ошибка при сборе статистики${NC}"
    fi
    echo ""
}

# Собираем статистику для каждой таблицы
for table in "${TABLES[@]}"; do
    gather_table_stats "$table"
done

echo ""
echo -e "${GREEN}=========================================="
echo "Сбор статистики завершен"
echo "==========================================${NC}"
echo ""

# Проверяем статистику
echo -e "${YELLOW}Проверка собранной статистики:${NC}"
echo ""

sqlplus -S "${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_DSN}" <<EOF
SET PAGESIZE 1000
SET LINESIZE 200
SET FEEDBACK OFF

COLUMN table_name FORMAT A40
COLUMN num_rows FORMAT 999,999,999
COLUMN last_analyzed FORMAT A20

SELECT 
    table_name,
    num_rows,
    TO_CHAR(last_analyzed, 'YYYY-MM-DD HH24:MI:SS') AS last_analyzed
FROM user_tables
WHERE table_name IN ('STECCOM_EXPENSES', 'SPNET_TRAFFIC', 'BM_CURRENCY_RATE', 'BM_INVOICE', 'BM_INVOICE_ITEM', 'BM_PERIOD', 'SERVICES', 'BM_RESOURCE_TYPE')
ORDER BY table_name;

EXIT;
EOF

echo ""
echo -e "${GREEN}✅ Готово! Статистика собрана для всех таблиц${NC}"
echo ""
echo -e "${YELLOW}💡 Рекомендации:${NC}"
echo "  - Статистику следует собирать регулярно (например, после загрузки данных)"
echo "  - Для больших таблиц сбор статистики может занять время"
echo "  - Используйте кнопку '📈 Со статистикой' для анализа производительности запросов"
echo ""

