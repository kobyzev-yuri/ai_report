#!/bin/bash
# Скрипт для применения представления на сервере
# Использование: ssh -p 1194 root@82.114.2.2 "bash -s" < apply_view.sh

source /usr/local/projects/ai_report/config.env 2>/dev/null
cd /usr/local/projects/ai_report/oracle/views

echo "Применение представления V_CONSOLIDATED_REPORT_WITH_BILLING..."
timeout 60 sqlplus -S ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_HOST}:${ORACLE_PORT}/${ORACLE_SERVICE:-bm7} @04_v_consolidated_report_with_billing.sql 2>&1 | tail -10

echo ""
echo "✅ Представление применено (или проверьте ошибки выше)"





