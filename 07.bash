  ssh -p 1194 root@82.114.2.2 "source /usr/local/projects/ai_report/config.env 2>/dev/null; cd /usr/local/projects/ai_report/oracle/views && timeout 60 sqlplus -S \${ORACLE_USER}/\${ORACLE_PASSWORD}@\${ORACLE_HOST}:\${ORACLE_PORT}/\${ORACLE_SERVICE:-bm7} @oracle/views/07_v_analytics_invoice_period.sql 2>&1 | tail -10"

