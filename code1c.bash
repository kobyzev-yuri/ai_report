# Применить исправленный view
sqlplus -s $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @oracle/views/03_v_iridium_services_info.sql

# Проверить результат
sqlplus -s $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @oracle/test/test_code_1c_export.sql

# Переэкспортировать данные
sqlplus -s $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @oracle/test/export_v_iridium_services_info.sql
