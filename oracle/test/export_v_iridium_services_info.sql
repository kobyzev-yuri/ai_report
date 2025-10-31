-- ============================================================================
-- Export V_IRIDIUM_SERVICES_INFO to TSV (Tab-Separated Values)
-- Last successful version with proper tab delimiters
-- Usage: sqlplus -s $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @export_v_iridium_services_info.sql
--   or: sqlplus -s username/password@service_name @export_v_iridium_services_info.sql
-- Output: V_IRIDIUM_SERVICES_INFO.txt (17 columns, tab-separated)
-- ============================================================================

SET TERMOUT OFF
SET ECHO OFF
SET FEEDBACK OFF
SET VERIFY OFF
SET HEADING OFF
SET PAGESIZE 0
SET LINESIZE 32767
SET WRAP OFF
SET TRIMSPOOL ON

SPOOL V_IRIDIUM_SERVICES_INFO.txt

-- Экспорт с явной конкатенацией через CHR(9) для гарантированных табов
-- Это гарантирует правильные табы независимо от версии SQL*Plus
-- IMEI берется из PASSWD (для VSAT нужно использовать s.VSAT в view)
SELECT
  SERVICE_ID || CHR(9) ||
  '"' || REPLACE(CONTRACT_ID, '"', '""') || '"' || CHR(9) ||
  CASE WHEN IMEI IS NULL THEN '' ELSE '"' || REPLACE(IMEI, '"', '""') || '"' END || CHR(9) ||
  TARIFF_ID || CHR(9) ||
  '"' || REPLACE(NVL(AGREEMENT_NUMBER,''), '"', '""') || '"' || CHR(9) ||
  '"' || REPLACE(NVL(ORDER_NUMBER,''), '"', '""') || '"' || CHR(9) ||
  STATUS || CHR(9) ||
  ACTUAL_STATUS || CHR(9) ||
  CUSTOMER_ID || CHR(9) ||
  '"' || REPLACE(NVL(ORGANIZATION_NAME,''), '"', '""') || '"' || CHR(9) ||
  '"' || REPLACE(NVL(PERSON_NAME,''), '"', '""') || '"' || CHR(9) ||
  '"' || REPLACE(NVL(CUSTOMER_NAME,''), '"', '""') || '"' || CHR(9) ||
  TO_CHAR(CREATE_DATE,'YYYY-MM-DD HH24:MI:SS') || CHR(9) ||
  TO_CHAR(START_DATE,'YYYY-MM-DD HH24:MI:SS') || CHR(9) ||
  TO_CHAR(STOP_DATE,'YYYY-MM-DD HH24:MI:SS') || CHR(9) ||
  ACCOUNT_ID || CHR(9) ||
  CASE WHEN CODE_1C IS NULL THEN '' ELSE '"' || REPLACE(CODE_1C, '"', '""') || '"' END
FROM V_IRIDIUM_SERVICES_INFO;

SPOOL OFF
EXIT

-- ============================================================================
-- Usage:
--   sqlplus -s billing7/billing@bm7 @export_v_iridium_services_info.sql
--   или из директории oracle/test:
--   sqlplus -s billing7/billing@bm7 @oracle/test/export_v_iridium_services_info.sql
--
-- Notes:
-- 1. Используется CHR(9) для гарантированных табов (работает во всех версиях SQL*Plus)
-- 2. Если нужно использовать VSAT вместо PASSWD для IMEI, измените view:
--    oracle/views/03_v_iridium_services_info.sql: s.PASSWD -> s.VSAT
-- 3. Файл будет создан в текущей директории (где запущен sqlplus)
-- 4. Проверка формата:
--    awk -F'\t' '{print NF}' V_IRIDIUM_SERVICES_INFO.txt | head -10 | sort -u
--    Должно выводить только "17" для всех строк
-- 5. Импорт в PostgreSQL:
--    python3 oracle/test/import_iridium.py --input oracle/test/V_IRIDIUM_SERVICES_INFO.txt \
--      --dsn "host=localhost dbname=billing user=postgres password=1234" \
--      --table iridium_services_info --truncate
-- ============================================================================

