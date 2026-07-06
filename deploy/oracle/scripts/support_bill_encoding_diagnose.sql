-- Диагностика кодировки SUPPORT.BILL (bm7). Запуск: sqlplus support/...@bm7 @oracle/scripts/support_bill_encoding_diagnose.sql
-- Перед sqlplus на Linux: export NLS_LANG=RUSSIAN_CIS.CL8MSWIN1251

SET LINESIZE 200
SET PAGESIZE 100
COL customer FORMAT A40
COL letter FORMAT A30
COL pay_type FORMAT A20
COL hx FORMAT A60

PROMPT === NLS database ===
SELECT parameter, value
  FROM nls_database_parameters
 WHERE parameter IN ('NLS_CHARACTERSET', 'NLS_NCHAR_CHARACTERSET');

PROMPT === Пример HEX (если D2E5F0... — в БД CP1251 «Территория», править клиент, не данные) ===
SELECT b_id,
       customer,
       RAWTOHEX(CAST(customer AS VARCHAR2(4000))) AS customer_hex
  FROM support.bill
 WHERE customer IS NOT NULL
   AND ROWNUM <= 5;

PROMPT === Авансовые счета (bill_id IS NULL): UTF-8 mojibake РџР… (тип 2) ===
SELECT b_id, bill_id, customer, service, letter,
       RAWTOHEX(CAST(customer AS VARCHAR2(4000))) AS customer_hex
  FROM support.bill
 WHERE bill_id IS NULL
   AND (REGEXP_LIKE(NVL(customer,'x'), '(Р.|С.){2,}')
     OR REGEXP_LIKE(NVL(service,'x'), '(Р.|С.){2,}')
     OR REGEXP_LIKE(NVL(letter,'x'), '(Р.|С.){2,}')
     OR REGEXP_LIKE(NVL(pay_type,'x'), '(Р.|С.){2,}'))
   AND ROWNUM <= 20;

PROMPT === Строки с типичными «кракозябрами» в выводе (тип 1: ҐЋ®) ===
SELECT b_id, customer, letter, pay_type
  FROM support.bill
 WHERE (REGEXP_LIKE(NVL(customer,'x'), '[ҐЋ®€¤¬]')
    OR REGEXP_LIKE(NVL(letter,'x'), '[ҐЋ®€¤¬]')
    OR REGEXP_LIKE(NVL(pay_type,'x'), '[ҐЋ®€¤¬]')
    OR REGEXP_LIKE(NVL(service,'x'), '[ҐЋ®€¤¬]')
    OR REGEXP_LIKE(NVL(description,'x'), '[ҐЋ®€¤¬]'))
   AND ROWNUM <= 20;

PROMPT === Пример b_id=177455 (аванс) ===
SELECT b_id, bill_id, customer, service, letter, pay_type,
       RAWTOHEX(CAST(customer AS VARCHAR2(4000))) AS customer_hex,
       RAWTOHEX(CAST(letter AS VARCHAR2(4000))) AS letter_hex
  FROM support.bill
 WHERE b_id = 177455;

PROMPT === Только просмотр в UTF-8 (если БД CL8MSWIN1251, данные не трогаем) ===
SELECT b_id,
       CONVERT(customer, 'UTF8', 'CL8MSWIN1251') AS customer_utf8,
       CONVERT(letter, 'UTF8', 'CL8MSWIN1251') AS letter_utf8,
       CONVERT(pay_type, 'UTF8', 'CL8MSWIN1251') AS pay_type_utf8
  FROM support.bill
 WHERE b_id IN (
       SELECT b_id FROM support.bill
        WHERE REGEXP_LIKE(NVL(customer,'x'), '[ҐЋ®€¤¬«ЈІ]')
          AND ROWNUM <= 5
 );
