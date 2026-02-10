-- ============================================================================
-- Удаление дубликатов для контракта SUB-54278652475 и IMEI 300234069001680
-- ВНИМАНИЕ: Этот скрипт удаляет дубликаты, оставляя только первую запись (с минимальным ID)
-- ============================================================================

SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING ON
SET LINESIZE 2000

PROMPT ============================================================================
PROMPT ПРЕДУПРЕЖДЕНИЕ: Этот скрипт удалит дубликаты из таблицы STECCOM_EXPENSES
PROMPT ============================================================================
PROMPT 
PROMPT Перед выполнением убедитесь, что:
PROMPT 1. Вы сделали резервную копию данных
PROMPT 2. Вы проверили дубликаты с помощью check_duplicates_sub_54278652475.sql
PROMPT 3. Вы понимаете, какие записи будут удалены
PROMPT 
PROMPT Нажмите Ctrl+C для отмены или Enter для продолжения...
PAUSE

-- Сначала показываем, что будет удалено
PROMPT 
PROMPT ============================================================================
PROMPT Записи, которые будут УДАЛЕНЫ (дубликаты):
PROMPT ============================================================================

SELECT 
    se.ID,
    TO_CHAR(se.INVOICE_DATE, 'YYYY-MM-DD') AS invoice_date,
    se.CONTRACT_ID,
    se.ICC_ID_IMEI AS imei,
    se.DESCRIPTION,
    se.AMOUNT,
    TO_CHAR(se.TRANSACTION_DATE, 'YYYY-MM-DD') AS transaction_date,
    se.SOURCE_FILE,
    TO_CHAR(se.LOAD_DATE, 'YYYY-MM-DD HH24:MI:SS') AS load_date
FROM STECCOM_EXPENSES se
WHERE se.CONTRACT_ID = 'SUB-54278652475'
  AND se.ICC_ID_IMEI = '300234069001680'
  AND se.CONTRACT_ID IS NOT NULL
  AND se.ICC_ID_IMEI IS NOT NULL
  AND se.INVOICE_DATE IS NOT NULL
  AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
  AND se.ID NOT IN (
      -- Оставляем только первую запись (с минимальным ID) из каждой группы дубликатов
      SELECT MIN(se2.ID)
      FROM STECCOM_EXPENSES se2
      WHERE se2.CONTRACT_ID = 'SUB-54278652475'
        AND se2.ICC_ID_IMEI = '300234069001680'
        AND se2.CONTRACT_ID IS NOT NULL
        AND se2.ICC_ID_IMEI IS NOT NULL
        AND se2.INVOICE_DATE IS NOT NULL
        AND (se2.SERVICE IS NULL OR UPPER(TRIM(se2.SERVICE)) != 'BROADBAND')
        AND TO_CHAR(se2.INVOICE_DATE, 'YYYYMM') = TO_CHAR(se.INVOICE_DATE, 'YYYYMM')
        AND se2.CONTRACT_ID = se.CONTRACT_ID
        AND se2.ICC_ID_IMEI = se.ICC_ID_IMEI
        AND UPPER(TRIM(se2.DESCRIPTION)) = UPPER(TRIM(se.DESCRIPTION))
        AND se2.AMOUNT = se.AMOUNT
        AND (se2.TRANSACTION_DATE = se.TRANSACTION_DATE OR (se2.TRANSACTION_DATE IS NULL AND se.TRANSACTION_DATE IS NULL))
      GROUP BY 
          TO_CHAR(se2.INVOICE_DATE, 'YYYYMM'),
          se2.CONTRACT_ID,
          se2.ICC_ID_IMEI,
          UPPER(TRIM(se2.DESCRIPTION)),
          se2.AMOUNT,
          se2.TRANSACTION_DATE
  )
ORDER BY se.ID;

PROMPT 
PROMPT ============================================================================
PROMPT Подсчет записей для удаления...
PROMPT ============================================================================

SELECT 
    COUNT(*) AS records_to_delete,
    ROUND(SUM(AMOUNT), 2) AS total_amount_to_remove
FROM STECCOM_EXPENSES se
WHERE se.CONTRACT_ID = 'SUB-54278652475'
  AND se.ICC_ID_IMEI = '300234069001680'
  AND se.CONTRACT_ID IS NOT NULL
  AND se.ICC_ID_IMEI IS NOT NULL
  AND se.INVOICE_DATE IS NOT NULL
  AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
  AND se.ID NOT IN (
      SELECT MIN(se2.ID)
      FROM STECCOM_EXPENSES se2
      WHERE se2.CONTRACT_ID = 'SUB-54278652475'
        AND se2.ICC_ID_IMEI = '300234069001680'
        AND se2.CONTRACT_ID IS NOT NULL
        AND se2.ICC_ID_IMEI IS NOT NULL
        AND se2.INVOICE_DATE IS NOT NULL
        AND (se2.SERVICE IS NULL OR UPPER(TRIM(se2.SERVICE)) != 'BROADBAND')
        AND TO_CHAR(se2.INVOICE_DATE, 'YYYYMM') = TO_CHAR(se.INVOICE_DATE, 'YYYYMM')
        AND se2.CONTRACT_ID = se.CONTRACT_ID
        AND se2.ICC_ID_IMEI = se.ICC_ID_IMEI
        AND UPPER(TRIM(se2.DESCRIPTION)) = UPPER(TRIM(se.DESCRIPTION))
        AND se2.AMOUNT = se.AMOUNT
        AND (se2.TRANSACTION_DATE = se.TRANSACTION_DATE OR (se2.TRANSACTION_DATE IS NULL AND se.TRANSACTION_DATE IS NULL))
      GROUP BY 
          TO_CHAR(se2.INVOICE_DATE, 'YYYYMM'),
          se2.CONTRACT_ID,
          se2.ICC_ID_IMEI,
          UPPER(TRIM(se2.DESCRIPTION)),
          se2.AMOUNT,
          se2.TRANSACTION_DATE
  );

PROMPT 
PROMPT ============================================================================
PROMPT Для выполнения удаления раскомментируйте следующий блок:
PROMPT ============================================================================
PROMPT 
PROMPT -- BEGIN
PROMPT --     DELETE FROM STECCOM_EXPENSES
PROMPT --     WHERE CONTRACT_ID = 'SUB-54278652475'
PROMPT --       AND ICC_ID_IMEI = '300234069001680'
PROMPT --       AND CONTRACT_ID IS NOT NULL
PROMPT --       AND ICC_ID_IMEI IS NOT NULL
PROMPT --       AND INVOICE_DATE IS NOT NULL
PROMPT --       AND (SERVICE IS NULL OR UPPER(TRIM(SERVICE)) != 'BROADBAND')
PROMPT --       AND ID NOT IN (
PROMPT --           SELECT MIN(se2.ID)
PROMPT --           FROM STECCOM_EXPENSES se2
PROMPT --           WHERE se2.CONTRACT_ID = 'SUB-54278652475'
PROMPT --             AND se2.ICC_ID_IMEI = '300234069001680'
PROMPT --             AND se2.CONTRACT_ID IS NOT NULL
PROMPT --             AND se2.ICC_ID_IMEI IS NOT NULL
PROMPT --             AND se2.INVOICE_DATE IS NOT NULL
PROMPT --             AND (se2.SERVICE IS NULL OR UPPER(TRIM(se2.SERVICE)) != 'BROADBAND')
PROMPT --             AND TO_CHAR(se2.INVOICE_DATE, 'YYYYMM') = TO_CHAR(STECCOM_EXPENSES.INVOICE_DATE, 'YYYYMM')
PROMPT --             AND se2.CONTRACT_ID = STECCOM_EXPENSES.CONTRACT_ID
PROMPT --             AND se2.ICC_ID_IMEI = STECCOM_EXPENSES.ICC_ID_IMEI
PROMPT --             AND UPPER(TRIM(se2.DESCRIPTION)) = UPPER(TRIM(STECCOM_EXPENSES.DESCRIPTION))
PROMPT --             AND se2.AMOUNT = STECCOM_EXPENSES.AMOUNT
PROMPT --             AND (se2.TRANSACTION_DATE = STECCOM_EXPENSES.TRANSACTION_DATE OR (se2.TRANSACTION_DATE IS NULL AND STECCOM_EXPENSES.TRANSACTION_DATE IS NULL))
PROMPT --           GROUP BY 
PROMPT --               TO_CHAR(se2.INVOICE_DATE, 'YYYYMM'),
PROMPT --               se2.CONTRACT_ID,
PROMPT --               se2.ICC_ID_IMEI,
PROMPT --               UPPER(TRIM(se2.DESCRIPTION)),
PROMPT --               se2.AMOUNT,
PROMPT --               se2.TRANSACTION_DATE
PROMPT --       );
PROMPT --     COMMIT;
PROMPT --     DBMS_OUTPUT.PUT_LINE('Дубликаты успешно удалены');
PROMPT -- END;
PROMPT -- /

EXIT;





















