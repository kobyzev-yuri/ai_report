-- ============================================================================
-- УДАЛЕНИЕ ДУБЛИКАТОВ - ВЫПОЛНЕНИЕ
-- Контракт: SUB-54278652475, IMEI: 300234069001680
-- ВНИМАНИЕ: Этот скрипт реально удаляет данные!
-- ============================================================================

SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING ON
SET LINESIZE 2000

-- Используем CTE для более безопасного удаления
WITH duplicates_to_delete AS (
    SELECT se.ID
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
)
SELECT COUNT(*) AS records_to_delete FROM duplicates_to_delete;

-- Раскомментируйте следующий блок для выполнения удаления:
/*
DELETE FROM STECCOM_EXPENSES
WHERE ID IN (
    SELECT ID FROM duplicates_to_delete
);

COMMIT;
*/

EXIT;























