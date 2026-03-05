-- Отчёт по сервисам на точках доступа AP 523, 524 (Новосибирск)
-- Колонки: номер услуги, номер договора, название клиента, адрес клиента, адрес услуги, код 1С абонента, тип услуги
-- Исправленная версия: s.CUSTOMER_ID в GROUP BY, в подзапросе oi.ID = s.CUSTOMER_ID (без MAX — иначе ORA-00934)

SELECT
    s.SERVICE_ID AS "Номер услуги",
    accounts.DESCRIPTION AS "Номер договора",
    NVL(
        MAX(CASE WHEN cd.MNEMONIC = 'description' AND cc.CONTACT_DICT_ID = 23 THEN cc.VALUE END),
        TRIM(
            NVL(MAX(CASE WHEN cd.MNEMONIC = 'last_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' ||
            NVL(MAX(CASE WHEN cd.MNEMONIC = 'first_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' ||
            NVL(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '')
        )
    ) AS "Название клиента",
    NVL(
        NVL(
            MAX(CASE WHEN cd.MNEMONIC = 'b_paddress' AND cc.CONTACT_DICT_ID = 140 THEN cc.VALUE END),
            MAX(CASE WHEN cd.MNEMONIC = 'paddress' AND cc.CONTACT_DICT_ID = 10 THEN cc.VALUE END)
        ),
        MAX(CASE WHEN cd.MNEMONIC = 'address' AND cc.CONTACT_DICT_ID = 9 THEN cc.VALUE END)
    ) AS "Адрес клиента",
    s.DESCRIPTION AS "Адрес услуги",
    (SELECT oi.EXT_ID
     FROM OUTER_IDS oi
     WHERE oi.ID = s.CUSTOMER_ID
       AND UPPER(TRIM(oi.TBL)) = 'CUSTOMERS'
       AND ROWNUM = 1) AS "Код 1С абонента",
    s.TYPE_ID AS "Тип услуги"
FROM SERVICES s
JOIN ACCOUNTS accounts ON s.ACCOUNT_ID = accounts.ACCOUNT_ID
LEFT JOIN BM_CUSTOMER_CONTACT cc ON s.CUSTOMER_ID = cc.CUSTOMER_ID
LEFT JOIN BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID
WHERE s.STATUS > 0
  AND s.SERVICE_ID IN (
      SELECT SERVICE_ID
      FROM SERVICE_AP_LOCK
      WHERE AP_ID IN (523, 524)
  )
GROUP BY
    s.SERVICE_ID,
    accounts.DESCRIPTION,
    s.DESCRIPTION,
    s.TYPE_ID,
    s.CUSTOMER_ID
ORDER BY s.SERVICE_ID;
