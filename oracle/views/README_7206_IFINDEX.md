# VIEW для замены индексов интерфейсов 7206

## Описание

VIEW `V_7206_IFINDEX_REPLACEMENT` предназначен для операторов и показывает сервисы, которые нужно обновить при смене индексов интерфейсов с working на spare.

## Структура VIEW

VIEW содержит следующие колонки:

- **SERVICES_EXT_ID** - ID записи в services_ext
- **SERVICE_ID** - ID услуги
- **CUSTOMER_ID** - ID клиента
- **ACCOUNT_ID** - ID аккаунта
- **CUSTOMER_NAME** - Название клиента (организация или ФИО)
- **OLD_VALUE** - Старый параметр подключения (текущее значение в services_ext.VALUE)
- **NEW_VALUE** - Новый параметр подключения (с замененными MAC адресами)
- **OLD_INDEX** - Старый индекс интерфейса (working)
- **NEW_INDEX** - Новый индекс интерфейса (spare)
- **OLD_MAC** - Старый MAC адрес (соответствует старому индексу)
- **NEW_MAC** - Новый MAC адрес (соответствует новому индексу)
- **INTERFACE_NAME** - Название интерфейса
- **MAC_REPLACEMENTS_COUNT** - Количество замененных MAC адресов в строке
- **DATE_BEG** - Дата начала действия параметра
- **DICT_ID** - ID словаря (14009)

## Условия фильтрации

VIEW показывает только записи, которые:
- `date_end IS NULL` (активные записи)
- `dict_id = 14009`
- `type_id = 10`
- Содержат MAC адреса, которые нужно заменить

## Пример использования

```sql
-- Просмотр всех сервисов, которые нужно обновить
SELECT 
    SERVICE_ID,
    CUSTOMER_NAME,
    CUSTOMER_ID,
    ACCOUNT_ID,
    OLD_VALUE,
    NEW_VALUE,
    OLD_INDEX,
    NEW_INDEX,
    OLD_MAC,
    NEW_MAC
FROM V_7206_IFINDEX_REPLACEMENT
ORDER BY CUSTOMER_NAME, SERVICE_ID;

-- Просмотр для конкретного клиента
SELECT 
    SERVICE_ID,
    CUSTOMER_NAME,
    OLD_VALUE,
    NEW_VALUE
FROM V_7206_IFINDEX_REPLACEMENT
WHERE CUSTOMER_ID = 12345;

-- Подсчет количества сервисов для обновления по клиентам
SELECT 
    CUSTOMER_ID,
    CUSTOMER_NAME,
    COUNT(*) AS SERVICES_COUNT
FROM V_7206_IFINDEX_REPLACEMENT
GROUP BY CUSTOMER_ID, CUSTOMER_NAME
ORDER BY SERVICES_COUNT DESC;
```

## Маппинг индексов

Маппинг индексов хранится в VIEW `V_7206_IFINDEX_MAPPING` и использует наивное преобразование:
- Индекс 254 → MAC 00:00:00:00:02:54
- Индекс преобразуется в строку из 4 символов (с ведущими нулями)
- Строка разбивается на два байта по 2 символа
- Эти байты используются как hex строки в MAC адресе

## Примечания

- VIEW автоматически заменяет все MAC адреса в строке параметра подключения
- Если в строке несколько MAC адресов, все они будут заменены
- Операторы должны использовать NEW_VALUE для обновления services_ext.VALUE






















