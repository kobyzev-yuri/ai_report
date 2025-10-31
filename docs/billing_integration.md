# Billing Integration - База знаний

## Обзор
Документ содержит описание структуры таблиц Oracle биллинга (billing7@bm7) для интеграции с системой отчетности.

**Примечание по типам данных**: 
- `DOUBLE PRECISION` = `NUMBER` в Oracle (yasql отображает NUMBER как DOUBLE PRECISION)

---

## Таблицы

### SERVICES
**Назначение**: Основная таблица услуг/сервисов в биллинговой системе.

**Структура**:
```sql
SERVICE_ID          NULL  DOUBLE PRECISION    -- ID услуги
DOMAIN_ID           NULL  DOUBLE PRECISION    -- ID домена
GROUP_ID            NULL  DOUBLE PRECISION    -- ID группы
CUSTOMER_ID         NULL  DOUBLE PRECISION    -- ID клиента
ACCOUNT_ID          NULL  DOUBLE PRECISION    -- ID аккаунта
TYPE_ID             NULL  DOUBLE PRECISION    -- ID типа услуги
TARIFF_ID           NULL  DOUBLE PRECISION    -- ID тарифа
CARD_ID             NULL  DOUBLE PRECISION    -- ID карты
LOGIN               NULL  VARCHAR2(255)       -- Логин
PASSWD              NULL  VARCHAR2(255)       -- Пароль
CR_PASSWD           NULL  VARCHAR2(255)       -- CR пароль
STATUS              NULL  DOUBLE PRECISION    -- Статус
ACTUAL_STATUS       NULL  DOUBLE PRECISION    -- Актуальный статус
DESCRIPTION         NULL  VARCHAR2(512)       -- Описание
CREATE_DATE         NULL  DATE                -- Дата создания
RECKONING_DATE      NULL  DATE                -- Дата начала расчетов
ACTIVITY_DATE       NULL  DATE                -- Дата активности
STATUS_DATE         NULL  DATE                -- Дата изменения статуса
START_DATE          NULL  DATE                -- Дата начала
STOP_DATE           NULL  DATE                -- Дата окончания
PROXY_AP_ID         NULL  DOUBLE PRECISION    -- ID прокси точки доступа
OPEN_DATE           NULL  DATE                -- Дата открытия
SUB_TYPE_ID         NULL  DOUBLE PRECISION    -- ID подтипа
VSAT                NULL  VARCHAR2(255)       -- VSAT
BLANK               NULL  VARCHAR2(255)       -- Blank
ADDRESS             NULL  VARCHAR2(255)       -- Адрес
EXPORT_DATE         NULL  DATE                -- Дата экспорта
CLOSE_DATE          NULL  DATE                -- Дата закрытия
PSTN_PD             NULL  DOUBLE PRECISION    -- PSTN PD
BALANCE_IRIDIUM     NULL  DOUBLE PRECISION    -- Баланс Iridium
STATUS_IRIDIUM      NULL  VARCHAR2(255)       -- Статус Iridium
SSORM               NULL  DOUBLE PRECISION    -- СОРМ
OLD_BALANCE_IRIDIUM NULL  DOUBLE PRECISION    -- Старый баланс Iridium
LAT                 NULL  VARCHAR2(255)       -- Широта
LON                 NULL  VARCHAR2(255)       -- Долгота
CRM_DATE            NULL  DATE                -- Дата CRM
```

**Ключевые поля**:
- `SERVICE_ID` - первичный ключ
- `TYPE_ID` - тип услуги (9002 = SBD)
- `LOGIN` - идентификатор услуги, связь с Iridium данными (Contract_ID)
- `PASSWD` - IMEI устройства
- `CUSTOMER_ID` - связь с клиентом
- `ACCOUNT_ID` - связь со счетом
- `TARIFF_ID` - связь с тарифным планом
- `STATUS`, `ACTUAL_STATUS` - состояние услуги (10 = активна, 2 = actual status)
- `BALANCE_IRIDIUM` - текущий баланс
- `OLD_BALANCE_IRIDIUM` - предыдущий баланс

**Связи**: 
- → `CUSTOMERS` (через CUSTOMER_ID)
- → `ACCOUNTS` (через ACCOUNT_ID)
- → `TARIFFS` (через TARIFF_ID)
- → **Iridium данные** (через LOGIN = Contract_ID)

**Фильтры для отчетов**:
```sql
-- Только SBD сервисы
WHERE TYPE_ID = 9002

-- Связь с Iridium данными
WHERE LOGIN = 'SUB-52477661656'  -- Contract_ID из SPNET_TRAFFIC/STECCOM_EXPENSES
```

**Пример данных**:
```
SERVICE_ID: 446053
TYPE_ID: 9002 (SBD)
TARIFF_ID: 137848
LOGIN: SUB-52477661656 (= Contract_ID в Iridium)
PASSWD: 300234060238170 (= IMEI)
CUSTOMER_ID: 241935
ACCOUNT_ID: 304973
STATUS: 10 (активна)
ACTUAL_STATUS: 2
BALANCE_IRIDIUM: 134330 (копейки?)
VSAT: 300234060238170 (дублирует IMEI)
BLANK: 34 (код клиента?)
DESCRIPTION: 300234060238170:34:Арктический и антарктический научно-исследовательский институт (ААНИИ ФГБУ)
CREATE_DATE: 2024-06-01
RECKONING_DATE: 2025-11-01 (дата следующего расчета?)
```

**Примечания**:
- **TYPE_ID = 9002** - сервисы SBD (Iridium Short Burst Data)
- **LOGIN** - это **Contract_ID** из файлов SPNet/STECCOM - ключевое поле для связи!
- **PASSWD** хранит **IMEI** устройства
- VSAT дублирует IMEI из PASSWD
- **BLANK** - **номер первого приложения к договору** (в ACCOUNTS.DESCRIPTION - номер договора)
- DESCRIPTION в формате: `IMEI:НомерПриложения:НазваниеКлиента`
- BALANCE_IRIDIUM - только для prepaid voice (TYPE_ID=9001), не для SBD
- RECKONING_DATE - дата списания абонплаты (для финансовых отчетов не важно)

---

### ACCOUNTS
**Назначение**: Таблица договоров (лицевых счетов).

**Структура**:
```sql
ACCOUNT_ID                   NULL  DOUBLE PRECISION    -- ID лицевого счета
DOMAIN_ID                    NULL  DOUBLE PRECISION    -- ID домена
GROUP_ID                     NULL  DOUBLE PRECISION    -- ID группы
CUSTOMER_ID                  NULL  DOUBLE PRECISION    -- ID абонента
AGREEMENT_ID                 NULL  DOUBLE PRECISION    -- ID договора
CURRENCY_ID                  NULL  DOUBLE PRECISION    -- ID валюты
MONEY                        NULL  NUMBER(15,5)        -- Баланс счета
THRESHOLD                    NULL  NUMBER(15,5)        -- Порог блокировки
THRESHOLD_PERIODIC           NULL  NUMBER(15,5)        -- Периодический порог
NDS_PERCENT                  NULL  NUMBER(15,5)        -- Процент НДС
NSP_PERCENT                  NULL  NUMBER(15,5)        -- Процент НСП
STATUS                       NULL  DOUBLE PRECISION    -- Статус счета
WARNINGS                     NULL  DOUBLE PRECISION    -- Предупреждения
DESCRIPTION                  NULL  VARCHAR2(255)       -- Описание
CREATE_DATE                  NULL  DATE                -- Дата создания
ACTIVITY_DATE                NULL  DATE                -- Дата активности
STATUS_DATE                  NULL  DATE                -- Дата изменения статуса
STOP_DATE                    NULL  DATE                -- Дата остановки
KEEP_ON_IF_UNPAID            NULL  DOUBLE PRECISION    -- Не блокировать при задолженности
GUARANTEE_LETTER_DATE        NULL  DATE                -- Дата гарантийного письма
GUARANTEE_LETTER_DESCRIPTION NULL  VARCHAR2(255)       -- Описание гарантийного письма
TYPE_PAY                     NULL  VARCHAR2(255)       -- Тип оплаты
TRANS_ACCOUNT_ID             NULL  DOUBLE PRECISION    -- ID транзитного счета
CORRECTION_VID               NULL  DOUBLE PRECISION    -- Вид корректировки
ACCOUNT_START                NULL  DATE                -- Дата начала действия счета
ACCOUNT_STOP                 NULL  DATE                -- Дата окончания действия счета
ACCOUNT_MONEY                NULL  DOUBLE PRECISION    -- Сумма по счету
IS_UE                        NULL  DOUBLE PRECISION    -- Признак ЮЛ/ФЛ
OLD_CURRENCY_ID              NULL  DOUBLE PRECISION    -- Старый ID валюты
```

**Ключевые поля**:
- `ACCOUNT_ID` - первичный ключ, связь с SERVICES
- `CUSTOMER_ID` - связь с CUSTOMERS
- `AGREEMENT_ID` - внутренний ID договора
- **`DESCRIPTION`** - **номер договора в СТЭККОМ** (основной идентификатор договора)
- `MONEY` - текущий баланс лицевого счета
- `NDS_PERCENT` - процент НДС для расчетов
- `STATUS` - статус договора
- `CURRENCY_ID` - валюта счета

**Связи**:
- ← `SERVICES` (через ACCOUNT_ID)
- → `CUSTOMERS` (через CUSTOMER_ID)

**Примечания**:
- Содержит финансовую информацию по договорам
- **`DESCRIPTION`** - номер договора в СТЭККОМ (основной идентификатор для документов)
- `MONEY` - текущий баланс (NUMBER(15,5) = точность до 5 знаков)
- `NDS_PERCENT`, `NSP_PERCENT` - налоговые параметры
- `IS_UE` - признак юридического/физического лица
- Связь с услугами: `SERVICES.BLANK` содержит номер первого приложения к этому договору

---

### CUSTOMERS
**Назначение**: Таблица абонентов (клиентов).

**Структура**:
```sql
CUSTOMER_ID      NULL  DOUBLE PRECISION    -- ID абонента
DOMAIN_ID        NULL  DOUBLE PRECISION    -- ID домена
GROUP_ID         NULL  DOUBLE PRECISION    -- ID группы
CUSTOMER_TYPE_ID NULL  DOUBLE PRECISION    -- ID типа абонента
STATUS           NULL  DOUBLE PRECISION    -- Статус
CREATE_DATE      NULL  DATE                -- Дата создания
ACTIVITY_DATE    NULL  DATE                -- Дата активности
STATUS_DATE      NULL  DATE                -- Дата изменения статуса
STOP_DATE        NULL  DATE                -- Дата остановки
NOT_EXPORT       NULL  DOUBLE PRECISION    -- Не экспортировать
CSORM            NULL  DOUBLE PRECISION    -- СОРМ
```

**Ключевые поля**:
- `CUSTOMER_ID` - первичный ключ
- `CUSTOMER_TYPE_ID` - тип абонента
- `STATUS` - статус абонента
- `CSORM` - признак СОРМ

**Связи**:
- ← `SERVICES` (через CUSTOMER_ID)
- ← `ACCOUNTS` (через CUSTOMER_ID)

**Примечания**:
- Базовая справочная таблица абонентов
- Дополнительная информация об абонентах хранится в связанных таблицах (BM_CUSTOMER_CONTACT и т.д.)

---

### BM_CUSTOMER_CONTACT
**Назначение**: Контактная информация абонентов (название компании, ФИО, телефоны и т.д.).

**Структура**:
```sql
CUSTOMER_CONTACT_ID NULL  DOUBLE PRECISION    -- ID контакта
CUSTOMER_ID         NULL  DOUBLE PRECISION    -- ID абонента
CONTACT_DICT_ID     NULL  DOUBLE PRECISION    -- ID типа контакта (из словаря)
VALUE               NULL  VARCHAR2(1020)      -- Значение контакта
```

**Ключевые поля**:
- `CUSTOMER_CONTACT_ID` - первичный ключ
- `CUSTOMER_ID` - связь с CUSTOMERS
- `CONTACT_DICT_ID` - связь с BM_CONTACT_DICT (тип контакта)
- `VALUE` - значение (название, ФИО, телефон и т.д.)

**Связи**:
- → `CUSTOMERS` (через CUSTOMER_ID)
- → `BM_CONTACT_DICT` (через CONTACT_DICT_ID)

**Примечания**:
- Используется для получения названия компании или ФИО абонента
- Один абонент может иметь несколько контактов разных типов

---

### BM_CONTACT_DICT
**Назначение**: Справочник типов контактной информации.

**Структура**:
```sql
CONTACT_DICT_ID NULL  DOUBLE PRECISION    -- ID типа контакта
MNEMONIC        NULL  VARCHAR2(255)       -- Мнемоника (код типа)
FORMAT_ID       NULL  DOUBLE PRECISION    -- ID формата
NAME            NULL  VARCHAR2(255)       -- Название типа контакта
CTL_TYPE        NULL  VARCHAR2(255)       -- Тип контрола
```

**Ключевые поля**:
- `CONTACT_DICT_ID` - первичный ключ
- `MNEMONIC` - символьный код типа контакта (удобно для запросов)
- `NAME` - название типа контакта (например: "Название компании", "ФИО", "Телефон")

**Связи**:
- ← `BM_CUSTOMER_CONTACT` (через CONTACT_DICT_ID)

**Примечания**:
- Справочная таблица для типов контактов
- Используется совместно с BM_CUSTOMER_CONTACT для извлечения информации об абоненте

**Основные типы контактов (MNEMONIC)**:
- `b_name` (ID=11) - Organization name (название организации)
- `first_name` (ID=1) - First name (имя)
- `last_name` (ID=3) - Last name (фамилия)
- `middle_name` (ID=4) - Middle name (отчество)
- `passport` (ID=5) - Passport (паспорт)

**Пример запроса для получения названия организации**:
```sql
SELECT 
    c.CUSTOMER_ID,
    cc.VALUE as organization_name
FROM CUSTOMERS c
JOIN BM_CUSTOMER_CONTACT cc ON c.CUSTOMER_ID = cc.CUSTOMER_ID
JOIN BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID
WHERE c.CUSTOMER_ID = 241935
  AND cd.MNEMONIC = 'b_name'
```

**Пример запроса для получения ФИО физического лица**:
```sql
SELECT 
    c.CUSTOMER_ID,
    MAX(CASE WHEN cd.MNEMONIC = 'last_name' THEN cc.VALUE END) as last_name,
    MAX(CASE WHEN cd.MNEMONIC = 'first_name' THEN cc.VALUE END) as first_name,
    MAX(CASE WHEN cd.MNEMONIC = 'middle_name' THEN cc.VALUE END) as middle_name
FROM CUSTOMERS c
JOIN BM_CUSTOMER_CONTACT cc ON c.CUSTOMER_ID = cc.CUSTOMER_ID
JOIN BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID
WHERE c.CUSTOMER_ID = 241935
  AND cd.MNEMONIC IN ('first_name', 'last_name', 'middle_name')
GROUP BY c.CUSTOMER_ID
```

---

### OUTER_IDS
**Назначение**: Таблица связи внутренних ID биллинга с внешними системами (1С и др.).

**Структура**:
```sql
EXT_ID NULL  VARCHAR2(255)       -- Внешний ID (например, код из 1С)
PARENT NULL  VARCHAR2(255)       -- Родительский элемент
ID     NULL  DOUBLE PRECISION    -- Внутренний ID из биллинга
TBL    NULL  VARCHAR2(255)       -- Название таблицы (CUSTOMERS, ACCOUNTS и т.д.)
```

**Ключевые поля**:
- `EXT_ID` - внешний идентификатор (код из 1С)
- `ID` - внутренний ID из биллинга
- `TBL` - название таблицы, к которой относится ID

**Использование**:
```sql
-- Получить код 1С по customer_id
SELECT ext_id AS code_1c
FROM outer_ids
WHERE id = 241935
  AND tbl = 'CUSTOMERS';

-- Найти customer_id по коду 1С
SELECT id AS customer_id
FROM outer_ids
WHERE ext_id = '00007428'
  AND tbl = 'CUSTOMERS';
```

**Примечания**:
- `EXT_ID` для CUSTOMERS - это код клиента из 1С
- Универсальная таблица для связи с разными внешними системами
- Поле TBL указывает на какую таблицу ссылается ID

---

## Views для интеграции

### V_IRIDIUM_SERVICES_INFO
**Назначение**: Представление для связки данных биллинга с отчетами Iridium M2M.

**Описание**: Объединяет все необходимые данные для идентификации сервисов SBD и их владельцев.

**Ключевые поля для интеграции**:
- `CONTRACT_ID` (LOGIN) - связь с SPNET_TRAFFIC.CONTRACT_ID и STECCOM_EXPENSES.CONTRACT_ID
- `IMEI` (PASSWD) - связь с SPNET_TRAFFIC.IMEI и STECCOM_EXPENSES.IMEI
- `AGREEMENT_NUMBER` - номер договора в СТЭККОМ
- `ORDER_NUMBER` - номер заказа/приложения
- `CUSTOMER_NAME` - название организации или ФИО
- `CODE_1C` - код клиента из 1С

**DDL**:
```sql
CREATE OR REPLACE VIEW V_IRIDIUM_SERVICES_INFO AS
SELECT 
    s.SERVICE_ID,
    s.LOGIN AS CONTRACT_ID,
    s.PASSWD AS IMEI,
    s.TARIFF_ID,
    a.DESCRIPTION AS AGREEMENT_NUMBER,
    s.BLANK AS ORDER_NUMBER,
    s.STATUS,
    s.ACTUAL_STATUS,
    c.CUSTOMER_ID,
    -- Название организации (для юр.лиц)
    MAX(CASE WHEN cd.MNEMONIC = 'b_name' THEN cc.VALUE END) AS ORGANIZATION_NAME,
    -- ФИО для физ.лиц (собираем в одну строку)
    TRIM(
        COALESCE(MAX(CASE WHEN cd.MNEMONIC = 'last_name' THEN cc.VALUE END), '') || ' ' ||
        COALESCE(MAX(CASE WHEN cd.MNEMONIC = 'first_name' THEN cc.VALUE END), '') || ' ' ||
        COALESCE(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' THEN cc.VALUE END), '')
    ) AS PERSON_NAME,
    -- Универсальное поле (название организации или ФИО)
    COALESCE(
        MAX(CASE WHEN cd.MNEMONIC = 'b_name' THEN cc.VALUE END),
        TRIM(
            COALESCE(MAX(CASE WHEN cd.MNEMONIC = 'last_name' THEN cc.VALUE END), '') || ' ' ||
            COALESCE(MAX(CASE WHEN cd.MNEMONIC = 'first_name' THEN cc.VALUE END), '') || ' ' ||
            COALESCE(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' THEN cc.VALUE END), '')
        )
    ) AS CUSTOMER_NAME,
    s.CREATE_DATE,
    s.START_DATE,
    s.STOP_DATE,
    a.ACCOUNT_ID,
    oi.EXT_ID AS CODE_1C
FROM SERVICES s
JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
JOIN CUSTOMERS c ON s.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN BM_CUSTOMER_CONTACT cc ON c.CUSTOMER_ID = cc.CUSTOMER_ID
LEFT JOIN BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID
    AND cd.MNEMONIC IN ('b_name', 'first_name', 'last_name', 'middle_name')
LEFT JOIN OUTER_IDS oi ON c.CUSTOMER_ID = oi.ID 
    AND oi.TBL = 'CUSTOMERS'
WHERE s.TYPE_ID = 9002  -- Только SBD сервисы
GROUP BY 
    s.SERVICE_ID,
    s.LOGIN,
    s.PASSWD,
    s.TARIFF_ID,
    a.DESCRIPTION,
    s.BLANK,
    s.STATUS,
    s.ACTUAL_STATUS,
    c.CUSTOMER_ID,
    s.CREATE_DATE,
    s.START_DATE,
    s.STOP_DATE,
    a.ACCOUNT_ID,
    oi.EXT_ID;
```

**Использование в отчетах**:
```sql
-- Пример 1: Связать с нашим отчетом по Contract_ID
SELECT 
    cor.*,
    v.CUSTOMER_NAME,
    v.AGREEMENT_NUMBER,
    v.ORDER_NUMBER,
    v.CODE_1C
FROM V_CONSOLIDATED_OVERAGE_REPORT cor
LEFT JOIN V_IRIDIUM_SERVICES_INFO v 
    ON cor.CONTRACT_ID = v.CONTRACT_ID;

-- Пример 2: Связать по IMEI
SELECT 
    st.*,
    v.CUSTOMER_NAME,
    v.AGREEMENT_NUMBER,
    v.ORDER_NUMBER,
    v.CODE_1C
FROM SPNET_TRAFFIC st
LEFT JOIN V_IRIDIUM_SERVICES_INFO v 
    ON st.IMEI = v.IMEI;

-- Пример 3: Получить все активные SBD сервисы с клиентами
SELECT 
    CONTRACT_ID,
    IMEI,
    CUSTOMER_NAME,
    AGREEMENT_NUMBER,
    ORDER_NUMBER,
    CODE_1C,
    STATUS
FROM V_IRIDIUM_SERVICES_INFO
WHERE STATUS = 10  -- активные
ORDER BY CUSTOMER_NAME;
```

**Примечания**:
- View фильтрует только SBD сервисы (TYPE_ID = 9002)
- `CUSTOMER_NAME` автоматически выбирает название организации (b_name) или ФИО
- Для физ.лиц ФИО собирается в формате: "Фамилия Имя Отчество"
- `CONTRACT_ID` = LOGIN из SERVICES - ключевое поле для связи с Iridium данными
- `AGREEMENT_NUMBER` = номер договора в СТЭККОМ
- `ORDER_NUMBER` = номер заказа/приложения к договору
- `CODE_1C` = код клиента из системы 1С (через OUTER_IDS)

---

### V_CONSOLIDATED_REPORT_WITH_BILLING
**Назначение**: Расширенный отчет с данными из биллинга (название клиента, договор, код 1С).

**Описание**: Объединяет консолидированный отчет по превышениям (V_CONSOLIDATED_OVERAGE_REPORT) с информацией о клиентах из биллинга.

**Добавленные поля из биллинга**:
- `CODE_1C` - код клиента из 1С
- `CUSTOMER_NAME` - название организации или ФИО
- `AGREEMENT_NUMBER` - номер договора в СТЭККОМ
- `ORDER_NUMBER` - номер заказа/приложения

**DDL**:
```sql
CREATE OR REPLACE VIEW V_CONSOLIDATED_REPORT_WITH_BILLING AS
SELECT 
    -- Все поля из основного отчета
    cor.BILL_MONTH,
    cor.IMEI,
    cor.CONTRACT_ID,
    cor.PLAN_NAME,
    cor.ACTIVATION_DATE,
    cor.SPNET_TOTAL_AMOUNT,
    cor.INCLUDED_KB,
    cor.TOTAL_USAGE_KB,
    cor.OVERAGE_KB,
    cor.CALCULATED_OVERAGE,
    cor.STECCOM_TOTAL_AMOUNT,
    -- Добавляем данные из биллинга
    v.CODE_1C,
    v.CUSTOMER_NAME,
    v.AGREEMENT_NUMBER,
    v.ORDER_NUMBER,
    v.STATUS AS SERVICE_STATUS,
    v.CUSTOMER_ID,
    v.ACCOUNT_ID
FROM V_CONSOLIDATED_OVERAGE_REPORT cor
LEFT JOIN V_IRIDIUM_SERVICES_INFO v 
    ON cor.CONTRACT_ID = v.CONTRACT_ID;
```

**Использование в Streamlit**:
```python
# В streamlit_report.py изменить основной запрос:
def get_main_report(selected_period=None, selected_plan=None):
    query = """
        SELECT 
            BILL_MONTH,
            IMEI,
            CONTRACT_ID,
            PLAN_NAME,
            ACTIVATION_DATE,
            SPNET_TOTAL_AMOUNT,
            INCLUDED_KB,
            TOTAL_USAGE_KB,
            OVERAGE_KB,
            CALCULATED_OVERAGE,
            STECCOM_TOTAL_AMOUNT,
            CODE_1C,
            CUSTOMER_NAME,
            AGREEMENT_NUMBER,
            ORDER_NUMBER
        FROM V_CONSOLIDATED_REPORT_WITH_BILLING
        WHERE 1=1
    """
    # ... добавить фильтры ...
    return pd.read_sql_query(query, conn)
```

**Пример запроса с фильтрами**:
```sql
-- Отчет за сентябрь 2025 с данными клиентов
SELECT 
    CUSTOMER_NAME,
    CODE_1C,
    AGREEMENT_NUMBER,
    ORDER_NUMBER,
    IMEI,
    PLAN_NAME,
    SPNET_TOTAL_AMOUNT,
    CALCULATED_OVERAGE,
    STECCOM_TOTAL_AMOUNT
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE BILL_MONTH = 92025
ORDER BY CUSTOMER_NAME, IMEI;

-- Отчет по конкретному клиенту (по коду 1С)
SELECT *
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE CODE_1C = '00007428'
ORDER BY BILL_MONTH DESC, IMEI;

-- Сводка по договорам
SELECT 
    AGREEMENT_NUMBER,
    CUSTOMER_NAME,
    COUNT(*) as services_count,
    SUM(SPNET_TOTAL_AMOUNT) as total_spnet,
    SUM(CALCULATED_OVERAGE) as total_overage,
    SUM(STECCOM_TOTAL_AMOUNT) as total_steccom
FROM V_CONSOLIDATED_REPORT_WITH_BILLING
WHERE BILL_MONTH = 92025
GROUP BY AGREEMENT_NUMBER, CUSTOMER_NAME
ORDER BY total_overage DESC;
```

**Примечания**:
- Использует LEFT JOIN, поэтому показывает все записи из отчета, даже если данных в биллинге нет
- Поля из биллинга будут NULL, если CONTRACT_ID не найден в SERVICES
- Готово для использования в Streamlit отчетах
- Можно добавить фильтр по CODE_1C или CUSTOMER_NAME для поиска по клиентам

---


