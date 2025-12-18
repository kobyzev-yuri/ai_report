# Структура предоплаченных карт (Prepaid Cards) для SSG и VOIP

## Обзор

Предоплаченные карты для SSG и VOIP сервисов имеют сложную структуру:
- Карты генерируются в пакетах (BATCHES)
- Для дилеров карты нарезаются кусками через BM_SERIES
- При активации карты radiusd тарификатор создает группу сервисов
- Тарифы сервисов связаны с batches

## Архитектура

```
BATCHES (пакет карт)
    ↓
CARDS (предоплаченные карты)
    ├── SERIAL_ID → BM_SERIES (для дилеров - нарезка кусками)
    │   └── SERVICE_ID → SERVICES (сервис дилера, связка с ACCOUNT_ID)
    └── CARD_ID → SERVICES (группа сервисов при активации)
        ├── SSG (TYPE_ID=14) - групповой сервис с подписками
        ├── VOIP - услуга телефонии
        └── Доступ к ЛК (TYPE_ID=30) - веб-сервер личного кабинета
```

## Ключевые концепции

### 1. Пакеты карт (BATCHES)

- **BATCH_ID** - уникальный идентификатор пакета карт
- Карты генерируются партиями в пакетах
- **TARIFF_ID** - тарифный план для сервисов карт в пакете
- Все карты в пакете имеют одинаковый тарифный план

### 2. Карты (CARDS)

- **CARD_ID** - уникальный идентификатор карты
- **BATCH_ID** - пакет, в котором была сгенерирована карта
- **SERIAL_ID** - серия для дилеров (может быть NULL, если карта продана без дилера)
- **MONEY** - баланс карты (предоплаченная сумма)
- При активации создается группа сервисов, все ссылаются на **CARD_ID**

### 3. Серии для дилеров (BM_SERIES)

- **SERIAL_ID** - уникальный идентификатор серии
- **SERVICE_ID** - сервис дилера (связка с аккаунтом дилера через ACCOUNT_ID)
- **BATCH_ID** - пакет карт, из которого нарезается серия
- Карты из пакетов нарезаются кусками для дилеров
- Иногда карты продаются без дилерской продажи (SERIAL_ID = NULL в CARDS)

### 4. Группа сервисов при активации карты

При активации карты **radiusd тарификатор** получает запрос и создает группу сервисов:

1. **SSG сервис (TYPE_ID=14)**
   - Групповой сервис с подписками для разных технических услуг по скорости
   - Подписки активируются динамически в зависимости от порогов счетчиков

2. **VOIP сервис**
   - Услуга телефонии
   - Может иметь свой тарифный план

3. **Доступ к Личному кабинету (TYPE_ID=30)**
   - Веб-сервер личного кабинета
   - Предоставляет доступ к управлению услугами

**Все сервисы в группе ссылаются на CARD_ID** - это связывает их с картой.

### 5. Связь тарифов с batches

- Тарифы сервисов при активации карты берутся из **TARIFF_ID** пакета (BATCHES)
- Все сервисы в группе используют тарифный план из пакета карты

## Механизм работы

### Пример работы предоплаченной карты

```
1. Генерация пакета карт:
   BATCH_ID = 1000
   TARIFF_ID = 500 (тарифный план для SSG/VOIP)
   CARD_COUNT = 1000 карт

2. Нарезка для дилера (опционально):
   SERIAL_ID = 2000
   SERVICE_ID = 3000 (сервис дилера)
   BATCH_ID = 1000
   → Карты из пакета 1000 нарезаются кусками для дилера 3000

3. Активация карты:
   CARD_ID = 5000
   BATCH_ID = 1000
   SERIAL_ID = 2000 (если продана через дилера)
   
   radiusd тарификатор создает группу сервисов:
   - SERVICE_ID = 6001 (SSG, TYPE_ID=14, CARD_ID=5000)
   - SERVICE_ID = 6002 (VOIP, CARD_ID=5000)
   - SERVICE_ID = 6003 (ЛК, TYPE_ID=30, CARD_ID=5000)
   
   Все сервисы используют TARIFF_ID = 500 из пакета 1000
```

## SQL запросы

### Получить все сервисы карты

```sql
SELECT 
    c.CARD_ID,
    c.CARD_NUMBER,
    c.MONEY AS CARD_BALANCE,
    s.SERVICE_ID,
    s.LOGIN,
    s.TYPE_ID,
    bt.MNEMONIC AS SERVICE_TYPE,
    s.STATUS,
    s.START_DATE
FROM CARDS c
JOIN SERVICES s ON c.CARD_ID = s.CARD_ID
LEFT JOIN BM_TYPE bt ON s.TYPE_ID = bt.TYPE_ID
WHERE c.CARD_ID = :card_id
ORDER BY s.TYPE_ID;
```

### Получить карту по сервису

```sql
SELECT 
    s.SERVICE_ID,
    s.LOGIN,
    s.TYPE_ID,
    c.CARD_ID,
    c.CARD_NUMBER,
    c.MONEY AS CARD_BALANCE,
    c.BATCH_ID,
    b.TARIFF_ID,
    t.NAME AS TARIFF_NAME
FROM SERVICES s
JOIN CARDS c ON s.CARD_ID = c.CARD_ID
LEFT JOIN BATCHES b ON c.BATCH_ID = b.BATCH_ID
LEFT JOIN BM_TARIFF t ON b.TARIFF_ID = t.TARIFF_ID
WHERE s.SERVICE_ID = :service_id;
```

### Получить все карты пакета

```sql
SELECT 
    b.BATCH_ID,
    b.BATCH_NUMBER,
    b.TARIFF_ID,
    t.NAME AS TARIFF_NAME,
    COUNT(c.CARD_ID) AS CARD_COUNT,
    SUM(c.MONEY) AS TOTAL_BALANCE
FROM BATCHES b
LEFT JOIN CARDS c ON b.BATCH_ID = c.BATCH_ID
LEFT JOIN BM_TARIFF t ON b.TARIFF_ID = t.TARIFF_ID
WHERE b.BATCH_ID = :batch_id
GROUP BY b.BATCH_ID, b.BATCH_NUMBER, b.TARIFF_ID, t.NAME;
```

### Получить серии дилера

```sql
SELECT 
    s.SERIAL_ID,
    s.SERVICE_ID,
    serv.LOGIN AS DEALER_LOGIN,
    serv.ACCOUNT_ID,
    a.NAME AS DEALER_ACCOUNT_NAME,
    s.BATCH_ID,
    b.BATCH_NUMBER,
    COUNT(c.CARD_ID) AS CARDS_IN_SERIES,
    s.REIS_START,
    s.REIS_STOP
FROM BM_SERIES s
JOIN SERVICES serv ON s.SERVICE_ID = serv.SERVICE_ID
LEFT JOIN ACCOUNTS a ON serv.ACCOUNT_ID = a.ACCOUNT_ID
LEFT JOIN BATCHES b ON s.BATCH_ID = b.BATCH_ID
LEFT JOIN CARDS c ON s.SERIAL_ID = c.SERIAL_ID
WHERE serv.ACCOUNT_ID = :dealer_account_id
GROUP BY s.SERIAL_ID, s.SERVICE_ID, serv.LOGIN, serv.ACCOUNT_ID, a.NAME, s.BATCH_ID, b.BATCH_NUMBER, s.REIS_START, s.REIS_STOP;
```

### Получить карты без дилерской продажи

```sql
SELECT 
    c.CARD_ID,
    c.CARD_NUMBER,
    c.BATCH_ID,
    b.BATCH_NUMBER,
    c.MONEY AS CARD_BALANCE,
    c.ACTIVATION_DATE
FROM CARDS c
JOIN BATCHES b ON c.BATCH_ID = b.BATCH_ID
WHERE c.SERIAL_ID IS NULL  -- Карты без дилерской продажи
ORDER BY c.CARD_ID;
```

### Получить тарифный план пакета

```sql
SELECT 
    b.BATCH_ID,
    b.BATCH_NUMBER,
    b.TARIFF_ID,
    t.NAME AS TARIFF_NAME,
    t.DESCRIPTION AS TARIFF_DESCRIPTION,
    tel.TARIFFEL_ID,
    tel.MONEY AS TARIFFEL_PRICE,
    ttype.MNEMONIC AS TARIFFEL_TYPE,
    ttype.NAME AS TARIFFEL_TYPE_NAME
FROM BATCHES b
JOIN BM_TARIFF t ON b.TARIFF_ID = t.TARIFF_ID
LEFT JOIN BM_TARIFFEL tel ON t.TARIFF_ID = tel.TARIFF_ID
LEFT JOIN BM_TARIFFEL_TYPE ttype ON tel.TARIFFEL_TYPE_ID = ttype.TARIFFEL_TYPE_ID
WHERE b.BATCH_ID = :batch_id
  AND (tel.DATE_END IS NULL OR tel.DATE_END >= SYSDATE)
ORDER BY ttype.MNEMONIC;
```

## Важные моменты

1. **Генерация карт в пакетах**
   - Карты генерируются партиями в BATCHES
   - Все карты в пакете имеют одинаковый тарифный план (TARIFF_ID)

2. **Нарезка для дилеров**
   - Карты из пакетов нарезаются кусками через BM_SERIES
   - SERIAL_ID связывается с SERVICE_ID дилера (аккаунт дилера через ACCOUNT_ID)
   - Иногда карты продаются без дилерской продажи (SERIAL_ID = NULL)

3. **Активация карты**
   - radiusd тарификатор получает запрос на активацию
   - Создает группу сервисов: SSG (TYPE_ID=14), VOIP, доступ к ЛК (TYPE_ID=30)
   - Все сервисы ссылаются на CARD_ID

4. **Связь тарифов с batches**
   - Тарифы сервисов при активации берутся из TARIFF_ID пакета
   - Все сервисы в группе используют тарифный план из пакета карты

5. **Группа сервисов**
   - SSG (TYPE_ID=14) - групповой сервис с подписками
   - VOIP - услуга телефонии
   - Доступ к ЛК (TYPE_ID=30) - веб-сервер личного кабинета
   - Все связаны через CARD_ID

6. **Доступ к Личному кабинету (TYPE_ID=30)**
   - Чувствительные данные (логины, пароли) не должны выводиться на экран
   - Используется для управления услугами через веб-интерфейс

