# Структура SSG сервисов (TYPE_ID=14)

## Обзор

SSG (TYPE_ID=14) - это **самый сложный тип тарификации** в биллинге. SSG сервис представляет собой **групповой сервис** с подписками для разных технических услуг по скорости.

## Архитектура SSG

```
SERVICES (TYPE_ID=14) - групповой сервис SSG
    ↓
BM_SRV_CNT_LINK - связь сервиса с подписками и счетчиками
    ├── SUBSCRIPTION_ID → BM_SUBSCRIPTION (подписки для разных скоростей)
    └── COUNTER_ID → BM_COUNTER (счетчики для отслеживания порогов)
        ↓
BM_TARIFFEL (условия активации подписок)
    ├── COUNTER_CLASS_ID (связь с классом счетчика)
    ├── FLAG_VALUE (логика применения: 0 = при превышении порога)
    └── STEP1-STEP4 (пороги для активации подписок)
```

## Ключевые концепции

### 1. Групповой сервис (SERVICE_ID)

- **SERVICE_ID** в `SERVICES` с `TYPE_ID=14` - это **групповой сервис**
- Один групповой сервис может содержать несколько подписок
- Каждая подписка представляет собой техническую услугу с определенной скоростью

### 2. Подписки (SUBSCRIPTION_ID)

- **SUBSCRIPTION_ID** в `BM_SUBSCRIPTION` - подписки для разных технических услуг по скорости
- Примеры скоростей: 512/256, 1024/512, 2048/1024 и т.д.
- Подписки активируются **динамически** в зависимости от условий в тарифном плане

### 3. Счетчики и пороги

- **COUNTER_ID** в `BM_COUNTER` отслеживает потребление (`VALUE`)
- **THRESHOLD** определяет порог для активации подписки
- **FLAG_VALUE = 0** для SSG: подписка активируется при `VALUE >= THRESHOLD`

### 4. Условия активации в тарифном плане

- Условия активации подписок определяются в `BM_TARIFFEL`
- `COUNTER_CLASS_ID` связывает тарифный элемент с классом счетчика
- `STEP1-STEP4` могут определять пороги для разных уровней подписок
- `FLAG_VALUE = 0` означает активацию при превышении порога

## Механизм работы

### Пример работы SSG сервиса

```
Групповой сервис SSG: SERVICE_ID = 1000, TYPE_ID = 14

Подписки:
1. SUBSCRIPTION_ID = 2001 (скорость 512/256)
   - COUNTER_ID = 3001
   - THRESHOLD = 100 GB
   - Активация: VALUE >= 100 GB

2. SUBSCRIPTION_ID = 2002 (скорость 1024/512)
   - COUNTER_ID = 3002
   - THRESHOLD = 500 GB
   - Активация: VALUE >= 500 GB

3. SUBSCRIPTION_ID = 2003 (скорость 2048/1024)
   - COUNTER_ID = 3003
   - THRESHOLD = 1000 GB
   - Активация: VALUE >= 1000 GB

Логика:
- Если VALUE < 100 GB → активна базовая подписка (или нет подписки)
- Если 100 GB <= VALUE < 500 GB → активирована подписка 2001 (512/256)
- Если 500 GB <= VALUE < 1000 GB → активирована подписка 2002 (1024/512)
- Если VALUE >= 1000 GB → активирована подписка 2003 (2048/1024)
```

## SQL запросы

### Получить все подписки SSG сервиса

```sql
SELECT 
    s.SERVICE_ID,
    s.LOGIN,
    s.TYPE_ID,
    scl.SUBSCRIPTION_ID,
    sub.ENABLED AS SUBSCRIPTION_ENABLED,
    scl.COUNTER_ID,
    c.VALUE AS COUNTER_VALUE,
    c.THRESHOLD,
    CASE 
        WHEN c.VALUE >= c.THRESHOLD THEN 'Активна'
        ELSE 'Неактивна'
    END AS SUBSCRIPTION_STATUS
FROM SERVICES s
JOIN BM_SRV_CNT_LINK scl ON s.SERVICE_ID = scl.SERVICE_ID
LEFT JOIN BM_SUBSCRIPTION sub ON scl.SUBSCRIPTION_ID = sub.SUBSCRIPTION_ID
LEFT JOIN BM_COUNTER c ON scl.COUNTER_ID = c.COUNTER_ID
WHERE s.TYPE_ID = 14
  AND s.SERVICE_ID = :service_id
ORDER BY c.THRESHOLD;
```

### Получить активные подписки SSG сервиса (с учетом порогов)

```sql
SELECT 
    s.SERVICE_ID,
    s.LOGIN,
    scl.SUBSCRIPTION_ID,
    c.VALUE AS COUNTER_VALUE,
    c.THRESHOLD,
    tel.MONEY AS SUBSCRIPTION_PRICE,
    tel.STEP1, tel.STEP2, tel.STEP3, tel.STEP4
FROM SERVICES s
JOIN BM_SRV_CNT_LINK scl ON s.SERVICE_ID = scl.SERVICE_ID
JOIN BM_COUNTER c ON scl.COUNTER_ID = c.COUNTER_ID
LEFT JOIN BM_TARIFFEL tel ON c.COUNTER_CLASS_ID = tel.COUNTER_CLASS_ID
    AND tel.TARIFFEL_TYPE_ID IN (
        SELECT TARIFFEL_TYPE_ID 
        FROM BM_TARIFFEL_TYPE 
        WHERE MNEMONIC = 'counter_cf'
    )
WHERE s.TYPE_ID = 14
  AND s.SERVICE_ID = :service_id
  AND c.VALUE >= c.THRESHOLD  -- FLAG_VALUE = 0 для SSG
  AND tel.FLAG_VALUE = 0
ORDER BY c.THRESHOLD DESC;  -- От большего порога к меньшему
```

### Получить тарифный план SSG с условиями активации подписок

```sql
SELECT 
    t.TARIFF_ID,
    t.NAME AS TARIFF_NAME,
    tel.TARIFFEL_ID,
    tel.MONEY AS SUBSCRIPTION_PRICE,
    tel.STEP1, tel.STEP2, tel.STEP3, tel.STEP4,
    tel.COUNTER_CLASS_ID,
    tel.FLAG_VALUE,
    ttype.MNEMONIC AS TARIFFEL_TYPE,
    ttype.NAME AS TARIFFEL_TYPE_NAME
FROM BM_TARIFF t
JOIN BM_TARIFFEL tel ON t.TARIFF_ID = tel.TARIFF_ID
LEFT JOIN BM_TARIFFEL_TYPE ttype ON tel.TARIFFEL_TYPE_ID = ttype.TARIFFEL_TYPE_ID
WHERE t.TYPE_ID = 14
  AND (tel.DATE_END IS NULL OR tel.DATE_END >= SYSDATE)
ORDER BY tel.STEP1, tel.STEP2, tel.STEP3, tel.STEP4;
```

## Важные моменты

1. **SSG (TYPE_ID=14) - самый сложный тип тарификации**
   - Групповой сервис с динамической активацией подписок
   - Подписки активируются в зависимости от порогов счетчиков

2. **Подписки для разных скоростей**
   - Каждая подписка представляет техническую услугу с определенной скоростью
   - Примеры: 512/256, 1024/512, 2048/1024 и т.д.

3. **Динамическая активация**
   - Подписки активируются автоматически при достижении порогов
   - Пороги определяются в тарифном плане через `BM_TARIFFEL`

4. **FLAG_VALUE = 0 для SSG**
   - Подписка активируется при `VALUE >= THRESHOLD`
   - Чем больше потребление, тем выше скорость подписки

5. **Связь через BM_SRV_CNT_LINK**
   - `SERVICE_ID` - групповой сервис SSG
   - `SUBSCRIPTION_ID` - подписка для определенной скорости
   - `COUNTER_ID` - счетчик для отслеживания порога

6. **Условия в тарифном плане**
   - `COUNTER_CLASS_ID` связывает тарифный элемент с классом счетчика
   - `STEP1-STEP4` могут определять пороги для разных уровней подписок
   - `MONEY` определяет цену подписки

