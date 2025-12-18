# Структура тарифных планов в биллинге

## Обзор

Тарифные планы в биллинге состоят из нескольких связанных таблиц:
- **BM_TARIFF** - основная таблица тарифов
- **BM_TARIFFEL** - тарифные элементы (составляющие тарифа с ценами)
- **BM_TARIFFEL_TYPE** - справочник типов тарифных элементов (статьи тарификации)
- **BM_ZONE** - справочник тарифных зон
- **BM_COUNTER** - счетчики для ступенчатой тарификации
- **BM_SRV_CNT_LINK** - связь счетчиков с сервисами

## Иерархия структуры

```
BM_TARIFF (тарифный план)
    ↓
BM_TARIFFEL (тарифные элементы с ценами)
    ├── BM_TARIFFEL_TYPE (тип статьи: абонплата, трафик, счетчик)
    ├── BM_ZONE (зона тарификации)
    ├── BM_COUNTER_CLASS (класс счетчика для коэффициента скидки)
    └── BM_RESOURCE_TYPE (расшифровка для бухгалтерии)
    
BM_SRV_CNT_LINK (связь счетчиков с сервисами)
    ├── SERVICES (услуга)
    └── BM_COUNTER (счетчик)
```

## Статьи тарификации (BM_TARIFFEL_TYPE)

Типы тарифных элементов определяются через `BM_TARIFFEL_TYPE.MNEMONIC`:

### 1. Абонентская плата (`fee`)
- **Тип**: `ENTITY_TYPE = 'unique'` (один элемент в тарифе)
- **Описание**: Фиксированная ежемесячная плата
- **Пример**: `MONEY = 8.5` - абонентская плата 8.5 руб/мес

### 2. Плата за трафик (`traffic_pay`, `ll_traffic_pay`)
- **Тип**: `ENTITY_TYPE = 'multi'` (может быть несколько элементов для разных зон)
- **Описание**: Переменная плата за объем трафика
- **Использование**: `STEP1-STEP4` для ступенчатой тарификации
- **Пример**: `MONEY = 1.74` за единицу трафика, `STEP1 = 10` - первая ступень

### 3. Коэффициент счетчика (`counter_cf`)
- **Тип**: `ENTITY_TYPE = 'multi'`
- **Описание**: Коэффициент скидки при достижении порога счетчика
- **Связь**: `COUNTER_CLASS_ID` связывает с классом счетчика
- **Механизм**: Когда счетчик (`BM_COUNTER`) достигает порога (`THRESHOLD`), активируется коэффициент скидки к основной цене за зону

### 4. Ступенчатая тарификация (`steps`)
- **Тип**: `ENTITY_TYPE = 'multi'`
- **Описание**: Разные цены в зависимости от объема (ступени)
- **Использование**: `STEP1`, `STEP2`, `STEP3`, `STEP4` определяют границы ступеней
- **Пример**: 
  - Ступень 1: `STEP1 = 0-10`, `MONEY = 1.0`
  - Ступень 2: `STEP1 = 10-50`, `MONEY = 0.8`
  - Ступень 3: `STEP1 > 50`, `MONEY = 0.6`

## Зоны тарификации (BM_ZONE)

Тарифные зоны определяют фиксированные цены в зависимости от зоны:

- **PH_ZONE_GROUP_ID** в `BM_TARIFF` - группа зон телефонии
- **TR_ZONE_GROUP_ID** в `BM_TARIFF` - группа зон трафика
- **ZONE_ID** в `BM_TARIFFEL` - конкретная зона с фиксированной ценой

**Пример**: Один тарифный элемент может иметь разные цены для разных зон:
- Зона "Местная": `ZONE_ID = 1`, `MONEY = 1.0`
- Зона "Междугородная": `ZONE_ID = 2`, `MONEY = 2.0`
- Зона "Международная": `ZONE_ID = 3`, `MONEY = 5.0`

## Счетчики и коэффициенты скидки

### Механизм работы счетчиков

1. **Счетчик связан с сервисом** через `BM_SRV_CNT_LINK`:
   - `SERVICE_ID` → услуга
   - `COUNTER_ID` → счетчик

2. **Счетчик отслеживает потребление** (`BM_COUNTER.VALUE`):
   - Накопленный объем трафика, времени и т.д.

3. **Логика активации коэффициента скидки** зависит от `FLAG_VALUE` в `BM_TARIFFEL`:
   
   **FLAG_VALUE = 0** (для SBD, TYPE_ID=10, 14 - Inet, SSG):
   - Коэффициент применяется **при превышении порога**: `VALUE >= THRESHOLD`
   - Чем больше потребление, тем больше скидка
   - Пример: `VALUE=150`, `THRESHOLD=100` → скидка активирована (150 >= 100)
   
   **FLAG_VALUE = 1** (для IP телефонии и телефонии):
   - Коэффициент применяется **до наступления порога**: `VALUE < THRESHOLD`
   - Скидка действует пока не достигнут порог
   - Пример: `VALUE=50`, `THRESHOLD=100` → скидка активирована (50 < 100)

4. **При достижении порога** (в зависимости от `FLAG_VALUE`):
   - Активируется коэффициент скидки из `BM_TARIFFEL`
   - Коэффициент определяется через `COUNTER_CLASS_ID`
   - `MNEMONIC = 'counter_cf'` в `BM_TARIFFEL_TYPE`

5. **Применение скидки**:
   - Коэффициент применяется к основной цене за зону (`BM_TARIFFEL.MONEY` с `ZONE_ID`)
   - Формула: `Итоговая цена = Базовая цена × Коэффициент скидки`

### Примеры работы счетчика

#### Пример 1: FLAG_VALUE = 0 (SBD, TYPE_ID=10, 14 - Inet, SSG)
```
Услуга: SUB-12345 (SERVICE_ID = 100, TYPE_ID = 10)
Счетчик: COUNTER_ID = 200
  - Текущее значение: VALUE = 150
  - Порог: THRESHOLD = 100
  
Тарифный элемент:
  - ZONE_ID = 1 (Местная зона)
  - MONEY = 10.0 (базовая цена)
  - COUNTER_CLASS_ID = 5 (класс счетчика)
  - MNEMONIC = 'counter_cf'
  - FLAG_VALUE = 0
  - MONEY = 0.9 (коэффициент скидки 10%)

Результат:
  - FLAG_VALUE = 0 → проверка: VALUE (150) >= THRESHOLD (100) → TRUE
  - Скидка активирована
  - Итоговая цена = 10.0 × 0.9 = 9.0 руб
```

#### Пример 2: FLAG_VALUE = 1 (IP телефония, телефония)
```
Услуга: SERVICE_ID = 200 (TYPE_ID = 1 - телефония)
Счетчик: COUNTER_ID = 300
  - Текущее значение: VALUE = 50
  - Порог: THRESHOLD = 100
  
Тарифный элемент:
  - ZONE_ID = 2 (Междугородная зона)
  - MONEY = 5.0 (базовая цена)
  - COUNTER_CLASS_ID = 8 (класс счетчика)
  - MNEMONIC = 'counter_cf'
  - FLAG_VALUE = 1
  - MONEY = 0.8 (коэффициент скидки 20%)

Результат:
  - FLAG_VALUE = 1 → проверка: VALUE (50) < THRESHOLD (100) → TRUE
  - Скидка активирована
  - Итоговая цена = 5.0 × 0.8 = 4.0 руб
```

## Примеры запросов

### Получить все статьи тарифа

```sql
SELECT 
    t.TARIFF_ID,
    t.NAME AS TARIFF_NAME,
    tel.TARIFFEL_ID,
    tel.MONEY,
    tel.STEP1, tel.STEP2, tel.STEP3, tel.STEP4,
    tel.ZONE_ID,
    z.DESCRIPTION AS ZONE_DESC,
    ttype.MNEMONIC AS ARTICLE_TYPE,
    ttype.NAME AS ARTICLE_NAME,
    rt.MNEMONIC AS RESOURCE_TYPE
FROM BM_TARIFF t
JOIN BM_TARIFFEL tel ON t.TARIFF_ID = tel.TARIFF_ID
LEFT JOIN BM_TARIFFEL_TYPE ttype ON tel.TARIFFEL_TYPE_ID = ttype.TARIFFEL_TYPE_ID
LEFT JOIN BM_RESOURCE_TYPE rt ON tel.RESOURCE_TYPE_ID = rt.RESOURCE_TYPE_ID
LEFT JOIN BM_ZONE z ON tel.ZONE_ID = z.ZONE_ID
WHERE t.TARIFF_ID = :tariff_id
  AND (tel.DATE_END IS NULL OR tel.DATE_END >= SYSDATE)
ORDER BY ttype.MNEMONIC, tel.ZONE_ID;
```

### Получить счетчики сервиса с коэффициентами скидки (с учетом FLAG_VALUE)

```sql
SELECT 
    s.SERVICE_ID,
    s.LOGIN,
    s.TYPE_ID,
    scl.COUNTER_ID,
    c.VALUE AS COUNTER_VALUE,
    c.THRESHOLD,
    tel.FLAG_VALUE,
    CASE 
        WHEN tel.FLAG_VALUE = 0 AND c.VALUE >= c.THRESHOLD THEN 'Активна (превышение порога)'
        WHEN tel.FLAG_VALUE = 1 AND c.VALUE < c.THRESHOLD THEN 'Активна (до порога)'
        ELSE 'Неактивна'
    END AS DISCOUNT_STATUS,
    tel.MONEY AS DISCOUNT_COEFFICIENT,
    tel.ZONE_ID,
    z.DESCRIPTION AS ZONE_DESC
FROM SERVICES s
JOIN BM_SRV_CNT_LINK scl ON s.SERVICE_ID = scl.SERVICE_ID
JOIN BM_COUNTER c ON scl.COUNTER_ID = c.COUNTER_ID
LEFT JOIN BM_TARIFFEL tel ON c.COUNTER_CLASS_ID = tel.COUNTER_CLASS_ID
    AND tel.TARIFFEL_TYPE_ID IN (
        SELECT TARIFFEL_TYPE_ID 
        FROM BM_TARIFFEL_TYPE 
        WHERE MNEMONIC = 'counter_cf'
    )
LEFT JOIN BM_ZONE z ON tel.ZONE_ID = z.ZONE_ID
WHERE s.SERVICE_ID = :service_id;
```

### Разделить статьи тарифа: абонплата и трафик

```sql
-- Абонентская плата
SELECT 
    t.TARIFF_ID,
    t.NAME AS TARIFF_NAME,
    tel.MONEY AS ABON_PAYMENT
FROM BM_TARIFF t
JOIN BM_TARIFFEL tel ON t.TARIFF_ID = tel.TARIFF_ID
JOIN BM_TARIFFEL_TYPE ttype ON tel.TARIFFEL_TYPE_ID = ttype.TARIFFEL_TYPE_ID
WHERE ttype.MNEMONIC = 'fee'
  AND (tel.DATE_END IS NULL OR tel.DATE_END >= SYSDATE);

-- Плата за трафик
SELECT 
    t.TARIFF_ID,
    t.NAME AS TARIFF_NAME,
    tel.MONEY AS TRAFFIC_PRICE,
    tel.STEP1, tel.STEP2, tel.STEP3, tel.STEP4,
    tel.ZONE_ID,
    z.DESCRIPTION AS ZONE_DESC
FROM BM_TARIFF t
JOIN BM_TARIFFEL tel ON t.TARIFF_ID = tel.TARIFF_ID
JOIN BM_TARIFFEL_TYPE ttype ON tel.TARIFFEL_TYPE_ID = ttype.TARIFFEL_TYPE_ID
LEFT JOIN BM_ZONE z ON tel.ZONE_ID = z.ZONE_ID
WHERE ttype.MNEMONIC IN ('traffic_pay', 'll_traffic_pay')
  AND (tel.DATE_END IS NULL OR tel.DATE_END >= SYSDATE);
```

## Важные моменты

1. **Абонентская плата** (`fee`) - фиксированная ежемесячная плата, обычно один элемент в тарифе
2. **Плата за трафик** (`traffic_pay`) - переменная плата, может быть несколько элементов для разных зон
3. **Счетчики** отслеживают потребление и активируют коэффициенты скидки при достижении порогов
4. **FLAG_VALUE определяет логику применения коэффициента скидки**:
   - `FLAG_VALUE = 0` (SBD, TYPE_ID=10, 14 - Inet, SSG): скидка при `VALUE >= THRESHOLD` (чем больше, тем больше скидка)
   - `FLAG_VALUE = 1` (IP телефония, телефония): скидка при `VALUE < THRESHOLD` (скидка действует пока не достигнут порог)
5. **Зоны** определяют фиксированные цены в зависимости от зоны тарификации
6. **Ступенчатая тарификация** использует `STEP1-STEP4` для определения границ ступеней
7. **Для расчета доходов** нужно суммировать `MONEY` всех тарифных элементов тарифа

