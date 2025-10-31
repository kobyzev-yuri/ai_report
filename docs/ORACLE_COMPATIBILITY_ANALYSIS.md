# Анализ совместимости Streamlit приложения с Oracle

## Текущее состояние

**Текущая СУБД:** PostgreSQL  
**Целевая СУБД:** Oracle  
**Приложение:** streamlit_report.py

---

## 🔍 Анализ совместимости

### 1. Подключение к базе данных

#### PostgreSQL (текущее)
```python
import psycopg2

DB_CONFIG = {
    'dbname': 'billing',
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': 5432
}

conn = psycopg2.connect(**DB_CONFIG)
```

#### Oracle (требуется)
```python
import cx_Oracle

DB_CONFIG = {
    'user': 'billing7',
    'password': 'billing',
    'dsn': 'localhost:1521/bm7'
}

# Или через Easy Connect
conn = cx_Oracle.connect(
    'billing7',
    'billing',
    '192.168.3.35:1521/bm7'
)
```

**Статус:** ❌ Несовместимо - требуется замена драйвера

---

### 2. SQL синтаксис

#### Проблема 1: CAST AS TEXT

**PostgreSQL:**
```sql
CAST(cor.BILL_MONTH % 10000 AS TEXT)
```

**Oracle:**
```sql
TO_CHAR(MOD(cor.BILL_MONTH, 10000))
-- или
CAST(MOD(cor.BILL_MONTH, 10000) AS VARCHAR2)
```

**Статус:** ❌ Несовместимо - требуется замена

#### Проблема 2: LPAD с текстом

**PostgreSQL:**
```sql
LPAD(CAST(value AS TEXT), 4, '0')
```

**Oracle:**
```sql
LPAD(TO_CHAR(value), 4, '0')
```

**Статус:** ⚠️ Требует модификации

#### Проблема 3: Модуло операция

**PostgreSQL:**
```sql
BILL_MONTH % 10000
```

**Oracle:**
```sql
MOD(BILL_MONTH, 10000)
```

**Статус:** ❌ Несовместимо

#### Проблема 4: Целочисленное деление

**PostgreSQL:**
```sql
BILL_MONTH / 10000  -- целочисленное деление
```

**Oracle:**
```sql
TRUNC(BILL_MONTH / 10000)  -- нужно явное округление
```

**Статус:** ⚠️ Работает, но лучше явно использовать TRUNC

---

### 3. Представления и функции

#### V_CONSOLIDATED_OVERAGE_REPORT

**PostgreSQL версия:**
```sql
AND TO_CHAR(se.INVOICE_DATE, 'YYYYMM') = (
    LPAD(CAST(cor.BILL_MONTH % 10000 AS TEXT), 4, '0') || 
    LPAD(CAST(cor.BILL_MONTH / 10000 AS TEXT), 2, '0')
)
```

**Oracle версия:**
```sql
AND TO_CHAR(se.INVOICE_DATE, 'YYYYMM') = (
    LPAD(TO_CHAR(MOD(cor.BILL_MONTH, 10000)), 4, '0') || 
    LPAD(TO_CHAR(TRUNC(cor.BILL_MONTH / 10000)), 2, '0')
)
```

**Статус:** ⚠️ Требуется адаптация представления

#### Функция calculate_overage

**PostgreSQL:**
```sql
CREATE OR REPLACE FUNCTION calculate_overage(
    p_plan_name VARCHAR,
    p_usage_bytes NUMERIC
) RETURNS NUMERIC
```

**Oracle:**
```sql
CREATE OR REPLACE FUNCTION calculate_overage(
    p_plan_name VARCHAR2,
    p_usage_bytes NUMBER
) RETURN NUMBER
```

**Статус:** ✅ Уже создано в `create_tariff_plans.sql`

---

### 4. Типы данных

| PostgreSQL | Oracle | Статус |
|------------|--------|--------|
| VARCHAR | VARCHAR2 | ⚠️ Требует изменения |
| TEXT | VARCHAR2/CLOB | ⚠️ Требует изменения |
| NUMERIC | NUMBER | ⚠️ Требует изменения |
| SERIAL | NUMBER + IDENTITY | ⚠️ Требует изменения |
| TIMESTAMP | DATE/TIMESTAMP | ✅ Совместимо |
| BOOLEAN | CHAR(1) | ⚠️ Требует изменения |

**Статус:** ⚠️ Большинство типов требуют замены в DDL

---

### 5. Pandas read_sql_query

**Совместимость:** ✅ Работает с обоими драйверами

```python
# PostgreSQL
df = pd.read_sql_query(query, psycopg2_connection)

# Oracle
df = pd.read_sql_query(query, cx_Oracle_connection)
```

**Статус:** ✅ Совместимо

---

## 📋 Список изменений для Oracle

### Критические изменения (обязательные)

1. **Замена драйвера:**
   - ❌ `psycopg2` → ✅ `cx_Oracle`

2. **Изменение SQL синтаксиса:**
   - ❌ `CAST(... AS TEXT)` → ✅ `TO_CHAR(...)`
   - ❌ `value % divisor` → ✅ `MOD(value, divisor)`
   - ❌ `value / divisor` → ✅ `TRUNC(value / divisor)`

3. **Параметры подключения:**
   - ❌ PostgreSQL config → ✅ Oracle DSN

### Некритические изменения (желательные)

1. **Оптимизация запросов для Oracle**
2. **Использование специфичных функций Oracle**
3. **Настройка пула соединений**

---

## 🔄 Стратегии миграции

### Вариант 1: Две версии приложения (РЕКОМЕНДУЕТСЯ)

**Плюсы:**
- Простота разработки
- Оптимизация под каждую СУБД
- Нет накладных расходов

**Минусы:**
- Две кодовые базы
- Дублирование изменений

**Реализация:**
```
streamlit_report.py          - PostgreSQL версия (отладка)
streamlit_report_oracle.py   - Oracle версия (продакшн)
```

### Вариант 2: Универсальное приложение

**Плюсы:**
- Единая кодовая база
- Гибкость переключения

**Минусы:**
- Сложность разработки
- Накладные расходы
- Сложнее отлаживать

**Реализация:**
```python
DB_TYPE = 'oracle'  # или 'postgres'

if DB_TYPE == 'oracle':
    import cx_Oracle as db_driver
    DB_CONFIG = {...}
else:
    import psycopg2 as db_driver
    DB_CONFIG = {...}
```

### Вариант 3: SQLAlchemy (универсальный ORM)

**Плюсы:**
- Максимальная абстракция
- Поддержка многих СУБД
- Стандартизация

**Минусы:**
- Дополнительная зависимость
- Overhead
- Ограничения специфичных функций

**Реализация:**
```python
from sqlalchemy import create_engine

# PostgreSQL
engine = create_engine('postgresql://user:pass@host:port/db')

# Oracle
engine = create_engine('oracle+cx_oracle://user:pass@host:port/?service_name=bm7')
```

---

## 🎯 Рекомендуемый подход

### Этап 1: Отладка на PostgreSQL ✅
- Используем текущий `streamlit_report.py`
- Отлаживаем бизнес-логику
- Тестируем отчеты
- Проверяем расчеты

### Этап 2: Создание Oracle версии
- Создаем `streamlit_report_oracle.py`
- Меняем драйвер на `cx_Oracle`
- Адаптируем SQL запросы
- Используем Oracle представления из `create_tariff_plans.sql`

### Этап 3: Развертывание
- PostgreSQL версия: локальная отладка
- Oracle версия: продакшн сервер

---

## 📝 Конкретные изменения в SQL

### Запрос 1: get_main_report()

**PostgreSQL (текущий):**
```sql
AND TO_CHAR(se.INVOICE_DATE, 'YYYYMM') = (
    LPAD(CAST(cor.BILL_MONTH % 10000 AS TEXT), 4, '0') || 
    LPAD(CAST(cor.BILL_MONTH / 10000 AS TEXT), 2, '0')
)
```

**Oracle (требуется):**
```sql
AND TO_CHAR(se.INVOICE_DATE, 'YYYYMM') = (
    LPAD(TO_CHAR(MOD(cor.BILL_MONTH, 10000)), 4, '0') || 
    LPAD(TO_CHAR(TRUNC(cor.BILL_MONTH / 10000)), 2, '0')
)
```

### Запрос 2: get_statistics()

**Совместимость:** ✅ Полностью совместим (стандартный SQL)

### Запрос 3: get_tariff_plans()

**Совместимость:** ✅ Полностью совместим

### Запрос 4: get_periods()

**Совместимость:** ✅ Полностью совместим

---

## 🛠️ Дополнительные требования

### Для PostgreSQL (текущее)
```bash
pip install psycopg2-binary streamlit pandas openpyxl
```

### Для Oracle (требуется)
```bash
# 1. Установить Oracle Instant Client
# https://www.oracle.com/database/technologies/instant-client.html

# 2. Установить пакеты Python
pip install cx_Oracle streamlit pandas openpyxl

# 3. Настроить переменные окружения
export LD_LIBRARY_PATH=/path/to/instantclient:$LD_LIBRARY_PATH
```

---

## 📊 Оценка трудозатрат

| Задача | Время | Сложность |
|--------|-------|-----------|
| Установка cx_Oracle | 30 мин | Средняя |
| Изменение подключения | 15 мин | Легкая |
| Адаптация SQL запросов | 1-2 часа | Средняя |
| Тестирование | 2-3 часа | Средняя |
| Документация | 1 час | Легкая |
| **ИТОГО** | **5-7 часов** | **Средняя** |

---

## ✅ Чек-лист миграции

### Подготовка
- [ ] Установлен Oracle Instant Client
- [ ] Установлен cx_Oracle
- [ ] Доступна Oracle база billing7/billing@bm7
- [ ] Созданы таблицы (create_tariff_plans.sql)
- [ ] Загружены данные (load_spnet_traffic.py, load_steccom_expenses.py)

### Разработка
- [ ] Создан streamlit_report_oracle.py
- [ ] Изменено подключение на cx_Oracle
- [ ] Адаптированы SQL запросы (CAST AS TEXT → TO_CHAR)
- [ ] Адаптированы операции (% → MOD, / → TRUNC)
- [ ] Проверены все представления

### Тестирование
- [ ] Подключение к Oracle работает
- [ ] Данные загружаются корректно
- [ ] Фильтры работают
- [ ] Расчеты верны (сверка с PostgreSQL)
- [ ] Экспорт CSV/Excel работает

### Развертывание
- [ ] Приложение запущено на сервере
- [ ] Доступ по URL настроен
- [ ] Документация обновлена
- [ ] Пользователи обучены

---

## 🔗 Связанные файлы

| Файл | Назначение | Статус Oracle |
|------|-----------|---------------|
| `streamlit_report.py` | PostgreSQL версия | ❌ Не совместим |
| `setup_postgres.sql` | PostgreSQL DDL | ❌ Не совместим |
| `create_tariff_plans.sql` | Oracle DDL | ✅ Готов |
| `oracle_tables.sql` | Oracle таблицы | ✅ Готов |
| `migrate_imei_to_varchar.sql` | Oracle миграция | ✅ Готов |
| `calculate_overage.py` | Python модуль | ✅ Совместим |
| `load_spnet_traffic.py` | Загрузчик SPNet | ⚠️ Нужна адаптация |
| `load_steccom_expenses.py` | Загрузчик STECCOM | ⚠️ Нужна адаптация |

---

## 🚀 Следующие шаги

1. **Создать Oracle версию Streamlit приложения**
   - Файл: `streamlit_report_oracle.py`
   - Драйвер: cx_Oracle
   - SQL: адаптированный синтаксис

2. **Протестировать на Oracle**
   - Подключение
   - Загрузка данных
   - Расчеты

3. **Задокументировать различия**
   - Инструкция по развертыванию
   - Особенности Oracle версии

---

## 💡 Выводы

### Критические проблемы совместимости:
1. ❌ Драйвер (psycopg2 vs cx_Oracle)
2. ❌ SQL синтаксис (CAST AS TEXT, %, /)
3. ⚠️ Параметры подключения

### Рекомендация:
✅ **Создать отдельную Oracle версию приложения**
- Проще разработка
- Оптимизация под каждую СУБД
- PostgreSQL для отладки, Oracle для продакшн

### Оценка:
- Трудозатраты: 5-7 часов
- Сложность: Средняя
- Риски: Низкие (SQL синтаксис хорошо документирован)


