# Конфигурация для Oracle billing schema

## Обзор

RAG система настроена для работы с **Oracle billing schema (STECCOM)**, используя Qdrant для векторного поиска и Oracle для выполнения SQL запросов.

## Архитектура

```
┌─────────────────────────────────────────┐
│         Финансист (вопрос)              │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│      RAG Assistant (Streamlit)          │
│  ┌──────────────────────────────────┐   │
│  │  1. Поиск в Qdrant (KB)          │   │
│  │  2. Генерация SQL                │   │
│  │  3. Выполнение в Oracle          │   │
│  └──────────────────────────────────┘   │
└──────────────┬──────────────────────────┘
               │
       ┌───────┴────────┐
       │                │
       ▼                ▼
┌─────────────┐  ┌──────────────┐
│   Qdrant    │  │   Oracle     │
│ (векторная) │  │  (billing)   │
│             │  │              │
│ KB: DDL,    │  │ SPNET_TRAFFIC│
│ документация│  │ STECCOM_...  │
│ Q/A примеры │  │ SERVICES     │
└─────────────┘  │ ANALYTICS    │
                 │ BM_INVOICE   │
                 └──────────────┘
```

## Компоненты

### 1. Qdrant (векторная БД)
- **Назначение:** Хранение KB и семантический поиск
- **Данные:** DDL, документация, Q/A примеры
- **Независим от Oracle:** Может работать с любой БД

### 2. Oracle Database (billing schema)
- **Назначение:** Выполнение SQL запросов
- **Схема:** `billing` (STECCOM)
- **Таблицы:** SPNET_TRAFFIC, STECCOM_EXPENSES, SERVICES, ANALYTICS, BM_INVOICE и др.

## Конфигурация

### Переменные окружения

```bash
# ========== Oracle Database (billing schema) ==========
ORACLE_USER=billing7
ORACLE_PASSWORD=billing
ORACLE_HOST=192.168.3.35
ORACLE_PORT=1521
ORACLE_SID=bm7
ORACLE_SERVICE=bm7
ORACLE_SCHEMA=billing

# ========== Qdrant (векторная БД) ==========
QDRANT_HOST=localhost
QDRANT_PORT=6333
QDRANT_COLLECTION=kb_billing

# ========== Модель эмбеддингов ==========
EMBEDDING_MODEL=intfloat/multilingual-e5-base
```

### Использование в коде

```python
from kb_billing.rag.config_sql4a import SQL4AConfig

# Oracle настройки
print(f"Oracle Schema: {SQL4AConfig.ORACLE_SCHEMA}")
print(f"Oracle Host: {SQL4AConfig.ORACLE_HOST}:{SQL4AConfig.ORACLE_PORT}")
print(f"DSN: {SQL4AConfig.get_oracle_dsn()}")
print(f"Connection: {SQL4AConfig.get_oracle_connection_string()}")

# Qdrant настройки
print(f"Qdrant: {SQL4AConfig.QDRANT_HOST}:{SQL4AConfig.QDRANT_PORT}")
print(f"Collection: {SQL4AConfig.QDRANT_COLLECTION}")
```

## Схема billing (STECCOM)

### Основные таблицы

#### Таблицы затрат Иридиум:
- `SPNET_TRAFFIC` - данные трафика SPNet
- `STECCOM_EXPENSES` - расходы STECCOM
- `TARIFF_PLANS` - тарифы Иридиум

#### Таблицы доходов/биллинг:
- `SERVICES` - услуги биллинга
- `ANALYTICS` - аналитика и начисления
- `BM_INVOICE` - счета-фактуры
- `BM_INVOICE_ITEM` - позиции счетов-фактур
- `BM_SERVICE_MONEY` - сессии услуг
- `BM_CDR_ACCT` - трафик из файлов
- И другие таблицы биллинга

### Представления (VIEW):
- `V_SPNET_OVERAGE_ANALYSIS` - анализ превышения трафика
- `V_CONSOLIDATED_OVERAGE_REPORT` - консолидированный отчет
- `V_IRIDIUM_SERVICES_INFO` - информация о сервисах
- `V_CONSOLIDATED_REPORT_WITH_BILLING` - расширенный отчет

## Выполнение SQL запросов

RAG система генерирует SQL запросы для Oracle и выполняет их:

```python
from kb_billing.rag.rag_assistant import RAGAssistant
import cx_Oracle

assistant = RAGAssistant()

# Генерация SQL на основе вопроса
question = "Покажи превышение трафика за октябрь 2025"
context = assistant.get_context_for_sql_generation(question)

# SQL запрос для Oracle
sql = "SELECT ... FROM billing.v_consolidated_report_with_billing ..."

# Выполнение в Oracle
conn = cx_Oracle.connect(
    SQL4AConfig.ORACLE_USER,
    SQL4AConfig.ORACLE_PASSWORD,
    SQL4AConfig.get_oracle_dsn()
)
cursor = conn.cursor()
cursor.execute(sql)
results = cursor.fetchall()
```

## Преимущества архитектуры

1. **Разделение ответственности:**
   - Qdrant для семантического поиска (быстро, оптимизировано)
   - Oracle для выполнения SQL (надежно, транзакции)

2. **Независимость:**
   - Qdrant не зависит от Oracle
   - Можно использовать с любой БД

3. **Масштабируемость:**
   - Qdrant легко масштабируется
   - Oracle остается основной БД для данных

## Миграция данных

Если нужно мигрировать KB из другой системы:

1. Экспорт данных из источника
2. Загрузка в Qdrant через `KBLoader`
3. Использование тех же Oracle настроек для выполнения SQL

## Troubleshooting

### Проблемы с Oracle подключением

```python
# Проверка настроек
from kb_billing.rag.config_sql4a import SQL4AConfig
print(SQL4AConfig.get_config_summary())

# Тест подключения
import cx_Oracle
try:
    conn = cx_Oracle.connect(
        SQL4AConfig.ORACLE_USER,
        SQL4AConfig.ORACLE_PASSWORD,
        SQL4AConfig.get_oracle_dsn()
    )
    print("✅ Подключение успешно")
    conn.close()
except Exception as e:
    print(f"❌ Ошибка: {e}")
```

### Проблемы с Qdrant

```python
from qdrant_client import QdrantClient
from kb_billing.rag.config_sql4a import SQL4AConfig

client = QdrantClient(
    host=SQL4AConfig.QDRANT_HOST,
    port=SQL4AConfig.QDRANT_PORT
)

# Проверка коллекции
try:
    collection = client.get_collection(SQL4AConfig.QDRANT_COLLECTION)
    print(f"✅ Коллекция найдена: {collection.points_count} точек")
except Exception as e:
    print(f"❌ Ошибка: {e}")
```

## Дополнительная информация

- Полная документация: `kb_billing/rag/README.md`
- Конфигурация sql4A: `kb_billing/rag/SQL4A_CONFIG.md`
- Быстрый старт: `kb_billing/rag/QUICK_START.md`












