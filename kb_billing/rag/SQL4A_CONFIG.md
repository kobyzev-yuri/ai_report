# Конфигурация на основе sql4A

## Обзор

RAG система для KB_billing настроена на использование тех же параметров и моделей, что и проект sql4A, для обеспечения совместимости и единообразия.

## Ключевые настройки из sql4A

### Модель эмбеддингов

**По умолчанию:** `intfloat/multilingual-e5-base`
- Размерность: **768**
- Отличная поддержка русского языка
- Используется в sql4A для работы с русским языком

**Альтернативы:**
- `intfloat/multilingual-e5-large` (1024 размерность) - лучшее качество
- `cointegrated/rubert-tiny2` (312 размерность) - легковесный вариант

### Настройки генерации эмбеддингов

- **Нормализация:** `normalize_embeddings=True` (как в sql4A)
- **Batch size:** 32 (по умолчанию)
- **Размерность векторов:** 768 (для multilingual-e5-base)

### Векторная БД

**Qdrant** (в нашем случае):
- Host: `localhost` (по умолчанию)
- Port: `6333` (по умолчанию)
- Collection: `kb_billing`

**Oracle Database** (основная БД для выполнения SQL):
- Схема: `billing` (STECCOM billing schema)
- Таблицы: SPNET_TRAFFIC, STECCOM_EXPENSES, SERVICES, ANALYTICS, BM_INVOICE и др.
- Используется для выполнения SQL запросов, сгенерированных RAG системой

### Настройки поиска

- **Default limit:** 5 результатов
- **Similarity threshold:** 0.7
- **Гибридный поиск:** семантический + фильтры по метаданным

## Конфигурация через переменные окружения

Создайте или обновите `config.env`:

```bash
# ========== RAG и векторная БД (совместимо с sql4A) ==========

# Модель эмбеддингов (как в sql4A)
EMBEDDING_MODEL=intfloat/multilingual-e5-base

# Qdrant настройки
QDRANT_HOST=localhost
QDRANT_PORT=6333
QDRANT_COLLECTION=kb_billing

# Oracle Schema для billing (используется для выполнения SQL запросов)
# Схема billing содержит таблицы STECCOM: SPNET_TRAFFIC, STECCOM_EXPENSES, SERVICES, ANALYTICS и др.
ORACLE_SCHEMA=billing

# Настройки поиска
DEFAULT_SEARCH_LIMIT=5
SIMILARITY_THRESHOLD=0.7
EMBEDDING_BATCH_SIZE=32

# Настройки генерации SQL
MAX_CONTEXT_EXAMPLES=5
MAX_CONTEXT_TOKENS=2000

# Логирование
LOG_LEVEL=INFO
```

## Использование конфигурации

### В Python коде

```python
from kb_billing.rag.config_sql4a import SQL4AConfig

# Автоматическая загрузка из config.env
print(f"Модель: {SQL4AConfig.EMBEDDING_MODEL}")
print(f"Размерность: {SQL4AConfig.EMBEDDING_DIMENSION}")
print(f"Qdrant: {SQL4AConfig.QDRANT_HOST}:{SQL4AConfig.QDRANT_PORT}")

# Использование в RAG ассистенте
from kb_billing.rag.rag_assistant import RAGAssistant

# Автоматически использует настройки из SQL4AConfig
assistant = RAGAssistant()
```

### В скриптах

```python
from kb_billing.rag.kb_loader import KBLoader

# Автоматически использует настройки из SQL4AConfig
loader = KBLoader()
loader.load_all()
```

## Совместимость с sql4A

### Структура данных

Наша структура данных совместима с sql4A:

```python
# sql4A использует:
content_type: 'ddl' | 'documentation' | 'question_sql'
metadata: JSONB
embedding: vector(768)

# Мы используем:
content_type: 'ddl' | 'documentation' | 'qa_example' | 'metadata'
metadata: JSONB (в payload Qdrant)
embedding: vector(768)
```

### Формат метаданных

Совместим с sql4A форматом:

```json
{
    "type": "qa_example",
    "category": "Превышение трафика",
    "complexity": 2,
    "business_entity": "overage",
    "table_names": ["V_CONSOLIDATED_REPORT_WITH_BILLING"]
}
```

## Архитектура для Oracle billing

Система использует гибридный подход:

1. **Qdrant** - векторная БД для хранения KB и семантического поиска
2. **Oracle Database** - основная БД для выполнения SQL запросов по схеме billing
3. **RAG система** - генерирует SQL запросы на основе KB и выполняет их в Oracle

### Схема billing (STECCOM)

Основные таблицы Oracle billing schema:
- `SPNET_TRAFFIC` - данные трафика SPNet
- `STECCOM_EXPENSES` - расходы STECCOM
- `SERVICES` - услуги биллинга
- `ANALYTICS` - аналитика и начисления
- `BM_INVOICE`, `BM_INVOICE_ITEM` - счета-фактуры
- И другие таблицы биллинга

## Проверка конфигурации

```python
from kb_billing.rag.config_sql4a import SQL4AConfig

# Получить сводку конфигурации
config = SQL4AConfig.get_config_summary()
print(config)
```

Вывод:
```python
{
    "embedding_model": "intfloat/multilingual-e5-base",
    "embedding_dimension": 768,
    "qdrant": {
        "host": "localhost",
        "port": 6333,
        "collection": "kb_billing"
    },
    "oracle": {
        "host": "192.168.3.35",
        "port": 1521,
        "schema": "billing",
        "user": "billing7",
        "sid": "bm7"
    },
    "search": {
        "default_limit": 5,
        "similarity_threshold": 0.7
    }
}
```

## Обновление конфигурации

Конфигурация автоматически загружается из:
1. Переменных окружения
2. Файла `config.env` в корне проекта
3. Значений по умолчанию (совместимых с sql4A)

Для изменения настроек обновите `config.env` или установите переменные окружения.

