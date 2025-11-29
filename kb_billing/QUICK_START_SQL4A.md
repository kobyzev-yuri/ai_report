# Быстрый старт: интеграция kb_billing с sql4A

## Краткое резюме

Проект `sql4A` уже реализовал проверенные подходы для работы с векторными БД и RAG. Рекомендуется использовать эти подходы для `kb_billing`.

## Ключевые компоненты из sql4A

### 1. pgvector + PostgreSQL
- ✅ Уже работает в sql4A
- ✅ Гибридный поиск (векторный + SQL фильтры)
- ✅ Структура: `vanna_vectors` с `content_type`, `metadata JSONB`, `embedding vector`

### 2. KBTrainingClient
- ✅ Унифицированный клиент для загрузки данных
- ✅ API эндпоинты: `/training/example`, `/training/ddl`, `/training/documentation`
- ✅ Поддержка массовой загрузки из JSON

### 3. Генерация эмбеддингов
- ✅ Скрипт `generate_embeddings_hf.py`
- ✅ Поддержка мультиязычных моделей (русский язык)

### 4. RAG Pipeline
- ✅ Семантический поиск
- ✅ Генерация SQL с контекстом
- ✅ Поддержка оптимизированных SQL

---

## Рекомендация: использовать pgvector (как в sql4A)

### Почему:
1. **Переиспользование кода** - можно адаптировать существующие компоненты
2. **Проверенные решения** - уже работает в production
3. **Единообразие** - один подход для всех KB проектов
4. **Знакомый стек** - PostgreSQL уже используется

### Альтернатива: Qdrant
- Если нужна максимальная производительность векторного поиска
- Если нет возможности использовать PostgreSQL

---

## План действий

### Шаг 1: Настройка инфраструктуры

```bash
# 1. Установить PostgreSQL с pgvector (если еще нет)
# См. sql4A/docs/POSTGRESQL_INSTALLATION.md

# 2. Создать таблицу для kb_billing
psql $DATABASE_URL << EOF
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE kb_billing_vectors (
    id SERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    content_type VARCHAR(50) NOT NULL,
    metadata JSONB NOT NULL,
    embedding vector(768),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX kb_billing_vectors_embedding_ivf 
ON kb_billing_vectors USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);

CREATE INDEX kb_billing_vectors_type_idx 
ON kb_billing_vectors (content_type);

CREATE INDEX kb_billing_vectors_metadata_idx 
ON kb_billing_vectors USING GIN (metadata);
EOF
```

### Шаг 2: Адаптация клиента

Создать `kb_billing_training_client.py` на основе `sql4A/src/tools/kb_training_client.py`:

```python
# Адаптировать методы для kb_billing метаданных
# Добавить поддержку category, complexity, business_entity
# См. INTEGRATION_WITH_SQL4A.md для деталей
```

### Шаг 3: Загрузка данных

```python
from kb_billing_training_client import KBBillingTrainingClient

client = KBBillingTrainingClient(api_base_url="http://localhost:8000")

# Загрузка всей kb_billing
kb_dir = Path("/mnt/ai/cnn/ai_report/kb_billing")
stats = client.load_from_kb_billing_directory(kb_dir)
```

### Шаг 4: Генерация эмбеддингов

```bash
# Использовать скрипт из sql4A
python -m src.tools.generate_embeddings_hf \
    --dsn "$DATABASE_URL" \
    --model "intfloat/multilingual-e5-base" \
    --table "kb_billing_vectors" \
    --content-column "content" \
    --embedding-column "embedding"
```

### Шаг 5: Тестирование

```python
# Тест поиска
results = await search_kb_billing(
    query_text="превышение трафика октябрь",
    query_embedding=model.encode("превышение трафика октябрь"),
    content_type="qa_example",
    category="Превышение трафика"
)
```

---

## Структура метаданных для kb_billing

### Расширенные метаданные (по сравнению с sql4A):

```json
{
    "type": "qa_example",
    "category": "Превышение трафика",      // Новое
    "complexity": 2,                        // Новое
    "business_entity": "overage",           // Новое
    "table_names": ["V_CONSOLIDATED_REPORT_WITH_BILLING"],  // Новое
    "context": "Анализ превышения трафика за период",      // Новое
    "question": "...",
    "sql": "...",
    "domain": "billing"
}
```

---

## Преимущества использования sql4A подходов

1. ✅ **Переиспользование кода** - меньше разработки
2. ✅ **Проверенные решения** - уже работают
3. ✅ **Единообразие** - один подход для всех KB
4. ✅ **Поддержка** - есть документация
5. ✅ **Расширяемость** - легко добавить новые типы

---

## Следующие шаги

1. Изучить `sql4A/src/vanna/vanna_pgvector_native.py`
2. Изучить `sql4A/src/tools/kb_training_client.py`
3. Создать адаптированный клиент для kb_billing
4. Настроить PostgreSQL с pgvector
5. Загрузить данные
6. Протестировать поиск

Подробности: см. `INTEGRATION_WITH_SQL4A.md`




