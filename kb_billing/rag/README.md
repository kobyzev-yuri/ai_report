# RAG система для KB_billing

RAG (Retrieval-Augmented Generation) система для генерации SQL запросов и поиска информации по Oracle billing схеме STECCOM.

## Структура

```
kb_billing/rag/
├── __init__.py              # Инициализация пакета
├── kb_loader.py             # Загрузка KB в Qdrant
├── rag_assistant.py         # RAG ассистент для поиска и генерации
├── streamlit_assistant.py   # Интеграция с Streamlit
├── init_kb.py              # Скрипт инициализации KB
└── README.md               # Документация
```

## Установка

### 1. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 2. Запуск Qdrant

```bash
# Docker вариант (рекомендуется)
docker run -d \
  --name qdrant \
  -p 6333:6333 \
  -v $(pwd)/qdrant_storage:/qdrant/storage \
  qdrant/qdrant

# Проверка работы
curl http://localhost:6333/health
```

### 3. Инициализация KB

```bash
# Загрузка всех данных KB в Qdrant
python kb_billing/rag/init_kb.py

# С опциями
python kb_billing/rag/init_kb.py \
  --host localhost \
  --port 6333 \
  --collection kb_billing \
  --model intfloat/multilingual-e5-base \
  --recreate
```

## Использование

### В Python коде

```python
from kb_billing.rag.rag_assistant import RAGAssistant

# Инициализация
assistant = RAGAssistant()

# Поиск похожих примеров
question = "Покажи превышение трафика за октябрь 2025"
examples = assistant.search_similar_examples(question, limit=5)

for example in examples:
    print(f"Вопрос: {example['question']}")
    print(f"SQL: {example['sql']}")
    print(f"Similarity: {example['similarity']:.3f}")

# Получение контекста для генерации SQL
context = assistant.get_context_for_sql_generation(question)
formatted = assistant.format_context_for_llm(context)
print(formatted)
```

### В Streamlit интерфейсе

Ассистент автоматически доступен в первой закладке веб-интерфейса после запуска:

```bash
./run_streamlit_background.sh
```

## Компоненты

### KBLoader

Класс для загрузки KB_billing в Qdrant:

- Загрузка Q/A примеров из `training_data/sql_examples.json`
- Загрузка документации таблиц из `tables/*.json`
- Загрузка документации представлений из `views/*.json`
- Загрузка метаданных из `metadata.json`

### RAGAssistant

RAG ассистент для работы с KB:

- `search_similar_examples()` - поиск похожих Q/A примеров
- `search_table_info()` - поиск информации о таблице
- `search_semantic()` - семантический поиск по KB
- `get_context_for_sql_generation()` - получение контекста для генерации SQL

## Модели эмбеддингов

По умолчанию используется `intfloat/multilingual-e5-base` (768 размерность).

Альтернативы:
- `intfloat/multilingual-e5-large` (1024 размерность) - лучшее качество
- `cointegrated/rubert-tiny2` (312 размерность) - легковесный вариант

## Конфигурация

Параметры Qdrant можно задать через переменные окружения или в Streamlit secrets:

```python
# В Streamlit secrets.toml или через переменные окружения
QDRANT_HOST=localhost
QDRANT_PORT=6333
```

## Структура данных в Qdrant

### Типы данных:

1. **qa_example** - Q/A примеры
   - `question` - вопрос на русском
   - `sql` - SQL запрос
   - `category` - категория
   - `complexity` - сложность (1-4)

2. **ddl** - DDL описания таблиц
   - `table_name` - имя таблицы
   - `content` - DDL запрос

3. **documentation** - документация таблиц
   - `table_name` - имя таблицы
   - `content` - текст документации
   - `key_columns` - ключевые колонки
   - `business_rules` - бизнес-правила
   - `relationships` - связи с другими таблицами

4. **view** - документация представлений
   - `view_name` - имя представления
   - `content` - текст документации

5. **metadata** - метаданные схемы
   - `database` - имя БД
   - `schema` - схема
   - `business_domains` - бизнес-домены

## Примеры использования

### Поиск информации о таблице

```python
assistant = RAGAssistant()
info = assistant.search_table_info("SPNET_TRAFFIC", info_type="both")
print(info["ddl"])
print(info["documentation"])
```

### Семантический поиск

```python
results = assistant.search_semantic(
    "превышение трафика",
    content_type="qa_example",
    limit=5
)
```

### Генерация контекста для LLM

```python
context = assistant.get_context_for_sql_generation(
    "Покажи себестоимость по всем SUB-",
    max_examples=5,
    include_tables=True
)

formatted = assistant.format_context_for_llm(context)
# Передать formatted в LLM для генерации SQL
```

## Troubleshooting

### Qdrant не запущен

```bash
# Проверка
curl http://localhost:6333/health

# Запуск
docker run -d -p 6333:6333 qdrant/qdrant
```

### Коллекция не найдена

```bash
# Пересоздать коллекцию
python kb_billing/rag/init_kb.py --recreate
```

### Ошибки импорта

```bash
# Установить зависимости
pip install qdrant-client sentence-transformers torch transformers
```

## Разработка

### Добавление новых примеров

Добавьте новые Q/A примеры в `kb_billing/training_data/sql_examples.json` и перезагрузите KB:

```bash
python kb_billing/rag/init_kb.py --recreate
```

### Изменение модели эмбеддингов

Измените параметр `embedding_model` в `KBLoader` и `RAGAssistant`:

```python
loader = KBLoader(embedding_model="intfloat/multilingual-e5-large")
```

## Лицензия

Часть проекта Iridium M2M Reporting System.

