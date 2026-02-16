# Интеграция kb_billing с подходами sql4A

## Обзор

Этот документ описывает, как применить проверенные подходы из проекта `sql4A` к базе знаний `kb_billing` для создания интеллектуального интерфейса к финансовым отчетам.

## Архитектура sql4A

### Ключевые компоненты:

1. **pgvector + PostgreSQL** - векторная БД
2. **Vanna AI** - фреймворк для NL→SQL генерации
3. **Унифицированный клиент** - `KBTrainingClient` для загрузки данных
4. **API эндпоинты** - `/training/example`, `/training/ddl`, `/training/documentation`
5. **Гибридный поиск** - семантический + фильтры по метаданным

### Структура данных в sql4A:

```sql
CREATE TABLE vanna_vectors (
    id SERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    content_type VARCHAR(50) NOT NULL,  -- 'ddl' | 'documentation' | 'question_sql'
    metadata JSONB,                      -- Гибкие метаданные
    embedding vector(768),              -- Эмбеддинги (768 для multilingual-e5-base)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX vanna_vectors_embedding_ivf 
ON vanna_vectors USING ivfflat (embedding vector_cosine_ops);

CREATE INDEX vanna_vectors_type_idx 
ON vanna_vectors (content_type);
```

---

## Адаптация для kb_billing

### 1. Структура таблицы для kb_billing

Рекомендуемая структура с расширенными метаданными для финансовых отчетов:

```sql
CREATE TABLE kb_billing_vectors (
    id SERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    content_type VARCHAR(50) NOT NULL,  -- 'ddl' | 'documentation' | 'qa_example' | 'metadata'
    metadata JSONB NOT NULL,            -- Расширенные метаданные
    embedding vector(768),             -- Эмбеддинги для русского языка
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для семантического поиска
CREATE INDEX kb_billing_vectors_embedding_ivf 
ON kb_billing_vectors USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);

-- Индексы для фильтрации
CREATE INDEX kb_billing_vectors_type_idx 
ON kb_billing_vectors (content_type);

CREATE INDEX kb_billing_vectors_metadata_idx 
ON kb_billing_vectors USING GIN (metadata);

-- Индекс для поиска по таблицам
CREATE INDEX kb_billing_vectors_table_name_idx 
ON kb_billing_vectors ((metadata->>'table_name'));
```

### 2. Структура метаданных (metadata JSONB)

#### Для DDL:
```json
{
    "type": "ddl",
    "table_name": "SPNET_TRAFFIC",
    "schema": "billing",
    "database": "Oracle"
}
```

#### Для документации:
```json
{
    "type": "documentation",
    "table_name": "SPNET_TRAFFIC",
    "category": "Таблицы затрат",
    "business_domain": "Iridium M2M сервисы",
    "key_columns": ["CONTRACT_ID", "IMEI", "BILL_MONTH"]
}
```

#### Для Q/A примеров:
```json
{
    "type": "qa_example",
    "question": "Покажи превышение трафика за октябрь 2025",
    "sql": "SELECT ... FROM v_consolidated_report_with_billing ...",
    "category": "Превышение трафика",
    "complexity": 2,
    "business_entity": "overage",
    "table_names": ["V_CONSOLIDATED_REPORT_WITH_BILLING"],
    "context": "Анализ превышения трафика за период",
    "verified": true,
    "domain": "billing"
}
```

#### Для метаданных (связи, правила):
```json
{
    "type": "metadata",
    "category": "business_rules",
    "rule": "Периоды НЕ суммируются - каждая строка = отдельный период",
    "applies_to": ["V_CONSOLIDATED_OVERAGE_REPORT", "V_CONSOLIDATED_REPORT_WITH_BILLING"]
}
```

---

## Адаптация KBTrainingClient для kb_billing

### Создание специализированного клиента

```python
#!/usr/bin/env python3
"""
Специализированный клиент для загрузки kb_billing в векторную БД
Адаптирован из sql4A KBTrainingClient
"""

from typing import Dict, Any, Optional, List
import json
from pathlib import Path
from src.tools.kb_training_client import KBTrainingClient  # Базовый класс из sql4A

class KBBillingTrainingClient(KBTrainingClient):
    """
    Клиент для загрузки kb_billing с поддержкой финансовых метаданных
    """
    
    def add_table_ddl(
        self,
        table_name: str,
        ddl: str,
        schema: str = "billing",
        database: str = "Oracle"
    ) -> Dict[str, Any]:
        """
        Добавление DDL таблицы с метаданными kb_billing
        
        Args:
            table_name: Имя таблицы
            ddl: DDL оператор
            schema: Схема БД
            database: Тип БД
            
        Returns:
            Результат добавления
        """
        metadata = {
            "type": "ddl",
            "table_name": table_name,
            "schema": schema,
            "database": database
        }
        
        # Используем базовый метод или API эндпоинт
        return self._add_ddl(ddl, metadata)
    
    def add_table_documentation(
        self,
        table_info: Dict[str, Any]  # Из JSON файла kb_billing/tables/*.json
    ) -> Dict[str, Any]:
        """
        Добавление документации таблицы из JSON файла kb_billing
        
        Args:
            table_info: Словарь с информацией о таблице из JSON
            
        Returns:
            Результат добавления
        """
        # Формируем текст документации
        doc_parts = [
            f"Таблица: {table_info['table_name']}",
            f"Описание: {table_info['description']}",
            "\nКлючевые поля:"
        ]
        
        for col, desc in table_info.get('key_columns', {}).items():
            doc_parts.append(f"- {col}: {desc}")
        
        if table_info.get('business_rules'):
            doc_parts.append("\nБизнес-правила:")
            for rule in table_info['business_rules']:
                doc_parts.append(f"- {rule}")
        
        content = "\n".join(doc_parts)
        
        metadata = {
            "type": "documentation",
            "table_name": table_info['table_name'],
            "category": table_info.get('category', 'Таблицы'),
            "business_domain": table_info.get('business_domain', 'billing'),
            "key_columns": list(table_info.get('key_columns', {}).keys())
        }
        
        return self._add_documentation(content, metadata)
    
    def add_qa_example(
        self,
        question: str,
        sql: str,
        category: str,
        complexity: int = 1,
        business_entity: str = None,
        context: str = None,
        table_names: List[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Добавление Q/A примера с метаданными kb_billing
        
        Args:
            question: Вопрос на русском языке
            sql: SQL запрос
            category: Категория (из kb_billing)
            complexity: Сложность (1-3)
            business_entity: Бизнес-сущность
            context: Контекст запроса
            table_names: Список используемых таблиц
            
        Returns:
            Результат добавления
        """
        metadata = {
            "type": "qa_example",
            "category": category,
            "complexity": complexity,
            "business_entity": business_entity,
            "context": context,
            "table_names": table_names or []
        }
        
        # Используем базовый метод с расширенными метаданными
        return self.add_training_example(
            question=question,
            sql=sql,
            domain="billing",
            tags=[category, business_entity] if business_entity else [category],
            **kwargs
        )
    
    def load_from_kb_billing_directory(
        self,
        kb_dir: Path,
        verbose: bool = True
    ) -> Dict[str, Any]:
        """
        Загрузка всей kb_billing из директории
        
        Args:
            kb_dir: Путь к директории kb_billing
            verbose: Выводить прогресс
            
        Returns:
            Статистика загрузки
        """
        stats = {
            "tables": {"total": 0, "loaded": 0},
            "views": {"total": 0, "loaded": 0},
            "qa_examples": {"total": 0, "loaded": 0}
        }
        
        # 1. Загрузка DDL и документации таблиц
        tables_dir = kb_dir / "tables"
        if tables_dir.exists():
            for table_file in tables_dir.glob("*.json"):
                try:
                    with open(table_file) as f:
                        table_info = json.load(f)
                    
                    stats["tables"]["total"] += 1
                    
                    # Добавляем DDL
                    if table_info.get('ddl'):
                        self.add_table_ddl(
                            table_name=table_info['table_name'],
                            ddl=table_info['ddl']
                        )
                    
                    # Добавляем документацию
                    self.add_table_documentation(table_info)
                    
                    stats["tables"]["loaded"] += 1
                    
                    if verbose:
                        print(f"✅ Загружена таблица: {table_info['table_name']}")
                        
                except Exception as e:
                    print(f"❌ Ошибка загрузки {table_file}: {e}")
        
        # 2. Загрузка документации представлений (VIEW)
        views_dir = kb_dir / "views"
        if views_dir.exists():
            for view_file in views_dir.glob("*.json"):
                try:
                    with open(view_file) as f:
                        view_info = json.load(f)
                    
                    stats["views"]["total"] += 1
                    
                    # Формируем документацию для VIEW
                    doc_parts = [
                        f"Представление: {view_info['view_name']}",
                        f"Описание: {view_info['description']}"
                    ]
                    
                    if view_info.get('key_columns'):
                        doc_parts.append("\nКлючевые поля:")
                        for col, desc in view_info['key_columns'].items():
                            doc_parts.append(f"- {col}: {desc}")
                    
                    content = "\n".join(doc_parts)
                    
                    metadata = {
                        "type": "documentation",
                        "view_name": view_info['view_name'],
                        "category": "Представления",
                        "source_tables": view_info.get('source_tables', [])
                    }
                    
                    self._add_documentation(content, metadata)
                    stats["views"]["loaded"] += 1
                    
                    if verbose:
                        print(f"✅ Загружено представление: {view_info['view_name']}")
                        
                except Exception as e:
                    print(f"❌ Ошибка загрузки {view_file}: {e}")
        
        # 3. Загрузка Q/A примеров
        training_file = kb_dir / "training_data" / "sql_examples.json"
        if training_file.exists():
            try:
                with open(training_file) as f:
                    examples = json.load(f)
                
                stats["qa_examples"]["total"] = len(examples)
                
                for example in examples:
                    try:
                        self.add_qa_example(
                            question=example['question'],
                            sql=example['sql'],
                            category=example.get('category', 'Общее'),
                            complexity=example.get('complexity', 1),
                            business_entity=example.get('business_entity'),
                            context=example.get('context'),
                            table_names=example.get('table_names', [])
                        )
                        stats["qa_examples"]["loaded"] += 1
                    except Exception as e:
                        print(f"❌ Ошибка загрузки примера: {e}")
                
                if verbose:
                    print(f"✅ Загружено Q/A примеров: {stats['qa_examples']['loaded']}/{stats['qa_examples']['total']}")
                    
            except Exception as e:
                print(f"❌ Ошибка загрузки {training_file}: {e}")
        
        return stats
```

---

## Гибридный поиск для kb_billing

### Адаптация поиска из sql4A

```python
async def search_kb_billing(
    query_text: str,
    query_embedding: List[float],
    content_type: Optional[str] = None,
    category: Optional[str] = None,
    table_name: Optional[str] = None,
    complexity: Optional[int] = None,
    limit: int = 5
) -> List[Dict[str, Any]]:
    """
    Гибридный поиск в kb_billing с фильтрами по метаданным
    
    Адаптировано из sql4A vanna_semantic_fixed.py
    """
    import asyncpg
    from pgvector.asyncpg import register_vector
    
    database_url = os.getenv("DATABASE_URL")
    conn = await asyncpg.connect(database_url)
    await register_vector(conn)
    
    try:
        # Базовый запрос с семантическим поиском
        query = """
            SELECT 
                id,
                content,
                content_type,
                metadata,
                1 - (embedding <=> $1::vector) as similarity
            FROM kb_billing_vectors
            WHERE embedding IS NOT NULL
        """
        
        params = [query_embedding]
        param_idx = 2
        
        # Добавляем фильтры по метаданным
        filters = []
        
        if content_type:
            filters.append(f"content_type = ${param_idx}")
            params.append(content_type)
            param_idx += 1
        
        if category:
            filters.append(f"metadata->>'category' = ${param_idx}")
            params.append(category)
            param_idx += 1
        
        if table_name:
            filters.append(f"(metadata->>'table_name' = ${param_idx} OR metadata->>'view_name' = ${param_idx})")
            params.append(table_name)
            param_idx += 1
        
        if complexity:
            filters.append(f"(metadata->>'complexity')::int <= ${param_idx}")
            params.append(complexity)
            param_idx += 1
        
        if filters:
            query += " AND " + " AND ".join(filters)
        
        # Сортировка по similarity и лимит
        query += f"""
            ORDER BY embedding <=> $1::vector
            LIMIT ${param_idx}
        """
        params.append(limit)
        
        rows = await conn.fetch(query, *params)
        
        results = []
        for row in rows:
            results.append({
                "id": row["id"],
                "content": row["content"],
                "content_type": row["content_type"],
                "metadata": row["metadata"],
                "similarity": float(row["similarity"])
            })
        
        return results
        
    finally:
        await conn.close()
```

---

## Стратегия загрузки kb_billing

### Этап 1: Подготовка данных

```python
from pathlib import Path
from kb_billing_training_client import KBBillingTrainingClient

client = KBBillingTrainingClient(api_base_url="http://localhost:8000")

# Загрузка всей kb_billing
kb_dir = Path("/mnt/ai/cnn/ai_report/kb_billing")
stats = client.load_from_kb_billing_directory(kb_dir, verbose=True)

print(f"""
Статистика загрузки:
- Таблицы: {stats['tables']['loaded']}/{stats['tables']['total']}
- Представления: {stats['views']['loaded']}/{stats['views']['total']}
- Q/A примеры: {stats['qa_examples']['loaded']}/{stats['qa_examples']['total']}
""")
```

### Этап 2: Генерация эмбеддингов

Используем скрипт из sql4A:

```bash
# Адаптированный скрипт для kb_billing
python -m src.tools.generate_embeddings_hf \
    --dsn "$DATABASE_URL" \
    --model "intfloat/multilingual-e5-base" \
    --table "kb_billing_vectors" \
    --content-column "content" \
    --embedding-column "embedding"
```

### Этап 3: Тестирование поиска

```python
# Тест гибридного поиска
results = await search_kb_billing(
    query_text="превышение трафика октябрь",
    query_embedding=model.encode("превышение трафика октябрь"),
    content_type="qa_example",
    category="Превышение трафика",
    limit=5
)

for result in results:
    print(f"Similarity: {result['similarity']:.3f}")
    print(f"Question: {result['metadata']['question']}")
    print(f"SQL: {result['metadata']['sql'][:100]}...")
    print("---")
```

---

## Интеграция с RAG pipeline

### Адаптация из sql4A query_service.py

```python
class BillingQueryService:
    """
    Сервис для генерации SQL запросов по финансовым отчетам
    Адаптирован из sql4A QueryService
    """
    
    def __init__(self):
        self.semantic_vanna = DocStructureVannaNative()  # Из sql4A
        self.llm = OpenAI_Chat()  # Или другой LLM
    
    async def generate_sql(
        self,
        user_question: str,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Генерация SQL запроса с использованием RAG
        
        Args:
            user_question: Вопрос финансиста
            context: Дополнительный контекст (период, фильтры и т.д.)
            
        Returns:
            Dict с SQL, объяснением и метаданными
        """
        # 1. Поиск релевантных примеров и документации
        query_embedding = self.semantic_vanna.generate_embedding(user_question)
        
        # Поиск похожих Q/A примеров
        similar_examples = await search_kb_billing(
            query_text=user_question,
            query_embedding=query_embedding,
            content_type="qa_example",
            limit=3
        )
        
        # Поиск релевантной документации
        relevant_docs = await search_kb_billing(
            query_text=user_question,
            query_embedding=query_embedding,
            content_type="documentation",
            limit=5
        )
        
        # Поиск DDL для используемых таблиц
        table_names = self._extract_table_names(similar_examples)
        ddl_context = []
        for table_name in table_names:
            ddl_results = await search_kb_billing(
                query_text=table_name,
                query_embedding=query_embedding,
                content_type="ddl",
                table_name=table_name,
                limit=1
            )
            if ddl_results:
                ddl_context.append(ddl_results[0]['content'])
        
        # 2. Формирование контекста для LLM
        context_parts = []
        
        # Добавляем DDL
        if ddl_context:
            context_parts.append("Структура таблиц:\n" + "\n\n".join(ddl_context))
        
        # Добавляем документацию
        if relevant_docs:
            context_parts.append("Документация:\n" + "\n\n".join([d['content'] for d in relevant_docs]))
        
        # Добавляем похожие примеры
        if similar_examples:
            context_parts.append("Похожие примеры:\n")
            for ex in similar_examples:
                context_parts.append(f"Q: {ex['metadata']['question']}\nA: {ex['metadata']['sql']}")
        
        full_context = "\n\n".join(context_parts)
        
        # 3. Генерация SQL через LLM
        sql = self.llm.generate_sql(
            question=user_question,
            ddl=full_context
        )
        
        return {
            "sql": sql,
            "context": full_context,
            "similar_examples": similar_examples,
            "relevant_docs": relevant_docs
        }
```

---

## Рекомендации по выбору векторной БД

### Вариант 1: pgvector (как в sql4A) - Рекомендуется

**Почему:**
- ✅ Уже используется в sql4A - можно переиспользовать код
- ✅ Знакомый стек (PostgreSQL)
- ✅ Гибридный поиск через SQL
- ✅ Транзакции и надежность
- ✅ Можно использовать существующий PostgreSQL или отдельный

**Минусы:**
- Менее оптимизирован для векторного поиска чем Qdrant
- Требует настройку индексов

### Вариант 2: Qdrant (если нужна максимальная производительность)

**Почему:**
- ✅ Лучшая производительность для векторного поиска
- ✅ Более гибкие фильтры по метаданным
- ✅ Легковесный (Docker)

**Минусы:**
- Нужен отдельный сервис
- Требует адаптацию кода из sql4A

---

## План внедрения

### Этап 1: Подготовка инфраструктуры
1. Установить PostgreSQL с pgvector (или использовать существующий)
2. Создать таблицу `kb_billing_vectors` по образцу sql4A
3. Настроить индексы

### Этап 2: Адаптация кода
1. Создать `KBBillingTrainingClient` на основе `KBTrainingClient` из sql4A
2. Адаптировать функции поиска из `vanna_semantic_fixed.py`
3. Создать скрипт загрузки kb_billing

### Этап 3: Загрузка данных
1. Загрузить все данные из kb_billing через клиент
2. Сгенерировать эмбеддинги (используя скрипт из sql4A)
3. Проверить качество поиска

### Этап 4: Интеграция с агентом
1. Создать `BillingQueryService` на основе `QueryService` из sql4A
2. Интегрировать RAG pipeline
3. Тестирование на реальных запросах финансистов

---

## Преимущества использования подходов sql4A

1. **Проверенные решения** - код уже работает в production
2. **Единообразие** - один подход для всех KB проектов
3. **Переиспользование кода** - можно адаптировать существующие компоненты
4. **Поддержка** - есть документация и примеры
5. **Расширяемость** - легко добавить новые типы данных

---

## Следующие шаги

1. ✅ Изучить структуру sql4A
2. ⏳ Создать адаптированный клиент для kb_billing
3. ⏳ Настроить PostgreSQL с pgvector
4. ⏳ Загрузить данные из kb_billing
5. ⏳ Протестировать поиск
6. ⏳ Интегрировать с агентом















