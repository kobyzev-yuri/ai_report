# План интеграции FastAPI для Oracle (на основе sql4A)

## 📋 Обзор

Этот документ описывает план создания FastAPI для проекта ai_report на основе архитектуры и наработок проекта sql4A. API позволит использовать базу знаний (KB) из различных интерфейсов: веб, Telegram, мобильные приложения и т.д.

---

## 🏗️ 1. Архитектура API

### 1.1 Структура проекта

```
ai_report/
├── api/                          # Новый модуль FastAPI
│   ├── __init__.py
│   ├── main.py                   # Основной FastAPI app
│   ├── oracle_routes.py          # Роуты для Oracle (адаптация из sql4A)
│   ├── kb_routes.py              # Роуты для работы с KB
│   └── models/
│       ├── __init__.py
│       ├── requests.py           # Pydantic модели запросов
│       └── responses.py          # Pydantic модели ответов
├── services/                     # Бизнес-логика
│   ├── __init__.py
│   ├── query_service.py          # Сервис генерации SQL (адаптация из sql4A)
│   ├── oracle_service.py         # Сервис выполнения SQL в Oracle
│   └── kb_service.py             # Сервис работы с KB (RAGAssistant)
├── adapters/                     # Адаптеры для БД
│   ├── __init__.py
│   └── oracle_adapter.py         # Адаптер Oracle (из sql4A)
└── kb_billing/rag/               # Существующая RAG система
    ├── rag_assistant.py          # Используется в query_service
    └── kb_loader.py               # Используется для загрузки KB
```

### 1.2 Компоненты системы

```
┌─────────────┐
│   Client    │  (Web, Telegram, Mobile)
└──────┬──────┘
       │ HTTP/REST
       ▼
┌─────────────────┐
│   FastAPI       │  (api/main.py)
│   - /query      │
│   - /execute    │
│   - /kb/*       │
└──────┬──────────┘
       │
       ├──────────────┬──────────────┐
       ▼              ▼              ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│ Query       │ │ Oracle      │ │ KB          │
│ Service     │ │ Service     │ │ Service     │
│ (RAG)       │ │ (Execute)   │ │ (Manage)    │
└──────┬──────┘ └──────┬──────┘ └──────┬──────┘
       │                │                │
       ▼                ▼                ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│ RAGAssistant│ │ Oracle      │ │ Qdrant      │
│ (KB)        │ │ Database    │ │ (Vectors)    │
└─────────────┘ └─────────────┘ └─────────────┘
```

---

## 🔌 2. API Endpoints

### 2.1 Основные endpoints (аналогично sql4A)

#### Генерация SQL
- `POST /api/query` - Генерация SQL из вопроса
- `POST /api/query/execute` - Генерация и выполнение SQL
- `POST /api/query/explain` - Генерация SQL с EXPLAIN PLAN

#### Выполнение SQL
- `POST /api/oracle/execute` - Выполнение SQL запроса
- `POST /api/oracle/execute-with-stats` - Выполнение со статистикой

#### Работа с KB
- `GET /api/kb/examples` - Поиск похожих примеров
- `POST /api/kb/add-example` - Добавление примера в pending
- `GET /api/kb/pending` - Список ожидающих примеров
- `POST /api/kb/approve/{example_id}` - Одобрение примера
- `POST /api/kb/reject/{example_id}` - Отклонение примера
- `POST /api/kb/rebuild` - Перестройка KB

#### Информация о БД
- `GET /api/oracle/tables` - Список таблиц
- `GET /api/oracle/tables/{table_name}/columns` - Колонки таблицы
- `GET /api/oracle/tables/{table_name}/ddl` - DDL таблицы

#### Health check
- `GET /health` - Проверка здоровья системы
- `GET /api/health` - Детальная проверка компонентов

### 2.2 Модели запросов (Pydantic)

```python
# models/requests.py

class QueryRequest(BaseModel):
    question: str
    user_id: str
    role: Optional[str] = None
    department: Optional[str] = None
    context: Optional[Dict[str, Any]] = None

class ExecuteRequest(BaseModel):
    sql: str
    user_id: str
    with_stats: bool = False

class KBExampleRequest(BaseModel):
    question: str
    sql: str
    category: str
    complexity: int = 1
    business_entity: Optional[str] = None
    context: Optional[str] = None
    user_id: str
```

### 2.3 Модели ответов (Pydantic)

```python
# models/responses.py

class SQLResponse(BaseModel):
    sql: str
    question: str
    user_id: str
    timestamp: datetime
    confidence: Optional[float] = None

class QueryResultResponse(BaseModel):
    data: List[Dict[str, Any]]
    columns: List[str]
    row_count: int
    execution_time: float
    sql: str
    stats: Optional[str] = None  # Execution plan

class KBExampleResponse(BaseModel):
    success: bool
    example_id: Optional[str] = None
    message: str
    status: str  # pending | approved | rejected
```

---

## 🔧 3. Интеграция с существующими компонентами

### 3.1 Использование RAGAssistant

```python
# services/query_service.py

from kb_billing.rag.rag_assistant import RAGAssistant

class QueryService:
    def __init__(self):
        self.rag_assistant = RAGAssistant()
        # Инициализация из config.env или переменных окружения
    
    async def generate_sql(self, question: str, user_context: Dict) -> str:
        # Получение контекста из KB
        context = self.rag_assistant.get_context_for_sql_generation(question)
        
        # Генерация SQL через LLM (если доступен)
        if os.getenv("OPENAI_API_KEY"):
            sql = self.rag_assistant.generate_sql_with_llm(
                question=question,
                context=context,
                api_key=os.getenv("OPENAI_API_KEY"),
                api_base=os.getenv("OPENAI_BASE_URL")
            )
            return sql
        
        # Иначе возвращаем первый похожий пример
        examples = self.rag_assistant.search_similar_examples(question, limit=1)
        if examples:
            return examples[0]['sql']
        
        raise ValueError("Не удалось сгенерировать SQL")
```

### 3.2 Использование Oracle подключения

```python
# services/oracle_service.py

from adapters.oracle_adapter import OracleAdapter
import pandas as pd

class OracleService:
    def __init__(self):
        self.adapter = OracleAdapter()
    
    async def execute_sql(self, sql: str, with_stats: bool = False) -> Dict:
        conn = self.adapter._get_connection()
        try:
            if with_stats:
                # Включить сбор статистики
                cursor = conn.cursor()
                cursor.execute("ALTER SESSION SET STATISTICS_LEVEL = ALL")
                cursor.execute("ALTER SESSION SET TIMED_STATISTICS = TRUE")
            
            # Выполнение запроса
            start_time = time.time()
            df = pd.read_sql(sql, conn)
            execution_time = time.time() - start_time
            
            # Получение статистики (если запрошено)
            stats = None
            if with_stats:
                stats = self._get_execution_plan(cursor)
            
            return {
                "data": df.to_dict('records'),
                "columns": list(df.columns),
                "row_count": len(df),
                "execution_time": execution_time,
                "stats": stats
            }
        finally:
            conn.close()
```

### 3.3 Адаптер Oracle (из sql4A)

```python
# adapters/oracle_adapter.py
# Использовать готовый адаптер из sql4A/src/adapters/oracle_adapter.py
# Адаптировать под структуру ai_report
```

---

## 📦 4. Зависимости и требования

### 4.1 Новые зависимости

```txt
# requirements.txt (дополнить)

fastapi>=0.104.0
uvicorn[standard]>=0.24.0
pydantic>=2.0.0
python-multipart>=0.0.6  # Для file uploads
```

### 4.2 Существующие зависимости (использовать)

- `qdrant-client` - для работы с векторной БД
- `sentence-transformers` - для эмбеддингов
- `cx_Oracle` или `oracledb` - для Oracle
- `pandas` - для работы с данными
- `openai` - для генерации SQL (опционально)

---

## 🚀 5. План реализации

### 5.1 Фаза 1: Базовая структура API (Приоритет: ВЫСОКИЙ)

- [ ] Создать структуру директорий `api/`, `services/`, `adapters/`
- [ ] Скопировать и адаптировать `oracle_adapter.py` из sql4A
- [ ] Создать базовый FastAPI app (`api/main.py`)
- [ ] Реализовать `QueryService` с интеграцией `RAGAssistant`
- [ ] Реализовать `OracleService` для выполнения SQL
- [ ] Создать модели запросов/ответов (Pydantic)
- [ ] Реализовать базовые endpoints:
  - `POST /api/query` - генерация SQL
  - `POST /api/query/execute` - генерация и выполнение
  - `GET /health` - health check

**Оценка времени**: 3-5 дней

### 5.2 Фаза 2: Расширенные endpoints (Приоритет: СРЕДНИЙ)

- [ ] Реализовать `POST /api/oracle/execute` - прямое выполнение SQL
- [ ] Реализовать `POST /api/oracle/execute-with-stats` - со статистикой
- [ ] Реализовать `GET /api/oracle/tables` - список таблиц
- [ ] Реализовать `GET /api/oracle/tables/{table_name}/columns` - колонки
- [ ] Реализовать `GET /api/oracle/tables/{table_name}/ddl` - DDL
- [ ] Добавить обработку ошибок и валидацию

**Оценка времени**: 2-3 дня

### 5.3 Фаза 3: Управление KB через API (Приоритет: СРЕДНИЙ)

- [ ] Реализовать `GET /api/kb/examples` - поиск примеров
- [ ] Реализовать `POST /api/kb/add-example` - добавление в pending
- [ ] Реализовать `GET /api/kb/pending` - список pending
- [ ] Реализовать `POST /api/kb/approve/{example_id}` - одобрение
- [ ] Реализовать `POST /api/kb/reject/{example_id}` - отклонение
- [ ] Реализовать `POST /api/kb/rebuild` - перестройка KB

**Оценка времени**: 3-4 дня

### 5.4 Фаза 4: Интеграция с интерфейсами (Приоритет: НИЗКИЙ)

- [ ] Интеграция с Streamlit (использовать API вместо прямых вызовов)
- [ ] Telegram бот (использовать API)
- [ ] Мобильное приложение (REST API)
- [ ] Аутентификация и авторизация (JWT токены)

**Оценка времени**: 1-2 недели

---

## 🔄 6. Использование наработок из sql4A

### 6.1 Что можно использовать напрямую

1. **Oracle Adapter** (`sql4A/src/adapters/oracle_adapter.py`)
   - Полностью готовый адаптер для Oracle
   - Методы: `get_tables()`, `get_table_columns()`, `get_ddl()`, `add_table_comment()`
   - Адаптировать только конфигурацию подключения
   - **Путь**: `/mnt/ai/cnn/sql4A/src/adapters/oracle_adapter.py`

2. **Модели запросов/ответов** (`sql4A/src/models/`)
   - `QueryRequest`, `SQLResponse`, `QueryResultResponse`
   - `TrainingExampleRequest`, `TrainingResponse`
   - Адаптировать под специфику ai_report (добавить поля для KB)
   - **Путь**: `/mnt/ai/cnn/sql4A/src/models/requests.py`, `responses.py`

3. **Oracle Routes** (`sql4A/src/api/oracle_routes.py`)
   - Готовые endpoints для работы с Oracle
   - `/api/oracle/tables`, `/api/oracle/tables/{table_name}/columns`, `/api/oracle/tables/{table_name}/ddl`
   - Адаптировать под структуру ai_report
   - **Путь**: `/mnt/ai/cnn/sql4A/src/api/oracle_routes.py`

4. **Query Service архитектура** (`sql4A/src/services/query_service.py`)
   - Архитектура сервиса генерации SQL
   - Методы: `generate_sql()`, `add_training_example()`, `test_vector_search()`
   - Адаптировать под использование `RAGAssistant` вместо Vanna
   - **Путь**: `/mnt/ai/cnn/sql4A/src/services/query_service.py`

5. **Базовая структура FastAPI** (`sql4A/src/api/main.py`)
   - Инициализация FastAPI app
   - CORS настройки
   - Health check endpoints
   - Структура роутинга
   - **Путь**: `/mnt/ai/cnn/sql4A/src/api/main.py`

### 6.2 Что нужно адаптировать

1. **Query Service**
   - Заменить Vanna AI на `RAGAssistant` из `kb_billing/rag/rag_assistant.py`
   - Использовать существующую систему эмбеддингов (Qdrant через RAGAssistant)
   - Интегрировать с существующей KB структурой (`sql_examples.json`)
   - Использовать `get_context_for_sql_generation()` вместо Vanna методов

2. **Конфигурация**
   - Использовать `config.env` из ai_report (уже есть Oracle настройки)
   - Переменные окружения уже настроены: `ORACLE_USER`, `ORACLE_PASSWORD`, `ORACLE_HOST`, etc.
   - Qdrant настройки: `QDRANT_HOST`, `QDRANT_PORT`, `QDRANT_COLLECTION`

3. **Модели KB**
   - Добавить поля для KB метаданных (category, complexity, business_entity)
   - Интегрировать с форматом `sql_examples.json`
   - Поддержать статусы: `pending`, `approved`, `rejected`

4. **Выполнение SQL**
   - Использовать существующие функции из `streamlit_assistant.py`:
     - `execute_sql_query()` - базовое выполнение
     - `execute_sql_with_stats()` - со статистикой
     - `explain_plan()` - план выполнения

### 6.3 Конкретные файлы для копирования/адаптации

#### Копировать с минимальными изменениями:
- `sql4A/src/adapters/oracle_adapter.py` → `ai_report/adapters/oracle_adapter.py`
- `sql4A/src/api/oracle_routes.py` → `ai_report/api/oracle_routes.py` (адаптировать)
- `sql4A/src/models/requests.py` → `ai_report/api/models/requests.py` (расширить)
- `sql4A/src/models/responses.py` → `ai_report/api/models/responses.py` (расширить)

#### Создать на основе sql4A:
- `sql4A/src/api/main.py` → `ai_report/api/main.py` (адаптировать под RAGAssistant)
- `sql4A/src/services/query_service.py` → `ai_report/services/query_service.py` (переписать под RAGAssistant)

#### Использовать из ai_report:
- `kb_billing/rag/rag_assistant.py` - RAG система
- `kb_billing/rag/kb_loader.py` - загрузка KB
- `streamlit_assistant.py` - функции выполнения SQL (адаптировать для API)

---

## 📝 7. Примеры использования API

### 7.1 Генерация SQL

```bash
curl -X POST "http://localhost:8000/api/query" \
  -H "Content-Type: application/json" \
  -d '{
    "question": "Покажи расходы по клиенту АК Ямал Салехардский филиал за февраль 2025",
    "user_id": "financier_001",
    "role": "financier"
  }'
```

### 7.2 Генерация и выполнение SQL

```bash
curl -X POST "http://localhost:8000/api/query/execute" \
  -H "Content-Type: application/json" \
  -d '{
    "question": "Найди информацию по клиенту АК Ямал Салехардский филиал - интересует код 1С",
    "user_id": "financier_001",
    "role": "financier"
  }'
```

### 7.3 Добавление примера в KB

```bash
curl -X POST "http://localhost:8000/api/kb/add-example" \
  -H "Content-Type: application/json" \
  -d '{
    "question": "Найди информацию по клиенту АК Ямал Салехардский филиал - интересует код 1С",
    "sql": "SELECT DISTINCT c.CUSTOMER_ID, ...",
    "category": "Клиенты",
    "complexity": 3,
    "business_entity": "customers",
    "user_id": "financier_001"
  }'
```

### 7.4 Telegram бот (пример)

```python
# telegram_bot.py

import requests
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters

API_BASE_URL = "http://localhost:8000"

async def handle_query(update: Update, context):
    question = update.message.text
    
    response = requests.post(
        f"{API_BASE_URL}/api/query/execute",
        json={
            "question": question,
            "user_id": str(update.effective_user.id),
            "role": "telegram_user"
        }
    )
    
    result = response.json()
    await update.message.reply_text(
        f"SQL: {result['sql']}\n\n"
        f"Результатов: {result['row_count']}\n"
        f"Время: {result['execution_time']:.2f} сек"
    )
```

---

## 🔒 8. Безопасность

### 8.1 Аутентификация

- [ ] JWT токены для API
- [ ] Интеграция с существующей системой аутентификации (`auth_db.py`)
- [ ] Rate limiting для предотвращения злоупотреблений

### 8.2 Валидация SQL

- [ ] Проверка на опасные операции (DROP, DELETE, TRUNCATE)
- [ ] Whitelist разрешенных таблиц (опционально)
- [ ] Ограничение времени выполнения запросов

### 8.3 Логирование

- [ ] Логирование всех запросов
- [ ] Логирование выполнения SQL
- [ ] Аудит изменений KB

---

## 📊 9. Мониторинг и метрики

### 9.1 Метрики API

- [ ] Количество запросов в секунду
- [ ] Время ответа API
- [ ] Процент успешных генераций SQL
- [ ] Процент успешных выполнений SQL

### 9.2 Метрики KB

- [ ] Количество примеров в KB
- [ ] Процент использования примеров из KB
- [ ] Качество генерации SQL (обратная связь)

---

## 🔗 10. Интеграция с существующими компонентами

### 10.1 Streamlit

- [ ] Использовать API вместо прямых вызовов `RAGAssistant`
- [ ] Сохранение примеров через API
- [ ] Единая точка входа для всех интерфейсов

### 10.2 Существующие скрипты

- [ ] `sync_and_rebuild_kb.sh` - использовать API для перестройки KB
- [ ] Интеграция с процессами развертывания

---

## 📅 11. Приоритеты реализации

### Высокий приоритет (MVP)

1. Базовая структура API
2. Генерация SQL через RAGAssistant
3. Выполнение SQL в Oracle
4. Health check

### Средний приоритет

1. Управление KB через API
2. Расширенные endpoints для Oracle
3. Интеграция с Streamlit

### Низкий приоритет

1. Telegram бот
2. Мобильное приложение
3. Продвинутые метрики и аналитика

---

## 📚 12. Связанные документы

- `TODO.md` - План развития KB (включает процесс добавления примеров через API)
- `kb_billing/rag/README.md` - Документация RAG системы
- `deploy/DEPLOYMENT_RAG.md` - Документация развертывания
- `/mnt/ai/cnn/sql4A/docs/API_REFERENCE.md` - Референс API sql4A
- `/mnt/ai/cnn/sql4A/src/api/main.py` - Реализация API в sql4A
- `/mnt/ai/cnn/sql4A/src/api/oracle_routes.py` - Oracle routes из sql4A

---

## 💡 13. Примечания

- API должен быть совместим с существующей архитектурой ai_report
- Использовать существующие компоненты (RAGAssistant, KBLoader) без дублирования
- Обеспечить возможность работы без LLM (только поиск по примерам)
- Поддержать как синхронные, так и асинхронные операции

---

## 🔧 14. Конкретные шаги интеграции

### Шаг 1: Подготовка структуры

```bash
# Создать структуру директорий
mkdir -p api/models services adapters

# Скопировать адаптер Oracle из sql4A
cp /mnt/ai/cnn/sql4A/src/adapters/oracle_adapter.py adapters/
cp /mnt/ai/cnn/sql4A/src/adapters/base.py adapters/  # если нужен базовый класс

# Скопировать модели
cp /mnt/ai/cnn/sql4A/src/models/requests.py api/models/
cp /mnt/ai/cnn/sql4A/src/models/responses.py api/models/
```

### Шаг 2: Адаптация Oracle Adapter

- Изменить импорты под структуру ai_report
- Использовать конфигурацию из `config.env` (уже есть Oracle настройки)
- Проверить совместимость с существующим подключением Oracle

### Шаг 3: Создание Query Service

- Использовать `RAGAssistant` вместо Vanna
- Интегрировать с существующими функциями выполнения SQL
- Поддержать режим без LLM (только поиск по примерам)

### Шаг 4: Создание Oracle Service

- Использовать функции из `streamlit_assistant.py`:
  - `execute_sql_query()` → `OracleService.execute()`
  - `execute_sql_with_stats()` → `OracleService.execute_with_stats()`
  - `explain_plan()` → `OracleService.explain_plan()`

### Шаг 5: Создание KB Service

- Использовать `RAGAssistant` для поиска примеров
- Использовать `KBLoader` для добавления примеров
- Реализовать работу с `pending_examples.json`

### Шаг 6: Создание FastAPI app

- Инициализировать FastAPI
- Подключить роуты
- Настроить CORS
- Добавить health check

### Шаг 7: Интеграция с существующим кодом

- Streamlit может использовать API вместо прямых вызовов
- Сохранение примеров через API endpoint
- Единая точка входа для всех интерфейсов

---

## 📝 История изменений

- **2025-01-XX**: Создан план интеграции FastAPI на основе sql4A

