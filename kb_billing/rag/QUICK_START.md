# Быстрый старт RAG системы для KB_billing

## Шаг 1: Установка зависимостей

```bash
pip install -r requirements.txt
```

Или отдельно:
```bash
pip install qdrant-client sentence-transformers torch transformers
```

## Шаг 2: Запуск Qdrant

```bash
# Запуск Qdrant в Docker
docker run -d \
  --name qdrant \
  -p 6333:6333 \
  -v $(pwd)/qdrant_storage:/qdrant/storage \
  qdrant/qdrant

# Проверка работы
curl http://localhost:6333/health
```

## Шаг 3: Инициализация KB

```bash
# Загрузка всех данных KB в Qdrant (выполняйте из корня репозитория, где лежит папка kb_billing)
cd /media/cnn/home/cnn/ai_report   # пример пути на вашей машине
python kb_billing/rag/init_kb.py

# С пересозданием коллекции (если нужно)
python kb_billing/rag/init_kb.py --recreate
```

Ожидаемый вывод:
```
============================================================
Инициализация KB_billing в Qdrant
============================================================
Qdrant: localhost:6333
Коллекция: kb_billing
Модель: intfloat/multilingual-e5-base
Пересоздать: False
============================================================

Загрузка Q/A примеров из ...
Загружено 24 Q/A примеров
Загрузка документации таблиц из ...
Загружено 36 точек документации таблиц
...
============================================================
✅ Инициализация завершена успешно!
============================================================
```

## Шаг 4: Запуск веб-интерфейса

```bash
# Запуск Streamlit
./run_streamlit_background.sh

# Или напрямую
streamlit run streamlit_report_oracle_backup.py
```

## Шаг 5: Использование ассистента

1. Откройте веб-интерфейс в браузере
2. Перейдите на первую закладку "🤖 Ассистент"
3. Введите вопрос на русском языке, например:
   - "Покажи превышение трафика за октябрь 2025"
   - "Найди все активные сервисы Iridium"
   - "Покажи себестоимость по всем SUB-"
4. Нажмите "📊 Сгенерировать SQL"

## Проверка работы

### Тест в Python

```python
from kb_billing.rag.rag_assistant import RAGAssistant

assistant = RAGAssistant()
examples = assistant.search_similar_examples("Покажи превышение трафика за октябрь 2025")

for example in examples:
    print(f"Вопрос: {example['question']}")
    print(f"SQL: {example['sql']}")
    print(f"Similarity: {example['similarity']:.3f}")
    print()
```

### Проверка Qdrant

```bash
# Проверка коллекций
curl http://localhost:6333/collections

# Информация о коллекции
curl http://localhost:6333/collections/kb_billing
```

## Troubleshooting

### Ошибка: "Connection refused" при подключении к Qdrant

```bash
# Проверьте, запущен ли Qdrant
docker ps | grep qdrant

# Если нет, запустите
docker run -d -p 6333:6333 qdrant/qdrant
```

### Ошибка: "Collection not found"

```bash
# Инициализируйте KB
python kb_billing/rag/init_kb.py --recreate
```

### Ошибка импорта модулей

```bash
# Убедитесь, что вы в корневой директории проекта (корень клона ai_report)
cd /media/cnn/home/cnn/ai_report   # пример; замените на свой путь

# Проверьте установку зависимостей
pip list | grep -E "qdrant|sentence-transformers"
```

### Медленная загрузка модели

При первом запуске модель эмбеддингов загружается из HuggingFace. Это может занять несколько минут. Модель кэшируется локально для последующих запусков.

## Следующие шаги

1. Добавьте больше Q/A примеров в `kb_billing/training_data/sql_examples.json`
2. Перезагрузите KB: `python kb_billing/rag/init_kb.py --recreate`
3. Используйте ассистента для генерации SQL запросов

## Дополнительная информация

- Полная документация: `kb_billing/rag/README.md`
- Рекомендации по архитектуре: `kb_billing/RAG_ARCHITECTURE_RECOMMENDATION.md`
- Векторная БД: Qdrant (коллекция kb_billing). См. [docs/kb-billing-vs-presales.md](../../docs/kb-billing-vs-presales.md)



