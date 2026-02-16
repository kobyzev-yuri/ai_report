# Процесс расширения KB по методологии sql4A

## Методология sql4A: три шага обучения агента

### Шаг 1: DDL (Data Definition Language)
**Назначение:** Структура таблиц и представлений Oracle

**Источники:**
- `oracle/tables/*.sql` - CREATE TABLE statements
- `oracle/views/*.sql` - CREATE VIEW statements  
- `DESCRIBE table_name` из Oracle - реальная структура таблиц

**Формат в KB:**
- Поле `"ddl"` в `kb_billing/tables/*.json`
- Загружается в Qdrant с `content_type="ddl"`

### Шаг 2: Q/A Examples (Примеры вопрос-ответ)
**Назначение:** Примеры вопросов финансистов и соответствующих SQL запросов

**Источники:**
- `kb_billing/training_data/sql_examples.json`
- Реальные запросы пользователей
- Типовые финансовые отчеты

**Формат:**
```json
{
  "question": "отчет о дубликатах в analytics за период (period_id)",
  "sql": "WITH duplicate_groups AS (...) SELECT ...",
  "context": "Поиск дубликатов...",
  "business_entity": "analytics",
  "complexity": 4,
  "category": "Дубликаты"
}
```

### Шаг 3: Documentation (Документация)
**Назначение:** Бизнес-логика, правила, связи между таблицами, примеры использования

**Источники:**
- `kb_billing/tables/*.json` - описания таблиц
- `kb_billing/views/*.json` - описания представлений
- Объяснения пользователя о бизнес-процессах

**Формат:**
- `description` - общее описание
- `key_columns` - ключевые поля с описаниями
- `business_rules` - бизнес-правила
- `relationships` - связи с другими таблицами
- `usage_notes` - примеры использования и важные замечания

## Процесс добавления знаний

### Когда пользователь объясняет что-то:

1. **Определить тип знания:**
   - DDL? → Добавить в `tables/*.json` поле `"ddl"`
   - Q/A пример? → Добавить в `training_data/sql_examples.json`
   - Документация? → Обновить `tables/*.json` или `views/*.json`

2. **Добавить знания:**
   - Обновить соответствующий JSON файл
   - Убедиться в правильности формата
   - Добавить метаданные (category, complexity, business_entity)

3. **Синхронизировать и обновить KB:**
   ```bash
   SSH_CMD="ssh -p 1194" ./sync_and_update_kb.sh root@82.114.2.2
   ```

## Текущее состояние KB

### Таблицы с DDL (4):
- SPNET_TRAFFIC.json ✅
- STECCOM_EXPENSES.json ✅
- TARIFF_PLANS.json ✅
- IRIDIUM_SERVICES_INFO.json ✅

### Таблицы БЕЗ DDL (28):
- ANALYTICS.json ❌
- ACCOUNTS.json ❌
- CUSTOMERS.json ❌
- SERVICES.json ❌
- BM_* таблицы ❌
- И другие...

### Что нужно сделать:

1. **Добавить DDL для всех таблиц:**
   - Извлечь из Oracle через `DESCRIBE` или `DBMS_METADATA.GET_DDL`
   - Добавить в `tables/*.json` как поле `"ddl"`

2. **Проверить Q/A примеры:**
   - Убедиться, что есть примеры для всех основных сценариев
   - Добавить примеры для новых функций (дубликаты, счета за период)

3. **Обновить документацию:**
   - Добавить информацию о новых VIEW (V_ANALYTICS_INVOICE_PERIOD)
   - Обновить бизнес-правила на основе объяснений пользователя

## Скрипт для извлечения DDL из Oracle

```sql
-- Извлечение DDL для таблицы ANALYTICS
SELECT DBMS_METADATA.GET_DDL('TABLE', 'ANALYTICS', 'BILLING7') FROM DUAL;
```

Или через DESCRIBE:
```sql
DESC ANALYTICS;
```

## Важно:

- **НЕ запускать init_kb.py локально** - Qdrant на удаленном сервере
- **Всегда использовать sync_and_update_kb.sh** для синхронизации и обновления KB
- **При добавлении знаний** - обновлять все три типа: DDL, Q/A, Documentation


