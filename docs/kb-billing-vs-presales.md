# KB для биллинга и для присэйлов: как устроено

## Одна коллекция, два домена

Сейчас используется **одна векторная коллекция** в Qdrant с именем из конфига **`QDRANT_COLLECTION`** (по умолчанию **`kb_billing`**). В ней лежат данные и для биллинга (SQL-отчёты, таблицы, примеры), и для присэйлов (документация Confluence, схемы сети).

Различие — по полям в **payload** точек:

| Домен        | Типы точек (payload.type)     | payload.domain | Источник данных |
|-------------|-------------------------------|----------------|------------------|
| **Биллинг** | qa_example, documentation, ddl, view, metadata | нет или общий | training_data/, tables/, views/, metadata.json |
| **Присейлы**| confluence_section (или confluence_doc)       | satellite      | confluence_docs/*.json |

Поиск по присейлам делается с фильтром `content_type="confluence_section"` (и при отсутствии — `confluence_doc`). RAG-ассистент и Спутниковый библиотекарь при запросах по документации используют именно эти точки.

---

## Как формируется KB для присэйлов

1. **Источник:** Confluence (страницы + вложения: PDF, draw.io, DOCX и т.д.).
2. **Выгрузка:** интерфейс «Спутниковый библиотекарь» → синхронизация пространства или страниц по ID → сохранение в **`kb_billing/confluence_docs/*.json`** на сервере (см. [confluence-kb-what-is-indexed.md](confluence-kb-what-is-indexed.md)).
3. **Загрузка в векторную БД:** при нажатии «Перезагрузить KB в Qdrant» вызывается `KBLoader.load_all()`:
   - читаются все `confluence_docs/*.json`;
   - каждая **секция** документа (в т.ч. блок «Вложение: …») превращается в отдельную точку с `type=confluence_section`, `domain=satellite`;
   - точки попадают в ту же коллекцию **kb_billing** вместе с точками биллинга.
4. **Поиск:** по запросу считаются эмбеддинги, поиск в Qdrant с фильтром по `type=confluence_section` (или без фильтра — тогда возвращаются и биллинг, и присейлы).

Итог: KB для присэйлов формируется из Confluence через Спутникового библиотекаря и хранится в той же коллекции Qdrant `kb_billing`, что и биллинг; различие только по типу и домену точек.

---

## Хранение векторов: только Qdrant

В проекте **векторы хранятся только в Qdrant**. Используется коллекция **`kb_billing`** (имя задаётся `QDRANT_COLLECTION` в config.env). Таблицы в PostgreSQL или других БД для векторов не используются.

---

## Где что в коде

- **Конфиг коллекции:** `kb_billing/rag/config_sql4a.py` — `QDRANT_COLLECTION` (по умолчанию `kb_billing`).
- **Загрузка всего в Qdrant:** `kb_billing/rag/kb_loader.py` — `load_all()`: qa_examples, tables, views, metadata, **load_confluence_docs()** (присейлы).
- **Поиск по присейлам:** `rag_assistant.py` — `search_semantic(..., content_type="confluence_section")`.
- **Выгрузка Confluence → JSON:** `confluence_kb_generator.py`; интерфейс — `streamlit_confluence_librarian.py`.
