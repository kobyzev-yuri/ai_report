# Синхронизация Confluence → спутниковая KB: сценарии и общий модуль

Три сценария: **автоматический** парсинг (скрипты), **полуавтоматический** (веб: ввод пространства/URL → обработка → сохранение в KB), **деактивация/удаление** устаревшего контента. Общая логика вынесена в один модуль и переиспользуется в CLI и веб-интерфейсе.

---

## Тестирование на сервере

### 1. Синхронизация кода на сервер

Скрипты и модули попадают в `deploy/` при подготовке деплоя и затем на сервер при синхронизации:

```bash
# Локально (из корня репозитория)
./prepare_deployment.sh    # копирует kb_billing/rag/*.py и scripts/sync_*.py, clean_satellite_kb.py, analyze_confluence_space.py в deploy/
./sync_deploy.sh           # или: ./sync_deploy.sh user@server
```

На сервере после синхронизации будут:
- `kb_billing/rag/confluence_sync_runner.py`, `confluence_kb_generator.py`, `confluence_client.py`, парсеры и т.д.;
- `scripts/sync_confluence_spaces.py`, `scripts/sync_confluence_pages.py`, `scripts/confluence_kb_outdated.py`, `scripts/clean_satellite_kb.py`, `scripts/analyze_confluence_space.py`, `scripts/test_satellite_rag_search.py`.

Убедитесь, что на сервере есть `config.env` с `CONFLUENCE_URL` и `CONFLUENCE_TOKEN` (sync_deploy не перезаписывает config.env).

### 2. Зачистка спутниковой KB перед тестами

Если в спутниковой KB уже есть старые тестовые данные (например из Confluence Ширяева), их нужно убрать: удалить документы из `kb_billing/confluence_docs/` и затем перезагрузить Confluence в Qdrant.

**На сервере** (из каталога приложения, например `/usr/local/projects/ai_report` или `deploy/`):

```bash
# Сначала посмотреть, что будет затронуто (dry-run)
python scripts/clean_satellite_kb.py --dry-run

# Вариант А: полная зачистка (удалить все JSON и outdated.txt)
python scripts/clean_satellite_kb.py

# Вариант Б: перенести в backup/ с меткой времени (чтобы можно было откатить)
python scripts/clean_satellite_kb.py --backup
```

Если скрипты запускаются из подкаталога или путь к данным другой:

```bash
python scripts/clean_satellite_kb.py --output-dir /usr/local/projects/ai_report/kb_billing/confluence_docs
```

После зачистки **обязательно** выполнить перезагрузку Confluence в Qdrant, иначе в векторной БД останутся старые чанки:
- в веб-интерфейсе: вкладка «Спутниковый ассистент» → «Confluence / Qdrant» → кнопка «Перезагрузить в Qdrant только Confluence»;
- или программно (на сервере, из каталога приложения): загрузчик KB с вызовом `reload_confluence_only()`.

### 3. Последовательность тестов на сервере

1. Синхронизировать код: `./sync_deploy.sh` (после локального `./prepare_deployment.sh`).
2. Зайти на сервер, перейти в каталог приложения.
3. Зачистить спутниковую KB: `python scripts/clean_satellite_kb.py --backup` (или без `--backup`).
4. Перезагрузить Confluence в Qdrant (веб или loader).
5. Запустить тесты, например:
   - `python scripts/analyze_confluence_space.py ~a.zhegalov --limit 5 --output azhegalov.json`
   - `python scripts/sync_confluence_spaces.py ~a.zhegalov --limit 5`
   - `python scripts/sync_confluence_pages.py 4392673` (один page_id для проверки)
6. Снова перезагрузить Confluence в Qdrant и проверить поиск в веб-интерфейсе.

**Проверка поиска без веб-интерфейса** — тестовый скрипт (удобно гонять на сервере после перезагрузки KB):

```bash
# Запрос по умолчанию: «Спецификация Стар-Т типовая»
python scripts/test_satellite_rag_search.py

# Свой запрос
python scripts/test_satellite_rag_search.py "опишите Спецификация Стар-Т типовая"
python scripts/test_satellite_rag_search.py "Спецификация Стар-Т типовая" -v
```

Скрипт выводит: результаты поиска по `section_title` (подстрока в названии секции), результаты семантического поиска и итог — нашёлся ли нужный документ. Опция `-v` печатает начало `content` каждого чанка.

---

## Общий модуль: `kb_billing/rag/confluence_sync_runner.py`

Единая точка входа для скриптов и веб-интерфейса:

| Функция | Назначение |
|--------|------------|
| `get_client()`, `get_generator()` | Создание клиента и генератора (env: CONFLUENCE_URL, CONFLUENCE_TOKEN). |
| `sync_spaces(space_keys, limit, output_dir)` | Синхронизация пространств → по одному файлу `confluence_{key}.json` на пространство. |
| `sync_pages(urls_or_ids, output_dir, output_suffix, merge)` | Синхронизация страниц по URL/ID. При `merge=True` — дополнение/апдейт существующего JSON. |
| `mark_outdated(page_ids)` | Добавить page_id в `outdated.txt` (при перезагрузке в Qdrant не попадут в поиск). |
| `unmark_outdated(page_ids)` | Убрать из `outdated.txt`. |
| `remove_pages_from_kb(page_ids)` | Удалить документы из всех JSON в `confluence_docs` и добавить в outdated. |
| `update_pages_in_kb(page_ids)` | Обновить документы по page_id: заново забрать из Confluence и перезаписать в JSON. |

Формат ввода страниц: каждая строка — URL или `page_id`, опционально с описанием через таб или 2+ пробела (`URL  описание`).

---

## 1. Автоматический апдейт и загрузка (скрипты)

Используют общий модуль. После запуска нужно выполнить перезагрузку Confluence в Qdrant (веб или loader), чтобы чанки попали в поиск.

### Синхронизация пространств

**Указанные пространства:**
```bash
export CONFLUENCE_URL=https://docs.steccom.ru CONFLUENCE_TOKEN=...
python scripts/sync_confluence_spaces.py ~a.zhegalov SPC
python scripts/sync_confluence_spaces.py ~a.zhegalov --limit 20 --output-dir kb_billing/confluence_docs
```

**Все пространства Confluence** (по списку из API, не только одно):
```bash
python scripts/sync_confluence_spaces.py --all
python scripts/sync_confluence_spaces.py --all --limit 30 --exclude DEMO,TEST
python scripts/sync_confluence_spaces.py --all --personal-only   # только личные (~user)
```

- Результат: для каждого пространства файл `confluence_{space_key}.json` в каталоге KB.

### Синхронизация страниц по URL/ID (с возможностью merge)

Поддерживаемые форматы ссылки: `pageId=...`, `/spaces/.../pages/ID/...`, `/download/attachments/ID/...`, голый ID.

**Рекурсивно с дочерними страницами** (страница «Спецификации» + все документы-ссылки с неё):
```bash
python scripts/sync_confluence_pages.py 4392243 --with-children
python scripts/sync_confluence_pages.py "https://docs.steccom.ru/spaces/SPC/pages/4392243/Спецификации" --with-children --merge
```
Дочерние страницы запрашиваются через Confluence API (`/rest/api/content/{id}/child/page`). По умолчанию один уровень (`--max-depth 1`), не более 200 дочерних (`--limit-children 200`).

```bash
python scripts/sync_confluence_pages.py https://docs.steccom.ru/pages/viewpage.action?pageId=4392673
python scripts/sync_confluence_pages.py "https://docs.steccom.ru/spaces/SPC/pages/4392243/Спецификации"
python scripts/sync_confluence_pages.py 4392673 4392243
python scripts/sync_confluence_pages.py --file urls.txt --merge --output-suffix custom_pages
```

- Без `--merge`: создаётся/перезаписывается `confluence_custom_pages.json` только перечисленными страницами.
- С `--merge`: существующий файл загружается, перечисленные страницы обновляются или добавляются, остальные сохраняются.

### Деактивация и удаление устаревшего контента

```bash
python scripts/confluence_kb_outdated.py add 4392673 123456    # добавить в outdated.txt
python scripts/confluence_kb_outdated.py remove 4392673       # убрать из outdated.txt
python scripts/confluence_kb_outdated.py list                  # показать текущие устаревшие
python scripts/confluence_kb_outdated.py delete 4392673 123456 # удалить из JSON и пометить устаревшими
```

- `add` / `remove`: только список устаревших; при следующей перезагрузке Confluence в Qdrant эти page_id не подтянутся / снова подтянутся.
- `delete`: документы удаляются из всех `confluence_docs/*.json` и их page_id добавляются в `outdated.txt`.

---

## 2. Полуавтоматический режим (веб-интерфейс)

Вкладка **«Спутниковый ассистент»** → **«Confluence / Qdrant»**:

- **Пространство**: ввод ключа пространства, лимит страниц → кнопка «Синхронизировать пространство в KB» (как раньше, через генератор).
- **Конкретные страницы**: ввод URL или page_id (по строке), опция **«Дополнить/обновить существующий список (merge)»** → кнопка «Синхронизировать выбранные страницы в KB». Логика выполняется через `confluence_sync_runner.sync_pages(..., merge=...)`.
- После изменений в «Редактор KB» или после синхронизации — кнопка **«Перезагрузить в Qdrant только Confluence»**, чтобы обновить поиск.

Планируется: выбор пространства/URL → предпросмотр и правка смысловых блоков в веб → сохранение в KB через тот же runner.

---

## 3. Удаление и деактивация устаревшего контента

- **Только деактивация (не удалять из JSON):** скрипт `confluence_kb_outdated.py add <page_id...>` или в веб — возможность добавить page_id в `outdated.txt` (при необходимости можно вынести в UI).
- **Удаление из KB и пометка устаревшими:** скрипт `confluence_kb_outdated.py delete <page_id...>`.

Биллинг (Q/A, таблицы, представления) не затрагивается; меняется только загрузка Confluence-документов в Qdrant.

---

## Что не попадает в синхронизацию (технические и версионные объекты)

При синхронизации **по пространству** или **всех пространств** в KB попадает не весь контент Confluence. Учитывается следующее.

### Тип контента

- **Индексируются только страницы (`type=page`).** Запрос к API идёт с `type=page`.
- **Блог-посты (`blogpost`)** в Confluence не запрашиваются и в KB не попадают. При необходимости их можно добавить отдельным типом контента в клиенте.
- **Комментарии, лейблы, макросы как отдельные объекты** не выгружаются — только тело страницы (storage) и вложения.

### Версии

- Берётся **только текущая версия** страницы и вложений. История версий (старые ревизии) в KB не загружается — для поиска и ответов ассистента обычно нужна актуальная версия.
- При следующей синхронизации та же страница перезапишется новым содержимым (апдейт по текущей версии).

### Пространства

- Список пространств для `--all` берётся из **REST API** (`/rest/api/space`) с пагинацией; получаются все пространства, доступные токену.
- **Скрытые, архивные или недоступные** для токена пространства API не вернёт — они не попадут в синхронизацию.
- Фильтры `--exclude` и `--personal-only` дополнительно сужают список уже после ответа API.

### Вложения

- Обрабатываются вложения текущей версии страницы (см. парсеры: PDF, DOCX, XLS/XLSX, изображения, draw.io). Служебные форматы (`.tmp`, `.render`, `.tfss`) пропускаются.
- Старые версии вложений в Confluence не запрашиваются.

Итого: технические объекты вроде **истории версий** и **блог-постов** при текущей настройке в синхронизацию не попадают; для типичной спутниковой KB этого достаточно. При необходимости можно добавить поддержку `blogpost` и опцию «включать старые версии» (отдельная доработка).

---

## Каталоги и конфиг

- **Контент KB:** `kb_billing/confluence_docs/*.json`, список устаревших — `kb_billing/confluence_docs/outdated.txt`.
- **Конфиг:** `CONFLUENCE_URL`, `CONFLUENCE_TOKEN` в `config.env` или в окружении.
- Скрипты загружают `config.env` из корня репозитория при наличии файла.
