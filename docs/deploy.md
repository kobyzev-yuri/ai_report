# Развёртывание и поддержка ai_report

Один документ по схеме поддержки и тестирования проекта.

---

## Схема проекта

```
КОРЕНЬ (разработка)          deploy/ (пакет для сервера)        СЕРВЕР
─────────────────          ─────────────────────────          ──────
• Редактирование кода  →   prepare_deployment.sh         →   rsync/sync_deploy.sh
• Тесты локально            копирует нужные файлы              копирует deploy/ на сервер
• git push                  в deploy/                          Перезапуск: ./restart_streamlit.sh
```

- **Корень** — единственный источник истины. Все правки делаются здесь.
- **deploy/** — минимальный набор файлов для сервера. Создаётся скриптом, в git не коммитится как «исходник».
- **Сервер** — нет Docker Compose. Streamlit запущен процессом; перезапуск только через `./restart_streamlit.sh`, логи — `streamlit_8504.log`.

**Сервер:** 82.114.2.2, SSH порт 1194  
**Директория на сервере:** `/usr/local/projects/ai_report`  
**Доступ:** http://stat.steccom.ru:7776/ai_report

---

## 1. Подготовка пакета (локально)

Перед синхронизацией обновить содержимое `deploy/` из корня:

```bash
./prepare_deployment.sh
```

Скопирует в `deploy/`: приложение (Streamlit, tabs, auth), Oracle-скрипты, KB, скрипты управления (`restart_streamlit.sh` и др.).

---

## 2. Синхронизация на сервер

Из **корня** проекта (после `prepare_deployment.sh`):

```bash
SSH_CMD="ssh -p 1194" ./sync_deploy.sh root@82.114.2.2
```

Или вручную через rsync (из корня проекта):

```bash
rsync -avz --exclude-from=.rsyncignore -e "ssh -p 1194" \
  deploy/ root@82.114.2.2:/usr/local/projects/ai_report/
```

`config.env` и `users.db` на сервере не перезаписываются (исключены или уже есть).

---

## 3. Перезапуск на сервере

После любой синхронизации кода — перезапуск только на сервере:

```bash
ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && ./restart_streamlit.sh"
```

Логи:

```bash
ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && tail -f streamlit_8504.log"
```

---

## 4. Обновление базы знаний (KB)

Синхронизация KB и переиндексация в Qdrant (скрипт из **корня**):

```bash
SSH_CMD="ssh -p 1194" ./sync_and_update_kb.sh root@82.114.2.2
```

Используется при изменении примеров SQL, описаний таблиц/VIEW в `kb_billing/`. Перезапуск Streamlit после этого не обязателен, если меняли только KB.

---

## 5. Тестирование

| Этап | Действие |
|------|----------|
| Локально | Правки в корне → запуск Streamlit локально, проверка вкладок и логики |
| Пакет | `./prepare_deployment.sh` → проверка `deploy/` (наличие tabs, auth, KB, tests/) |
| Сервер | Синхронизация → `./restart_streamlit.sh` на сервере → проверка в браузере и по логам |

Тест биллинг-ассистента (топ-5 доходных клиентов) — **запускать на сервере** (нужны Qdrant, config.env с OPENAI_API_KEY, обученная KB):

```bash
ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && python3 -m tests.test_billing_assistant_top5"
```

Проверка здоровья на сервере:

```bash
# Streamlit
ssh -p 1194 root@82.114.2.2 "curl -s http://localhost:8504/_stcore/health"

# Qdrant (если используется)
ssh -p 1194 root@82.114.2.2 "curl -s http://localhost:6333/health"
```

---

## 6. Типовые сценарии

**Обновление кода (tabs, streamlit_*.py, auth и т.д.):**

```bash
./prepare_deployment.sh
SSH_CMD="ssh -p 1194" ./sync_deploy.sh root@82.114.2.2
ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && ./restart_streamlit.sh"
```

**Только обновление KB:**

```bash
SSH_CMD="ssh -p 1194" ./sync_and_update_kb.sh root@82.114.2.2
```

**Только примеры (sql_examples.json, user_added_examples.json)** — без полной перестройки таблиц/представлений/Confluence, быстрее:

```bash
# после синхронизации deploy/ на сервер
ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && python3 kb_billing/rag/init_kb.py --only-examples"
```

Перезапуск Streamlit не обязателен (KB подхватывается при следующем запросе).

**Обновление Oracle VIEW:** синхронизировать `deploy/` (или нужные файлы из `oracle/views/`), на сервере выполнить нужный `.sql`, при изменении кода приложения — перезапуск Streamlit (п. 3).

---

## 7. Важно

- На сервере **нет Docker Compose**. Не использовать `docker-compose restart streamlit`.
- Перезапуск только после синхронизации и только на сервере: `./restart_streamlit.sh`.
- Конфиг и БД пользователей: `config.env`, `users.db` — не перетирать с deploy; хранятся только на сервере (или копируются вручную при первом развёртывании).
