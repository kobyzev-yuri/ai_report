# Инструкция по работе с проектом ai_report

## 🔄 После миграции на новый диск

Проверь целостность и связь с GitHub:

1. **Git и GitHub**
   - `git remote -v` — должен быть `origin` → `https://github.com/kobyzev-yuri/ai_report.git`
   - `git fetch origin` или `git ls-remote origin HEAD` — проверка доступа к GitHub
   - Реши, что делать с локальными изменениями: `git status`, затем коммит или `git stash`

2. **Конфигурация**
   - В корне должен быть `config.env` (не в git), скопированный с `config.env.example` и заполненный
   - На сервере и в `deploy/` — свой `config.env` (создаётся вручную при первом деплое)

3. **Ошибка «Terminal sandbox could not start» (AppArmor, kernel 6.2+)**
   - На Linux с AppArmor Cursor не может запустить песочницу терминала. Варианты:
   - **Вариант A (рекомендуется):** установить профиль AppArmor для Cursor:
     ```bash
     # Debian / Ubuntu
     curl -fsSL https://downloads.cursor.com/lab/enterprise/cursor-sandbox-apparmor_0.2.0_all.deb -o cursor-sandbox-apparmor.deb
     sudo dpkg -i cursor-sandbox-apparmor.deb
     ```
     После установки перезапусти Cursor.
   - **Вариант B:** отключить песочницу для агента: в настройках Cursor → **Cursor Settings → Agents → Auto-Run** выбери **Ask Every Time** или **Run Everything** (команды будут выполняться без песочницы).
   - **Вариант C (временный workaround):** если бинарник песочницы есть, но не хватает прав:
     ```bash
     sudo chmod 4755 /usr/share/cursor/resources/app/resources/helpers/cursor-sandbox
     ```
     Путь может отличаться в зависимости от способа установки Cursor (AppImage, snap и т.д.).

---

## ⚠️ ВАЖНОЕ НАПОМИНАНИЕ

**При перезапуске Streamlit на сервере ВСЕГДА используй:**
```bash
ssh -p 1194 root@82.114.2.2 \
  "cd /usr/local/projects/ai_report && ./restart_streamlit.sh"
```

**НЕ используй `docker-compose restart streamlit`** — на сервере Streamlit запущен как обычный процесс, а не через Docker.

**При синхронизации deploy на сервер:** см. раздел [Deploy: синхронизация, перестройка KB (Qdrant), перезапуск](#deploy-синхронизация-перестройка-kb-qdrant-перезапуск) — там команды rsync, полная перестройка KB / только примеры (`init_kb.py --only-examples`), перезапуск.

---

## 🎯 Логика проекта

### Архитектура развертывания

```
┌─────────────────────────────────────────────────────────────┐
│              ЛОКАЛЬНАЯ РАБОЧАЯ ДИРЕКТОРИЯ                   │
│  (разработка, коммиты в git, push на GitHub)                │
├─────────────────────────────────────────────────────────────┤
│  • tabs/              - исходные модули Streamlit           │
│  • oracle/            - SQL скрипты (VIEW, функции, таблицы)│
│  • python/            - скрипты загрузки данных             │
│  • kb_billing/        - база знаний (KB) для RAG           │
│  • streamlit_*.py     - основные файлы приложения           │
│  • requirements.txt   - зависимости                         │
└─────────────────────────────────────────────────────────────┘
                        │
                        │ КОПИРОВАНИЕ в deploy/
                        │ (prepare_deployment.sh или вручную)
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              ДИРЕКТОРИЯ deploy/ (локально)                  │
│  (ТОЧНАЯ КОПИЯ файлов из корня для синхронизации)          │
├─────────────────────────────────────────────────────────────┤
│  • tabs/              - КОПИИ из tabs/                     │
│  • oracle/            - КОПИИ из oracle/                    │
│  • python/            - КОПИИ из python/                     │
│  • kb_billing/        - КОПИИ из kb_billing/                │
│  • streamlit_*.py     - КОПИИ из корня                      │
│  • *.sh               - скрипты управления                 │
└─────────────────────────────────────────────────────────────┘
                        │
                        │ rsync (через SSH)
                        │ (sync_and_update_kb.sh или вручную)
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              СЕРВЕР (82.114.2.2)                             │
│  /usr/local/projects/ai_report                              │
│  (НЕТ git, только файлы через rsync)                        │
├─────────────────────────────────────────────────────────────┤
│  • tabs/              - модули Streamlit                    │
│  • oracle/            - SQL скрипты                         │
│  • python/            - скрипты загрузки                    │
│  • kb_billing/        - база знаний                         │
│  • streamlit_*.py     - основные файлы                      │
│  • docker-compose.yml - (опционально; на сервере не используется) │
│  • config.env         - конфигурация (НЕ в git)            │
└─────────────────────────────────────────────────────────────┘
```

### Ключевые принципы

1. **Корневая директория - ЕДИНСТВЕННЫЙ источник истины**
   - Все изменения делаются ТОЛЬКО в корневой директории
   - Все файлы коммитятся в git и пушатся на GitHub
   - Корневая директория - это рабочая директория для разработки

2. **deploy/ - ТОЧНАЯ КОПИЯ файлов из корня**
   - `deploy/` НЕ является отдельной версией проекта
   - `deploy/` - это копия файлов из корня для синхронизации на сервер
   - Файлы в `deploy/` должны быть ИДЕНТИЧНЫ файлам в корне
   - Если файлы различаются - это ОШИБКА, нужно синхронизировать

3. **Подготовка deploy/**
   - Используется `prepare_deployment.sh` для автоматического копирования
   - ИЛИ копирование вручную: `cp файл deploy/файл`
   - После изменений в корне ВСЕГДА нужно обновить соответствующие файлы в `deploy/`

4. **Синхронизация на сервер**
   - `deploy/` синхронизируется на сервер через rsync (НЕ через git)
   - На сервере НЕТ git репозитория
   - Все файлы приходят только через rsync из `deploy/`

5. **Правило синхронизации**
   - **Корень → deploy/** (всегда)
   - **deploy/ → сервер** (через rsync)
   - **НИКОГДА не наоборот!**

---

## 📁 Структура проекта

### Локальная рабочая директория (корень проекта)

```
ai_report/
├── tabs/                      # Исходные модули Streamlit
│   ├── __init__.py
│   ├── common.py              # Общие функции (email, безопасная рассылка)
│   ├── tab_campaigns.py       # Email кампании
│   ├── tab_analytics.py       # Аналитика
│   ├── tab_report.py          # Отчеты
│   └── ...
│
├── deploy/                     # КОПИИ файлов для синхронизации на сервер
│   ├── tabs/                  # КОПИИ из tabs/ (должны быть идентичны!)
│   │   ├── __init__.py
│   │   ├── common.py          # КОПИЯ из tabs/common.py
│   │   ├── tab_campaigns.py   # КОПИЯ из tabs/tab_campaigns.py
│   │   └── ...
│   ├── oracle/                # КОПИИ из oracle/
│   ├── python/                # КОПИИ из python/
│   ├── kb_billing/            # КОПИИ из kb_billing/
│   ├── streamlit_*.py         # КОПИИ из корня
│   ├── *.sh                   # Скрипты управления
│   └── config.env.example     # Пример конфигурации
│
├── oracle/                    # SQL скрипты (исходники)
│   ├── tables/                # DDL таблиц
│   ├── views/                 # Представления (VIEW)
│   ├── functions/             # PL/SQL функции
│   └── data/                  # Справочные данные
│
├── python/                    # Python скрипты загрузки данных
│   ├── load_spnet_traffic.py
│   ├── load_steccom_expenses.py
│   └── ...
│
├── kb_billing/                # База знаний для RAG-ассистента
│   ├── tables/                # Описания таблиц (*.json)
│   ├── views/                 # Описания VIEW (*.json)
│   ├── training_data/         # Примеры SQL запросов (*.json)
│   └── rag/                   # RAG модули (Python)
│
├── streamlit_report_oracle_backup.py  # Основной файл Streamlit (ИСХОДНИК)
├── streamlit_data_loader.py           # Загрузчик данных
├── requirements.txt                   # Зависимости Python
├── prepare_deployment.sh             # Подготовка deploy/
├── sync_and_update_kb.sh            # Синхронизация + обновление KB
└── sync_and_rebuild_kb.sh           # Синхронизация + перестройка KB
```

---

## 🔄 Процесс разработки и деплоя

### 1. Локальная разработка

**ВСЕ изменения делаются ТОЛЬКО в корневой директории:**

```bash
# Редактирование модулей Streamlit
vim tabs/tab_campaigns.py
vim tabs/common.py

# Редактирование SQL скриптов
vim oracle/views/04_v_consolidated_report_with_billing.sql

# Редактирование KB файлов
vim kb_billing/tables/CUSTOMERS.json

# Редактирование основного файла Streamlit
vim streamlit_report_oracle_backup.py
```

### 2. Коммит в git

```bash
# Добавление изменений
git add tabs/tab_campaigns.py tabs/common.py streamlit_report_oracle_backup.py

# Коммит
git commit -m "Описание изменений"

# Push на GitHub (токен берётся из config.env, см. ниже)
./scripts/git_push.sh
# или вручную, если remote уже настроен с токеном:
git push origin main
```

**Токен GitHub:** в `config.env` задайте `GITHUB_TOKEN=ghp_...` (Personal Access Token). Файл `config.env` в `.gitignore`, в репозиторий не попадает. Скрипт `./scripts/git_push.sh` подставляет токен в URL и выполняет push.

### 3. Обновление deploy/ (ОБЯЗАТЕЛЬНО после изменений!)

**ВАЖНО: После любых изменений в корне нужно обновить соответствующие файлы в deploy/**

#### Вариант А: Автоматическое обновление через prepare_deployment.sh

```bash
# Подготовка deploy/ (копирует большинство файлов)
./prepare_deployment.sh

# НО: prepare_deployment.sh НЕ копирует tabs/!
# Поэтому tabs/ нужно копировать вручную:
cp -r tabs/ deploy/tabs/
```

#### Вариант Б: Ручное копирование (рекомендуется для точечных изменений)

```bash
# После изменения tabs/
cp tabs/common.py deploy/tabs/common.py
cp tabs/tab_campaigns.py deploy/tabs/tab_campaigns.py

# После изменения основного файла
cp streamlit_report_oracle_backup.py deploy/streamlit_report_oracle_backup.py

# После изменения oracle/
cp oracle/views/*.sql deploy/oracle/views/

# После изменения python/
cp python/*.py deploy/python/
```

### 4. Синхронизация на сервер

**ВАЖНО: После ЛЮБОЙ синхронизации на сервер нужно перезапустить Streamlit!**

#### Вариант А: Синхронизация KB + Streamlit (для обновления кода)

```bash
# Синхронизирует KB файлы и streamlit_report_oracle_backup.py из deploy/
SSH_CMD="ssh -p 1194" ./sync_and_update_kb.sh root@82.114.2.2

# Дополнительно синхронизировать tabs/ (если изменялись)
rsync -avz -e "ssh -p 1194" \
  deploy/tabs/ \
  root@82.114.2.2:/usr/local/projects/ai_report/tabs/

# ОБЯЗАТЕЛЬНО: Перезапуск Streamlit после синхронизации
ssh -p 1194 root@82.114.2.2 \
  "cd /usr/local/projects/ai_report && ./restart_streamlit.sh"
```

#### Вариант Б: Полная синхронизация deploy/ (для полного обновления)

```bash
# Синхронизация всей директории deploy/ на сервер
rsync -avz -e "ssh -p 1194" \
  --exclude='data/' \
  --exclude='*.log' \
  --exclude='__pycache__/' \
  deploy/ \
  root@82.114.2.2:/usr/local/projects/ai_report/

# ОБЯЗАТЕЛЬНО: Перезапуск Streamlit после синхронизации
ssh -p 1194 root@82.114.2.2 \
  "cd /usr/local/projects/ai_report && ./restart_streamlit.sh"
```

### 5. Проверка на сервере

```bash
# Проверка статуса
ssh -p 1194 root@82.114.2.2 \
  "cd /usr/local/projects/ai_report && ./status_all.sh"

# Просмотр логов (на сервере нет Docker — лог в streamlit_8504.log)
ssh -p 1194 root@82.114.2.2 \
  "cd /usr/local/projects/ai_report && tail -f streamlit_8504.log"
```

---

## 📋 Типичные сценарии

### Сценарий 1: Обновление модуля Streamlit (tabs/)

```bash
# 1. Редактирование локально (В КОРНЕ!)
vim tabs/tab_campaigns.py

# 2. Коммит в git
git add tabs/tab_campaigns.py
git commit -m "Обновление tab_campaigns"
git push origin main

# 3. ОБЯЗАТЕЛЬНО: Обновление в deploy/
cp tabs/tab_campaigns.py deploy/tabs/tab_campaigns.py

# 4. Синхронизация на сервер
rsync -avz -e "ssh -p 1194" \
  deploy/tabs/tab_campaigns.py \
  root@82.114.2.2:/usr/local/projects/ai_report/tabs/

# 5. Перезапуск (на сервере: ./restart_streamlit.sh, не docker-compose)
ssh -p 1194 root@82.114.2.2 \
  "cd /usr/local/projects/ai_report && ./restart_streamlit.sh"
```

### Сценарий 2: Обновление основного файла Streamlit

```bash
# 1. Редактирование локально (В КОРНЕ!)
vim streamlit_report_oracle_backup.py

# 2. Коммит в git
git add streamlit_report_oracle_backup.py
git commit -m "Обновление основного файла"
git push origin main

# 3. ОБЯЗАТЕЛЬНО: Обновление в deploy/
cp streamlit_report_oracle_backup.py deploy/streamlit_report_oracle_backup.py

# 4. Синхронизация на сервер
rsync -avz -e "ssh -p 1194" \
  deploy/streamlit_report_oracle_backup.py \
  root@82.114.2.2:/usr/local/projects/ai_report/

# 5. ОБЯЗАТЕЛЬНО: Перезапуск Streamlit после синхронизации
ssh -p 1194 root@82.114.2.2 \
  "cd /usr/local/projects/ai_report && ./restart_streamlit.sh"
```

### Сценарий 3: Обновление Oracle VIEW

```bash
# 1. Редактирование локально (В КОРНЕ!)
vim oracle/views/04_v_consolidated_report_with_billing.sql

# 2. Коммит в git
git add oracle/views/04_v_consolidated_report_with_billing.sql
git commit -m "Обновление VIEW"
git push origin main

# 3. ОБЯЗАТЕЛЬНО: Обновление в deploy/
cp oracle/views/04_v_consolidated_report_with_billing.sql \
   deploy/oracle/views/

# 4. Синхронизация на сервер
rsync -avz -e "ssh -p 1194" \
  deploy/oracle/views/04_v_consolidated_report_with_billing.sql \
  root@82.114.2.2:/usr/local/projects/ai_report/oracle/views/

# 5. Применение VIEW на сервере
ssh -p 1194 root@82.114.2.2 \
  "source /usr/local/projects/ai_report/config.env && \
   cd /usr/local/projects/ai_report/oracle/views && \
   sqlplus -S \${ORACLE_USER}/\${ORACLE_PASSWORD}@\${ORACLE_SERVICE} \
   @04_v_consolidated_report_with_billing.sql"

# 6. ОБЯЗАТЕЛЬНО: Перезапуск Streamlit после синхронизации Oracle
ssh -p 1194 root@82.114.2.2 \
  "cd /usr/local/projects/ai_report && ./restart_streamlit.sh"
```

### Сценарий 4: Обновление базы знаний (KB)

```bash
# 1. Редактирование локально (В КОРНЕ!)
vim kb_billing/tables/CUSTOMERS.json

# 2. Коммит в git
git add kb_billing/tables/CUSTOMERS.json
git commit -m "Обновление KB"
git push origin main

# 3. prepare_deployment.sh копирует kb_billing/ автоматически
# ИЛИ копируем вручную:
cp kb_billing/tables/CUSTOMERS.json deploy/kb_billing/tables/

# 4. Синхронизация + автоматическое обновление KB в Qdrant
SSH_CMD="ssh -p 1194" ./sync_and_update_kb.sh root@82.114.2.2
```

---

## 🚨 КРИТИЧЕСКИ ВАЖНО

### Правило синхронизации

**ВСЕГДА: Корень → deploy/ → Сервер → Перезапуск Streamlit**

1. **Корневая директория** - ЕДИНСТВЕННЫЙ источник истины
2. **deploy/** - ТОЧНАЯ КОПИЯ файлов из корня
3. **Сервер** - копия из deploy/
4. **После ЛЮБОЙ синхронизации на сервер ОБЯЗАТЕЛЬНО перезапустить Streamlit!**

**⚠️ НАПОМИНАНИЕ: Используй `./restart_streamlit.sh`, а НЕ `docker-compose restart streamlit`!**

### Правило перезапуска

**⚠️ ВАЖНО: На сервере используется скрипт `restart_streamlit.sh`, а НЕ `docker-compose restart streamlit`!**

**После синхронизации Oracle (VIEW, функции, таблицы) или кода Python (tabs/, streamlit_*.py) на сервер:**

```bash
# ОБЯЗАТЕЛЬНО перезапустить Streamlit правильной командой:
ssh -p 1194 root@82.114.2.2 \
  "cd /usr/local/projects/ai_report && ./restart_streamlit.sh"
```

**Что делает `restart_streamlit.sh`:**
- Проверяет текущий статус Streamlit (PID файл)
- Корректно останавливает процесс (SIGTERM, затем SIGKILL при необходимости)
- Очищает кэш Streamlit (`~/.streamlit/cache` и локальный кэш)
- Запускает Streamlit через `run_streamlit_background.sh`
- Проверяет успешность запуска

**Почему НЕ используется `docker-compose restart streamlit`:**
- На сервере Streamlit запущен как обычный процесс через systemd/nohup, а не через Docker
- Скрипт `restart_streamlit.sh` специально настроен для текущей конфигурации сервера
- Скрипт выполняет дополнительную очистку кэша, что важно при обновлении кода

**На сервере Docker Compose не используется.** Перезапуск только через `./restart_streamlit.sh`. Если в будущем конфигурация изменится (systemd/docker-compose), команды перезапуска нужно будет обновить.

**Проверка статуса после перезапуска:**
```bash
# Проверка логов
ssh -p 1194 root@82.114.2.2 \
  "cd /usr/local/projects/ai_report && tail -f streamlit_8504.log"

# Проверка статуса
ssh -p 1194 root@82.114.2.2 \
  "cd /usr/local/projects/ai_report && ./status_streamlit.sh"
```

**Почему это важно:**
- Streamlit загружает модули Python при старте
- Изменения в коде не применяются без перезапуска
- Изменения в Oracle VIEW не видны без перезапуска (если используются в коде)

### Проверка синхронизации

**Если файлы в корне и deploy/ различаются - это ОШИБКА!**

```bash
# Проверка различий
diff streamlit_report_oracle_backup.py deploy/streamlit_report_oracle_backup.py
diff tabs/tab_campaigns.py deploy/tabs/tab_campaigns.py

# Если есть различия - синхронизировать:
cp streamlit_report_oracle_backup.py deploy/streamlit_report_oracle_backup.py
cp tabs/tab_campaigns.py deploy/tabs/tab_campaigns.py
```

### Что НЕ делать

❌ **НЕ редактировать файлы напрямую в deploy/** - они будут перезаписаны  
❌ **НЕ делать git pull на сервере** - на сервере нет git  
❌ **НЕ синхронизировать сервер → deploy/** - только корень → deploy/ → сервер  
❌ **НЕ забывать обновлять deploy/** после изменений в корне

---

## 🔧 Важные замечания

### prepare_deployment.sh

**Что копирует автоматически:**
- ✅ `oracle/` → `deploy/oracle/`
- ✅ `python/` → `deploy/python/`
- ✅ `kb_billing/` → `deploy/kb_billing/`
- ✅ `streamlit_*.py` → `deploy/`
- ✅ `requirements.txt` → `deploy/`

**Что НЕ копирует:**
- ❌ `tabs/` → `deploy/tabs/` - нужно копировать вручную!

**Поэтому после `prepare_deployment.sh` всегда нужно:**
```bash
cp -r tabs/ deploy/tabs/
```

### Спутниковый ассистент (Gemini): Python 3.9+

Вкладка «Спутниковый ассистент» → «Ответы (инженер/абонент)» использует пакет **google-genai**, который требует **Python >= 3.9**. На сервере с системным Python 3.8 `pip install google-genai` выдаст «No matching distribution found». Решение: запускать Streamlit в окружении с Python 3.9+ (например `conda activate py11`, или `python3.9 -m venv venv && source venv/bin/activate`, затем `pip install -r requirements.txt` и запуск через этот интерпретатор). Если Python < 3.9, вкладка покажет предупреждение и не будет вызывать Gemini.

### sync_and_update_kb.sh

**Что синхронизирует:**
- ✅ `deploy/streamlit_report_oracle_backup.py` → сервер
- ✅ `kb_billing/rag/*.py` → сервер (код RAG, без удаления на сервере лишнего)
- ✅ `kb_billing/tables/*.json` → сервер (без --delete: файлы, добавленные на сервере, не удаляются)
- ✅ `kb_billing/views/*.json` → сервер (без --delete)
- ✅ `kb_billing/training_data/*.json` → сервер
- ✅ На сервере после синхронизации запускается обновление KB в Qdrant

**Что НЕ синхронизирует (и не перезаписывает на сервере):**
- ❌ **`kb_billing/confluence_docs/`** — не синхронизируется с локального на сервер. Документы Confluence пополняются **на сервере** через интерфейс «Спутниковый библиотекарь» (синхронизация из Confluence → сохранение в `confluence_docs/*.json` на сервере). Локальная «синхронизация KB» их не трогает.
- ❌ `deploy/tabs/` — нужно синхронизировать отдельно через rsync
- ❌ `deploy/oracle/` — нужно синхронизировать отдельно через rsync
- ❌ `deploy/python/` — нужно синхронизировать отдельно через rsync

**Смысл синхронизации KB:** обновить на сервере **код** RAG (`rag/*.py`), при необходимости — таблицы/примеры из корня (tables, views, training_data). Контент, который создаётся на сервере (Confluence-документы в `confluence_docs/`), не перезаписывается.

**Как устроена KB для биллинга и присэйлов:** одна коллекция Qdrant `kb_billing` (имя из `QDRANT_COLLECTION`): в ней точки для биллинга (qa_example, documentation, ddl, view) и для присэйлов (confluence_section, domain=satellite). KB для присэйлов формируется из Confluence через Спутникового библиотекаря → `confluence_docs/*.json` → загрузка в ту же коллекцию. Подробнее: [docs/kb-billing-vs-presales.md](docs/kb-billing-vs-presales.md).

### Deploy: синхронизация, перестройка KB (Qdrant), перезапуск

**Используй при каждой синхронизации на сервер.** Сервер: `root@82.114.2.2`, порт SSH **1194**, путь на сервере: `/usr/local/projects/ai_report`. URL приложения: **stat.steccom.ru:7776/ai_report**.

1. **Синхронизация deploy/ на сервер**
   ```bash
   # Корень клона на вашей машине (пример: /media/cnn/home/cnn/ai_report — путь не зашит в коде проекта)
   cd /media/cnn/home/cnn/ai_report
   rsync -avz -e "ssh -p 1194" --exclude='data/' --exclude='*.log' --exclude='__pycache__/' \
     deploy/ root@82.114.2.2:/usr/local/projects/ai_report/
   ```

2. **Перестройка KB в Qdrant** (на сервере после синхронизации)
   - **Полная перестройка** (таблицы, представления, метаданные, Confluence, примеры) — при изменении `tables/*.json`, `views/*.json`, `metadata.json`, Confluence-документов или при первом развёртывании:
     ```bash
     ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && (test -f config.env && set -a && . ./config.env && set +a); python3 kb_billing/rag/init_kb.py"
     ```
   - **Только примеры** (sql_examples.json, user_added_examples.json) — без пересчёта таблиц/представлений/Confluence; быстрее, когда меняли только примеры:
     ```bash
     ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && python3 kb_billing/rag/init_kb.py --only-examples"
     ```
   После `--only-examples` перезапуск Streamlit не обязателен (KB подхватывается при запросе). После полной перестройки перезапуск желателен.

3. **Перезапуск веб-интерфейса**
   ```bash
   ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && ./restart_streamlit.sh"
   ```

**Типовой порядок при работе с deploy:** синхронизация → при изменении KB (таблицы/примеры/Confluence) — полная перестройка или `--only-examples` → перезапуск Streamlit. Подробнее: [docs/deploy.md](docs/deploy.md).

### Чистый перенос kb_billing на сервер (только нужное из deploy)

Чтобы на сервере в `/usr/local/projects/ai_report/kb_billing` оставались только необходимые файлы из «чистого» deploy (без лишнего мусора и без перезаписи данных, созданных на сервере):

**Скрипт:** `scripts/deploy_kb_billing_clean.sh`

Переносит на сервер **только** содержимое из `deploy/kb_billing`:

- **rag/** — код RAG (полная замена, с `--delete`: на сервере удаляется то, чего нет в deploy)
- **tables/**, **views/**, **training_data/** — полная замена по содержимому deploy
- **Корень** — `*.json`, `*.md` (metadata.json, SUMMARY.md и т.д.)

**На сервере не трогается:** `confluence_docs/` (и всё, что там лежит — выгрузки Confluence, outdated.txt и т.п.).

После переноса скрипт удаляет на сервере в `kb_billing` каталоги `__pycache__` и файлы `*.pyc`.

```bash
# 1. Обновить deploy из корня (если ещё не обновлён)
./prepare_deployment.sh

# 2. Чистый перенос kb_billing на сервер
SSH_CMD="ssh -p 1194" ./scripts/deploy_kb_billing_clean.sh root@82.114.2.2

# 3. Перезапуск приложения (при необходимости)
ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && ./restart_streamlit.sh"
```

Так на сервере в `kb_billing` остаётся только нужный софт из deploy, а `confluence_docs/` и прочие серверные данные не затираются.

---

## 💾 Бэкап KB на QNAP

Базу знаний (в т.ч. `confluence_docs/`, tables, views, training_data) можно регулярно сохранять в архив на внешнем массиве, например **/qnap/kb**.

**Скрипт:** `scripts/backup_kb_to_qnap.sh`

**Варианты запуска:**

1. **На сервере** (где лежит KB), если QNAP смонтирован на сервере в `/qnap/kb`:
   ```bash
   /usr/local/projects/ai_report/scripts/backup_kb_to_qnap.sh
   ```
   Или из репозитория после синхронизации:
   ```bash
   cd /usr/local/projects/ai_report
   BACKUP_ROOT=/qnap/kb ./scripts/backup_kb_to_qnap.sh
   ```

2. **С другой машины** (где смонтирован `/qnap/kb`), бэкап с сервера по SSH:
   ```bash
   REMOTE_SERVER=root@82.114.2.2 SSH_CMD="ssh -p 1194" BACKUP_ROOT=/qnap/kb ./scripts/backup_kb_to_qnap.sh
   ```

**Переменные (опционально):**

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `BACKUP_ROOT` | `/qnap/kb` | Каталог на QNAP для бэкапов |
| `USE_DATE` | `1` | Подкаталог с датой (например `2025-03-04`); `0` — всё в `latest` |
| `KEEP_DAYS` | `30` | Хранить последние N дней; старые каталоги удаляются; `0` — не удалять |
| `REMOTE_SERVER` | — | Если задан — бэкап с сервера по SSH (см. вариант 2) |
| `SSH_CMD` | `ssh` | Команда SSH (например `ssh -p 1194`) |

**Регулярный бэкап по cron** (на сервере, если там смонтирован /qnap/kb):

```bash
# Ежедневно в 03:00 — бэкап KB в /qnap/kb/YYYY-MM-DD
0 3 * * * /usr/local/projects/ai_report/scripts/backup_kb_to_qnap.sh >> /var/log/ai_report_kb_backup.log 2>&1
```

Или с другой машины (где смонтирован QNAP и есть SSH-доступ к серверу):

```bash
0 3 * * * cd /path/to/ai_report && REMOTE_SERVER=root@82.114.2.2 SSH_CMD="ssh -p 1194" ./scripts/backup_kb_to_qnap.sh >> /var/log/ai_report_kb_backup.log 2>&1
```

В архив попадает вся директория `kb_billing/` (confluence_docs, tables, views, training_data, metadata, rag). Векторная БД Qdrant в этот скрипт не входит; при необходимости её бэкап настраивают отдельно (снимки Qdrant или копирование тома данных).

Если на сервере нет каталога `scripts/`, скопируйте скрипты один раз:  
`scp -P 1194 scripts/backup_kb_to_qnap.sh scripts/restore_kb_from_qnap.sh root@82.114.2.2:/usr/local/projects/ai_report/scripts/`  
и на сервере: `chmod +x /usr/local/projects/ai_report/scripts/*.sh`.

### Восстановление KB из архива

**Скрипт:** `scripts/restore_kb_from_qnap.sh`

Восстановление из каталога бэкапа (например `/qnap/kb/2025-03-04` или `latest`) в текущий проект (локально или на сервер).

```bash
# Показать доступные бэкапы и выйти (без восстановления)
./scripts/restore_kb_from_qnap.sh

# Восстановить из бэкапа за указанную дату (запросит подтверждение)
./scripts/restore_kb_from_qnap.sh 2025-03-04

# Восстановить из latest без запроса подтверждения
FORCE=1 ./scripts/restore_kb_from_qnap.sh latest
# или
./scripts/restore_kb_from_qnap.sh --yes latest
```

**Восстановление на сервер с машины, где смонтирован QNAP:**

```bash
REMOTE_SERVER=root@82.114.2.2 SSH_CMD="ssh -p 1194" ./scripts/restore_kb_from_qnap.sh 2025-03-04
```

После восстановления нужно перезагрузить KB в Qdrant (кнопка «Перезагрузить KB в Qdrant» в интерфейсе Спутникового библиотекаря или запуск `init_kb.py` на сервере).

---

## 📚 Дополнительная документация

- **[docs/deploy.md](docs/deploy.md)** — Развёртывание и поддержка (схема, синхронизация, перезапуск)

---

## 🎯 Краткая шпаргалка

```bash
# 1. Изменения локально (В КОРНЕ!)
vim tabs/tab_campaigns.py

# 2. Коммит в git
git add tabs/tab_campaigns.py
git commit -m "Описание изменений"
git push origin main

# 3. ОБЯЗАТЕЛЬНО: Обновление deploy/
cp tabs/tab_campaigns.py deploy/tabs/tab_campaigns.py

# 4. Синхронизация на сервер
rsync -avz -e "ssh -p 1194" \
  deploy/tabs/tab_campaigns.py \
  root@82.114.2.2:/usr/local/projects/ai_report/tabs/

# 5. ОБЯЗАТЕЛЬНО: Перезапуск Streamlit после синхронизации
ssh -p 1194 root@82.114.2.2 \
  "cd /usr/local/projects/ai_report && ./restart_streamlit.sh"
```

**Помните: Корень → deploy/ → Сервер → Перезапуск Streamlit (./restart_streamlit.sh на сервере, не docker-compose!)**
