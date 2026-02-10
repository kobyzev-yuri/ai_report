# 🎯 Визуальное руководство по развертыванию ai_report

## 📊 Архитектура проекта

```
┌─────────────────────────────────────────────────────────────────┐
│                     ai_report System                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │   Streamlit  │◄───│    Qdrant    │◄───│    Oracle    │      │
│  │  Web UI      │    │  Vector DB   │    │   Database   │      │
│  │  (Port 8504) │    │  (Port 6333) │    │  (billing)   │      │
│  └──────────────┘    └──────────────┘    └──────────────┘      │
│         │                    │                    │              │
│         │                    │                    │              │
│         ▼                    ▼                    ▼              │
│  ┌──────────────────────────────────────────────────────┐      │
│  │              RAG Assistant (GPT-4o)                   │      │
│  │  • SQL Generation from Natural Language              │      │
│  │  • Knowledge Base Search                             │      │
│  │  • Financial Analysis                                │      │
│  └──────────────────────────────────────────────────────┘      │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘

Сервер: 82.114.2.2:1194
Директория: /usr/local/projects/ai_report
Доступ: http://stat.steccom.ru:7776/ai_report
```

---

## 🚀 Методы развертывания (Визуальная схема)

```
                    ┌─────────────────────────┐
                    │     ПОДГОТОВКА          │
                    ├─────────────────────────┤
                    │ • check_prerequisites   │
                    │ • server_inspection     │
                    │ • Резервное копирование │
                    └───────────┬─────────────┘
                                │
                ┌───────────────┴───────────────┐
                │                               │
                ▼                               ▼
    ┌───────────────────────┐       ┌───────────────────────┐
    │  ПОЛНОЕ РАЗВЕРТЫВАНИЕ │       │  ЧАСТИЧНОЕ ОБНОВЛЕНИЕ │
    └───────────┬───────────┘       └───────────┬───────────┘
                │                               │
    ┌───────────┼───────────┐       ┌───────────┼───────────┐
    │           │           │       │           │           │
    ▼           ▼           ▼       ▼           ▼           ▼
┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐
│ Safe   │ │ Docker │ │ Manual │ │  Sync  │ │   KB   │ │ Oracle │
│ Deploy │ │Compose │ │ Deploy │ │  Files │ │ Update │ │  VIEW  │
├────────┤ ├────────┤ ├────────┤ ├────────┤ ├────────┤ ├────────┤
│ 5 min  │ │ 3 min  │ │ 10 min │ │ 1 min  │ │ 2 min  │ │ 2 min  │
│ ⭐⭐   │ │ ⭐⭐⭐ │ │ ⭐⭐⭐⭐│ │ ⭐⭐   │ │ ⭐     │ │ ⭐⭐   │
│ ⭐⭐⭐⭐⭐│ │ ⭐⭐⭐⭐│ │ ⭐⭐   │ │ ⭐⭐⭐ │ │ ⭐⭐⭐⭐│ │ ⭐⭐⭐ │
└────────┘ └────────┘ └────────┘ └────────┘ └────────┘ └────────┘
    │           │           │       │           │           │
    └───────────┴───────────┴───────┴───────────┴───────────┘
                                │
                                ▼
                    ┌─────────────────────────┐
                    │  ПОСТ-РАЗВЕРТЫВАНИЕ     │
                    ├─────────────────────────┤
                    │ • Health checks         │
                    │ • Status verification   │
                    │ • Log monitoring        │
                    └─────────────────────────┘
```

---

## 📋 Матрица выбора метода

```
┌──────────────────────────────────────────────────────────────────┐
│                    КОГДА ИСПОЛЬЗОВАТЬ?                            │
├──────────────────────────────────────────────────────────────────┤
│                                                                    │
│  Первое развертывание          →  safe_deploy.sh ⭐              │
│  Production окружение          →  Docker Compose                 │
│  Обновление кода               →  sync_deploy.sh                 │
│  Обновление примеров SQL       →  sync_and_update_kb.sh          │
│  Обновление Oracle VIEW        →  sync + sqlplus                 │
│  Отладка и тестирование        →  Manual Deploy                  │
│  Полная перестройка KB         →  sync_and_rebuild_kb.sh         │
│                                                                    │
└──────────────────────────────────────────────────────────────────┘
```

---

## 🎯 Быстрые команды

### 🟢 Первое развертывание
```bash
cd deploy
SSH_CMD="ssh -p 1194" ./safe_deploy.sh root@82.114.2.2
```

### 🔵 Обновление кода
```bash
cd deploy
SSH_CMD="ssh -p 1194" ./sync_deploy.sh root@82.114.2.2
ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && docker-compose restart streamlit"
```

### 🟣 Обновление базы знаний
```bash
SSH_CMD="ssh -p 1194" ./sync_and_update_kb.sh root@82.114.2.2
```

### 🟠 Обновление Oracle VIEW
```bash
# 1. Синхронизация
cd deploy
SSH_CMD="ssh -p 1194" ./sync_deploy.sh root@82.114.2.2

# 2. Применение VIEW
ssh -p 1194 root@82.114.2.2 "source /usr/local/projects/ai_report/config.env && \
  cd /usr/local/projects/ai_report/oracle/views && \
  sqlplus \${ORACLE_USER}/\${ORACLE_PASSWORD}@\${ORACLE_HOST}:\${ORACLE_PORT}/\${ORACLE_SERVICE} \
  @04_v_consolidated_report_with_billing.sql"
```

### 🟡 Проверка статуса
```bash
ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && ./status_all.sh"
```

### 🔴 Просмотр логов
```bash
ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && docker-compose logs -f streamlit"
```

---

## 📊 Сравнительная таблица методов

```
┌─────────────────┬──────┬───────────┬────────────┬────────────┬─────────────────────┐
│ Метод           │ Время│ Сложность │ Автоматизац│ Безопасност│ Лучше всего для     │
│                 │      │           │ ия         │ ь          │                     │
├─────────────────┼──────┼───────────┼────────────┼────────────┼─────────────────────┤
│ safe_deploy.sh  │ 5мин │ ⭐⭐      │ ⭐⭐⭐⭐⭐  │ ⭐⭐⭐⭐⭐  │ Первое развертывание│
│ ⭐ РЕКОМЕНДУЕТСЯ│      │           │            │            │ Полное обновление   │
├─────────────────┼──────┼───────────┼────────────┼────────────┼─────────────────────┤
│ Docker Compose  │ 3мин │ ⭐⭐⭐    │ ⭐⭐⭐⭐    │ ⭐⭐⭐⭐    │ Production          │
│                 │      │           │            │            │                     │
├─────────────────┼──────┼───────────┼────────────┼────────────┼─────────────────────┤
│ Manual Deploy   │ 10мин│ ⭐⭐⭐⭐  │ ⭐⭐        │ ⭐⭐⭐      │ Отладка             │
│                 │      │           │            │            │ Тестирование        │
├─────────────────┼──────┼───────────┼────────────┼────────────┼─────────────────────┤
│ sync_deploy.sh  │ 1мин │ ⭐⭐      │ ⭐⭐⭐      │ ⭐⭐⭐⭐    │ Обновление кода     │
│                 │      │           │            │            │                     │
├─────────────────┼──────┼───────────┼────────────┼────────────┼─────────────────────┤
│ KB Update       │ 2мин │ ⭐        │ ⭐⭐⭐⭐    │ ⭐⭐⭐⭐⭐  │ Изменения KB        │
│                 │      │           │            │            │ Примеры SQL         │
└─────────────────┴──────┴───────────┴────────────┴────────────┴─────────────────────┘
```

---

## 🔄 Типичные workflow

### Workflow 1: Разработка новой функции
```
1. Локальная разработка
   ├─ Редактирование кода
   └─ Тестирование

2. Синхронизация на сервер
   └─ SSH_CMD="ssh -p 1194" ./sync_deploy.sh root@82.114.2.2

3. Перезапуск сервиса
   └─ ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && docker-compose restart streamlit"

4. Проверка
   ├─ Логи: docker-compose logs -f streamlit
   └─ Веб-интерфейс: http://stat.steccom.ru:7776/ai_report
```

### Workflow 2: Добавление примеров SQL
```
1. Редактирование локально
   └─ vim kb_billing/training_data/sql_examples.json

2. Синхронизация + обновление KB
   └─ SSH_CMD="ssh -p 1194" ./sync_and_update_kb.sh root@82.114.2.2

3. Проверка в RAG-ассистенте
   └─ Открыть вкладку "🤖 Ассистент"
   └─ Проверить поиск примеров
```

### Workflow 3: Обновление Oracle VIEW
```
1. Редактирование VIEW
   └─ vim oracle/views/04_v_consolidated_report_with_billing.sql

2. Синхронизация
   └─ SSH_CMD="ssh -p 1194" ./sync_deploy.sh root@82.114.2.2

3. Применение VIEW
   └─ ssh -p 1194 root@82.114.2.2 "source /usr/local/projects/ai_report/config.env && \
      cd /usr/local/projects/ai_report/oracle/views && \
      sqlplus \${ORACLE_USER}/\${ORACLE_PASSWORD}@\${ORACLE_HOST}:\${ORACLE_PORT}/\${ORACLE_SERVICE} \
      @04_v_consolidated_report_with_billing.sql"

4. Проверка в Streamlit
   └─ Обновить страницу
   └─ Проверить данные
```

---

## 🛠️ Диагностика и мониторинг

### Проверка здоровья системы
```bash
# Все сервисы
ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && ./status_all.sh"

# Qdrant
ssh -p 1194 root@82.114.2.2 "curl http://localhost:6333/health"

# Streamlit
ssh -p 1194 root@82.114.2.2 "curl http://localhost:8504/_stcore/health"

# KB коллекция
ssh -p 1194 root@82.114.2.2 "curl http://localhost:6333/collections/kb_billing | jq '.result.points_count'"
```

### Просмотр логов
```bash
# Streamlit (Docker)
ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && docker-compose logs -f streamlit"

# Qdrant
ssh -p 1194 root@82.114.2.2 "docker logs -f ai_report_qdrant"

# Все контейнеры
ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && docker-compose logs -f"
```

### Перезапуск сервисов
```bash
# Streamlit
ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && docker-compose restart streamlit"

# Qdrant
ssh -p 1194 root@82.114.2.2 "docker restart ai_report_qdrant"

# Все сервисы
ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && docker-compose restart"
```

---

## 🔐 Безопасность и бэкапы

### Перед развертыванием
```bash
# 1. Резервное копирование
ssh -p 1194 root@82.114.2.2 "tar czf /tmp/ai_report_backup_$(date +%Y%m%d_%H%M%S).tar.gz /usr/local/projects/ai_report"

# 2. Проверка config.env
ssh -p 1194 root@82.114.2.2 "cat /usr/local/projects/ai_report/config.env | grep -E '^ORACLE_|^QDRANT_'"

# 3. Проверка свободного места
ssh -p 1194 root@82.114.2.2 "df -h /usr/local/projects"
```

### Защита конфигурации
```bash
# Локально
chmod 600 config.env

# На сервере
ssh -p 1194 root@82.114.2.2 "chmod 600 /usr/local/projects/ai_report/config.env"
```

### Бэкап Qdrant
```bash
# Создание бэкапа
ssh -p 1194 root@82.114.2.2 "docker exec ai_report_qdrant tar czf /qdrant/storage/backup_$(date +%Y%m%d).tar.gz /qdrant/storage"

# Копирование бэкапа
ssh -p 1194 root@82.114.2.2 "docker cp ai_report_qdrant:/qdrant/storage/backup_$(date +%Y%m%d).tar.gz /tmp/"
scp -P 1194 root@82.114.2.2:/tmp/backup_$(date +%Y%m%d).tar.gz ./
```

---

## 📁 Структура файлов (визуальная)

```
ai_report/
│
├── 📂 deploy/                       ← Директория развертывания
│   ├── 🚀 safe_deploy.sh           ← ⭐ Безопасное развертывание
│   ├── 🐳 deploy.sh                ← Docker/ручное развертывание
│   ├── 🔄 sync_deploy.sh           ← Синхронизация файлов
│   ├── 🔍 server_inspection.sh     ← Обследование сервера
│   ├── 📊 status_all.sh            ← Статус всех сервисов
│   ├── 🛑 stop_all.sh              ← Остановка всех сервисов
│   ├── 🗄️ init_kb.sh               ← Инициализация KB
│   ├── 🔄 update_kb.sh             ← Обновление KB
│   ├── 🐳 docker-compose.yml       ← Docker Compose конфигурация
│   ├── 🐳 Dockerfile.streamlit     ← Dockerfile для Streamlit
│   └── 📖 DEPLOYMENT_*.md          ← Документация
│
├── 🔄 sync_and_update_kb.sh        ← Синхронизация + обновление KB
├── 🔄 sync_and_rebuild_kb.sh       ← Синхронизация + перестройка KB
├── 📦 prepare_deployment.sh        ← Подготовка минимального пакета
│
├── 📂 oracle/                       ← Oracle скрипты
│   ├── 📂 tables/                  ← DDL таблиц
│   ├── 📂 views/                   ← Представления (VIEW)
│   ├── 📂 functions/               ← PL/SQL функции
│   └── 📂 data/                    ← Справочные данные
│
├── 📂 kb_billing/                   ← База знаний (KB)
│   ├── 📂 tables/                  ← Описания таблиц (JSON)
│   ├── 📂 views/                   ← Описания VIEW (JSON)
│   ├── 📂 training_data/           ← Примеры SQL запросов
│   └── 📂 rag/                     ← RAG модули (Python)
│
├── 📂 python/                       ← Python скрипты
│   ├── 📊 load_spnet_traffic.py    ← Загрузка данных SPNet
│   └── 📊 load_steccom_expenses.py ← Загрузка данных STECCOM
│
├── 🌐 streamlit_report_oracle_backup.py  ← Streamlit приложение
├── 🔌 db_connection.py             ← Подключение к Oracle
├── 🔐 auth_db.py                   ← Аутентификация
│
├── ▶️ run_streamlit_background.sh  ← Запуск Streamlit
├── ⏹️ stop_streamlit.sh            ← Остановка Streamlit
├── 🔄 restart_streamlit.sh         ← Перезапуск Streamlit
├── 📊 status_streamlit.sh          ← Статус Streamlit
│
├── 📖 README.md                    ← Описание проекта
├── 📖 cursor.md                    ← ⭐ Инструкция для Cursor IDE
├── 📖 DEPLOYMENT_METHODS_ANALYSIS.md  ← Детальный анализ методов
└── 📖 DEPLOYMENT_SUMMARY_RU.md     ← Краткая сводка (RU)
```

---

## ✅ Чеклист развертывания

### Перед развертыванием
- [ ] Резервное копирование создано
- [ ] config.env проверен
- [ ] SSH доступ работает
- [ ] Свободное место проверено
- [ ] Docker и Docker Compose установлены

### Во время развертывания
- [ ] Старый Streamlit остановлен
- [ ] Порты 8504 и 6333 свободны
- [ ] Файлы синхронизированы
- [ ] Контейнеры запущены
- [ ] KB инициализирована

### После развертывания
- [ ] Qdrant доступен (порт 6333)
- [ ] Streamlit доступен (порт 8504)
- [ ] KB коллекция создана
- [ ] Ассистент работает
- [ ] Поиск примеров работает
- [ ] SQL запросы выполняются
- [ ] Логи без ошибок
- [ ] Nginx proxy работает

---

## 🌐 Доступ к приложению

```
┌─────────────────────────────────────────────────────┐
│              ДОСТУП К ПРИЛОЖЕНИЮ                     │
├─────────────────────────────────────────────────────┤
│                                                       │
│  Прямой доступ:                                      │
│  http://82.114.2.2:8504                             │
│                                                       │
│  Через Nginx proxy:                                  │
│  http://stat.steccom.ru:7776/ai_report              │
│                                                       │
│  Основные вкладки:                                   │
│  • 🤖 Ассистент - RAG-ассистент для SQL             │
│  • 📈 Финансовый анализ - анализ прибыльности       │
│  • 📊 Главный отчет - сводная таблица               │
│  • 📁 Data Loader - загрузка данных                 │
│                                                       │
└─────────────────────────────────────────────────────┘
```

---

## 📚 Документация

### Основная документация
- **README.md** - Общее описание проекта
- **cursor.md** - Работа в Cursor IDE
- **DEPLOYMENT_METHODS_ANALYSIS.md** - Детальный анализ методов
- **DEPLOYMENT_SUMMARY_RU.md** - Краткая сводка

### Документация в deploy/
- **START_HERE.md** - Начните отсюда
- **DEPLOYMENT_SERVER.md** - Развертывание на сервере
- **DEPLOYMENT_RAG.md** - RAG система
- **DEPLOYMENT_PLAN.md** - План развертывания
- **QUICK_DEPLOY.md** - Быстрый старт

---

## 💡 Рекомендации

```
┌─────────────────────────────────────────────────────────────┐
│                    РЕКОМЕНДАЦИИ                              │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ✅ Для первого развертывания:                               │
│     → safe_deploy.sh                                         │
│                                                               │
│  ✅ Для обновлений кода:                                     │
│     → sync_deploy.sh + docker-compose restart                │
│                                                               │
│  ✅ Для обновления KB:                                       │
│     → sync_and_update_kb.sh                                  │
│                                                               │
│  ✅ Для отладки:                                             │
│     → Manual Deploy                                          │
│                                                               │
│  ✅ Всегда делайте резервные копии перед развертыванием!     │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

**Проект:** Iridium M2M Reporting  
**Технологии:** Oracle, Python, Streamlit, Qdrant, Docker  
**Дата:** Октябрь 2025  
**Разработано в:** Cursor IDE
