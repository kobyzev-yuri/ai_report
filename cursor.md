# Инструкция по работе с проектом ai_report

## Синхронизация и деплой

### Структура проекта

- `oracle/views/` - исходные SQL файлы представлений (VIEW)
- `oracle/functions/` - функции PL/SQL
- `python/` - Python скрипты для загрузки данных
- `kb_billing/` - база знаний (KB) для RAG-ассистента
  - `tables/*.json` - описания таблиц
  - `views/*.json` - описания представлений
  - `training_data/*.json` - примеры SQL запросов
- `deploy/` - директория для деплоя
  - `sync_deploy.sh` - скрипт полной синхронизации (VIEW, скрипты, KB)
  - `update_kb.sh` - скрипт обновления KB на сервере

### Процесс полной синхронизации (рекомендуется)

**Вариант 1: Полная синхронизация с автоматическим обновлением KB (рекомендуется)**

Используйте скрипт `sync_and_update_kb.sh` для синхронизации всех изменений и автоматического обновления KB:

```bash
# Из корня проекта
SSH_CMD="ssh -p 1194" ./sync_and_update_kb.sh root@82.114.2.2
```

Этот скрипт:
1. ✅ Синхронизирует все Oracle VIEW из `oracle/views/`
2. ✅ Синхронизирует все Python скрипты из `python/`
3. ✅ Синхронизирует все KB файлы (таблицы, VIEW, примеры)
4. ✅ Автоматически обновляет KB в Qdrant на сервере

**Вариант 2: Только синхронизация файлов (без обновления KB)**

Если нужно только синхронизировать файлы без обновления KB:

```bash
cd deploy
SSH_CMD="ssh -p 1194" ./sync_deploy.sh root@82.114.2.2
```

Затем на сервере вручную обновите KB:
```bash
ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && ./update_kb.sh"
```

### Процесс обновления отдельных компонентов

#### 1. Обновление Oracle VIEW

1. **Редактировать исходный файл:**
   ```bash
   vim oracle/views/04_v_consolidated_report_with_billing.sql
   ```

2. **Синхронизировать на сервер:**
   ```bash
   cd deploy
   SSH_CMD="ssh -p 1194" ./sync_deploy.sh root@82.114.2.2
   ```
   
   Или использовать полную синхронизацию:
   ```bash
   SSH_CMD="ssh -p 1194" ./sync_and_update_kb.sh root@82.114.2.2
   ```

3. **Применить VIEW на сервере:**
   
   **Вариант 1: Через SSH (рекомендуется)**
   ```bash
   ssh -p 1194 root@82.114.2.2 "source /usr/local/projects/ai_report/config.env 2>/dev/null; cd /usr/local/projects/ai_report/oracle/views && timeout 60 sqlplus -S \${ORACLE_USER}/\${ORACLE_PASSWORD}@\${ORACLE_HOST}:\${ORACLE_PORT}/\${ORACLE_SERVICE:-bm7} @04_v_consolidated_report_with_billing.sql 2>&1 | tail -10"
   ```
   
   **Вариант 2: Локально через sqlplus (через SSH туннель)**
   ```bash
   # Запустить туннель
   ./scripts/local/oracle_tunnel.sh start
   
   # Применить представление
   sqlplus billing7/billing@localhost:1521/bm7 @oracle/views/04_v_consolidated_report_with_billing.sql
   
   # Остановить туннель (если нужно)
   ./scripts/local/oracle_tunnel.sh stop
   ```

#### 2. Обновление Python скриптов

1. **Редактировать скрипт:**
   ```bash
   vim python/load_spnet_traffic.py
   ```

2. **Синхронизировать:**
   ```bash
   SSH_CMD="ssh -p 1194" ./sync_and_update_kb.sh root@82.114.2.2
   ```
   
   Скрипты автоматически синхронизируются в `/usr/local/projects/ai_report/python/`

#### 3. Обновление базы знаний (KB)

1. **Редактировать KB файлы:**
   ```bash
   # Обновить описание таблицы
   vim kb_billing/tables/CUSTOMERS.json
   
   # Или добавить новый VIEW
   vim kb_billing/views/V_ANALYTICS_INVOICE_PERIOD.json
   
   # Или добавить пример SQL запроса
   vim kb_billing/training_data/sql_examples.json
   ```

2. **Синхронизировать и обновить KB:**
   ```bash
   SSH_CMD="ssh -p 1194" ./sync_and_update_kb.sh root@82.114.2.2
   ```
   
   Это автоматически:
   - Синхронизирует все KB файлы на сервер
   - Обновит KB в Qdrant (добавит новые/обновит существующие записи)

### Важные замечания

- **Рекомендуется использовать `sync_and_update_kb.sh`** для полной синхронизации всех компонентов
- `sync_deploy.sh` синхронизирует файлы, но **не обновляет KB** автоматически
- После синхронизации VIEW нужно **применить представление** через sqlplus
- После синхронизации KB файлов нужно **обновить KB в Qdrant** через `update_kb.sh` или использовать `sync_and_update_kb.sh`
- Используйте `timeout` для предотвращения зависания команд
- KB обновляется **без пересоздания коллекции** (новые данные добавляются, старые обновляются)

### Проверка после применения

После применения представления проверьте:
```sql
-- Проверка суммы Advance Charge за октябрь
SELECT 
    ROUND(SUM(v.FEE_ADVANCE_CHARGE), 2) AS total_advance_charge,
    23834.5 AS expected_amount,
    ROUND(SUM(v.FEE_ADVANCE_CHARGE) - 23834.5, 2) AS difference
FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
WHERE v.FINANCIAL_PERIOD = '2025-10'
  AND v.FEE_ADVANCE_CHARGE > 0;
```

### Структура файлов

```
ai_report/
├── oracle/
│   ├── views/              # SQL файлы представлений (VIEW)
│   ├── functions/          # PL/SQL функции
│   └── tables/             # SQL файлы таблиц
├── python/                 # Python скрипты для загрузки данных
├── kb_billing/             # База знаний (KB) для RAG-ассистента
│   ├── tables/             # Описания таблиц (*.json)
│   ├── views/              # Описания представлений (*.json)
│   ├── training_data/      # Примеры SQL запросов (*.json)
│   └── rag/                # RAG модули (Python)
├── deploy/                 # Директория для деплоя
│   ├── sync_deploy.sh      # Скрипт синхронизации файлов
│   └── update_kb.sh        # Скрипт обновления KB на сервере
├── sync_and_update_kb.sh   # Полная синхронизация + обновление KB
└── sync_and_rebuild_kb.sh  # Полная синхронизация + перестройка KB
```

### Что синхронизируется

**`sync_deploy.sh` синхронизирует:**
- ✅ Oracle VIEW (`oracle/views/*.sql`)
- ✅ Oracle функции (`oracle/functions/*.sql`)
- ✅ Python скрипты (`python/*.py`)
- ✅ KB файлы (`kb_billing/tables/*.json`, `kb_billing/views/*.json`, `kb_billing/training_data/*.json`)
- ✅ RAG модули (`kb_billing/rag/*.py`)
- ✅ Streamlit приложение (`deploy/streamlit_report_oracle_backup.py`)
- ✅ Скрипты управления (`deploy/*.sh`)

**`sync_and_update_kb.sh` дополнительно:**
- ✅ Автоматически обновляет KB в Qdrant на сервере (без пересоздания коллекции)

**`sync_and_rebuild_kb.sh` дополнительно:**
- ✅ Полностью пересоздает KB в Qdrant (удаляет старые данные)

### Проблемы и решения

**Проблема: Команды зависают через Cursor**
- Используйте `timeout` для команд
- Выполняйте команды в терминале напрямую
- Используйте локальный sqlplus через туннель: `./scripts/local/oracle_tunnel.sh start`

**Проблема: Нужно проверить данные локально**
- Запустите туннель: `./scripts/local/oracle_tunnel.sh start`
- Подключитесь: `sqlplus billing7/billing@localhost:1521/bm7`
- Выполните SQL запросы
- Остановите туннель: `./scripts/local/oracle_tunnel.sh stop`

**Проблема: Файл не синхронизируется**
- Проверьте SSH подключение: `ssh -p 1194 root@82.114.2.2 "echo OK"`
- Проверьте права на скрипт: `chmod +x deploy/sync_deploy.sh`
- Проверьте, что файл находится в правильной директории (например, `oracle/views/` для VIEW)

**Проблема: KB не обновляется после синхронизации**
- Используйте `sync_and_update_kb.sh` вместо `sync_deploy.sh`
- Или вручную на сервере: `ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && ./update_kb.sh"`
- Проверьте доступность Qdrant: `ssh -p 1194 root@82.114.2.2 "curl http://localhost:6333/health"`

**Проблема: Старые данные в KB не удаляются**
- Используйте `sync_and_rebuild_kb.sh` для полной перестройки KB (удалит старые данные)
- Или вручную на сервере: `ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && python3 kb_billing/rag/init_kb.py --recreate"`

