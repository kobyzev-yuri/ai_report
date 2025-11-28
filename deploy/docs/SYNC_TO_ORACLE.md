# Синхронизация проекта на Oracle сервер

## Быстрый старт

### Вариант 1: Быстрая синхронизация на сервер vz2 (82.114.2.2)

Если вы используете алиас `vz2='ssh -p 1194 root@82.114.2.2'`:

```bash
./sync_to_vz2.sh
```

### Вариант 2: Настройка переменных окружения

```bash
export REMOTE_USER=root
export REMOTE_HOST=82.114.2.2
export REMOTE_PORT=1194
export REMOTE_PATH=/usr/local/projects/ai_report  # опционально
```

Затем запустите:
```bash
./sync_to_oracle_server.sh
```

## Что синхронизируется

### Включается:
- ✅ Все Python скрипты (`*.py`)
- ✅ SQL скрипты для Oracle (`oracle/`)
- ✅ Документация (`docs/`, `README.md`)
- ✅ Скрипты запуска (`*.sh`)
- ✅ `requirements.txt`
- ✅ `config.env.example` (но НЕ `config.env` с паролями)

### Исключается:
- ❌ `.git/` и `.gitignore`
- ❌ `__pycache__/` и `*.pyc`
- ❌ `data/` (данные загружаются отдельно)
- ❌ `config.env` (содержит пароли, создается на сервере)
- ❌ `archive/`
- ❌ `postgresql/` (не нужен на Oracle сервере)
- ❌ Логи и временные файлы (`*.log`, `*.pid`, `*.tmp`)
- ❌ IDE файлы (`.vscode/`, `.idea/`)

## Настройка на удаленном сервере

После синхронизации выполните на удаленном сервере:

### 1. Создайте config.env

```bash
cd /path/to/ai_report
cp config.env.example config.env
nano config.env  # отредактируйте с вашими данными Oracle
```

Пример `config.env` для Oracle:
```bash
# Oracle Configuration
ORACLE_USER=billing7
ORACLE_PASSWORD=your-password
ORACLE_HOST=192.168.3.35
ORACLE_PORT=1521
ORACLE_SERVICE=bm7
```

### 2. Установите зависимости

```bash
pip install -r requirements.txt
```

Или если используете conda:
```bash
conda install --file requirements.txt
```

### 3. Создайте директории для данных

```bash
mkdir -p data/SPNet\ reports
mkdir -p data/STECCOMLLCRussiaSBD.AccessFees_reports
```

### 4. Установите SQL скрипты в Oracle (если нужно)

```bash
# Подключитесь к Oracle и выполните:
cd oracle/tables
sqlplus billing7/password@bm7 @install_all_tables.sql

cd ../views
sqlplus billing7/password@bm7 @install_all_views.sql
```

### 5. Запустите Streamlit

```bash
# Через скрипт
./run_streamlit_background.sh

# Или напрямую
streamlit run streamlit_report_oracle_backup.py --server.port 8501
```

## Ручная синхронизация через rsync

Если нужно больше контроля, используйте rsync напрямую:

```bash
rsync -avz --progress \
    --exclude-from=.rsyncignore \
    ./ your-username@your-oracle-server:/path/to/ai_report/
```

## Файлы, которые нужно создать на сервере вручную

1. **config.env** - с реальными паролями Oracle
2. **data/** - директории для загрузки CSV файлов (создаются автоматически при первом запуске)

## Автоматическая синхронизация

Для автоматической синхронизации при изменениях можно использовать:

```bash
# Установите entr для отслеживания изменений
# Ubuntu/Debian: sudo apt install entr
# macOS: brew install entr

# Синхронизация при изменении файлов
find . -name "*.py" -o -name "*.sql" -o -name "*.sh" | entr -r ./sync_to_oracle_server.sh
```

## Проверка синхронизации

После синхронизации проверьте на удаленном сервере:

```bash
ssh your-username@your-oracle-server "ls -la /path/to/ai_report/"
```

## Важные замечания

1. **config.env НЕ синхронизируется** - содержит пароли, создайте его вручную на сервере
2. **data/ НЕ синхронизируется** - данные загружаются через интерфейс или отдельно
3. **postgresql/ НЕ синхронизируется** - нужен только для тестирования
4. Убедитесь, что на удаленном сервере установлен Python 3.8+ и все зависимости из `requirements.txt`

