#!/bin/bash
# Скрипт для подготовки минимального деплоя на сервер
# Создает директорию deploy/ только с необходимыми файлами

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DEPLOY_DIR="deploy"
echo "📦 Подготовка минимального деплоя в директорию: $DEPLOY_DIR"

# Удаляем старую директорию деплоя если есть
if [ -d "$DEPLOY_DIR" ]; then
    echo "🗑️  Удаление старой директории деплоя..."
    rm -rf "$DEPLOY_DIR"
fi

# Создаем структуру директорий
mkdir -p "$DEPLOY_DIR"
mkdir -p "$DEPLOY_DIR/oracle/tables"
mkdir -p "$DEPLOY_DIR/oracle/views"
mkdir -p "$DEPLOY_DIR/oracle/functions"
mkdir -p "$DEPLOY_DIR/oracle/data"
mkdir -p "$DEPLOY_DIR/python"
mkdir -p "$DEPLOY_DIR/kb_billing/tables"
mkdir -p "$DEPLOY_DIR/kb_billing/views"
mkdir -p "$DEPLOY_DIR/kb_billing/training_data"
mkdir -p "$DEPLOY_DIR/kb_billing/rag"
mkdir -p "$DEPLOY_DIR/docs"
mkdir -p "$DEPLOY_DIR/data/SPNet reports"
mkdir -p "$DEPLOY_DIR/data/STECCOMLLCRussiaSBD.AccessFees_reports"
mkdir -p "$DEPLOY_DIR/tabs"

echo "📋 Копирование файлов..."

# 1. Oracle скрипты (только необходимые для установки)
echo "  → Oracle скрипты..."
cp -r oracle/tables/*.sql "$DEPLOY_DIR/oracle/tables/" 2>/dev/null || true
cp -r oracle/views/*.sql "$DEPLOY_DIR/oracle/views/" 2>/dev/null || true
cp -r oracle/functions/*.sql "$DEPLOY_DIR/oracle/functions/" 2>/dev/null || true
cp -r oracle/data/*.sql "$DEPLOY_DIR/oracle/data/" 2>/dev/null || true
cp oracle/README.md "$DEPLOY_DIR/oracle/" 2>/dev/null || true

# 2. Python приложение
echo "  → Python приложение..."
cp streamlit_report_oracle_backup.py "$DEPLOY_DIR/"
cp streamlit_data_loader.py "$DEPLOY_DIR/"
cp db_connection.py "$DEPLOY_DIR/"
cp queries.py "$DEPLOY_DIR/"
cp auth_db.py "$DEPLOY_DIR/"
cp auth_db_v2.py "$DEPLOY_DIR/" 2>/dev/null || true
cp create_user.py "$DEPLOY_DIR/"
cp create_user_v2.py "$DEPLOY_DIR/" 2>/dev/null || true
cp python/*.py "$DEPLOY_DIR/python/" 2>/dev/null || true
echo "  → Вкладки (tabs)..."
cp tabs/*.py "$DEPLOY_DIR/tabs/" 2>/dev/null || true

# 3. Конфигурация
echo "  → Конфигурация..."
cp config.env.example "$DEPLOY_DIR/"
cp requirements.txt "$DEPLOY_DIR/"

# 4. Скрипты управления
echo "  → Скрипты управления..."
cp run_streamlit_background.sh "$DEPLOY_DIR/"
cp stop_streamlit.sh "$DEPLOY_DIR/"
cp status_streamlit.sh "$DEPLOY_DIR/"
cp restart_streamlit.sh "$DEPLOY_DIR/" 2>/dev/null || true

# 4.1. RAG deployment скрипты
echo "  → RAG deployment скрипты..."
if [ -f "deploy/deploy.sh" ]; then
    cp deploy/deploy.sh "$DEPLOY_DIR/"
    cp deploy/init_kb.sh "$DEPLOY_DIR/"
    cp deploy/stop_all.sh "$DEPLOY_DIR/"
    cp deploy/status_all.sh "$DEPLOY_DIR/"
fi
if [ -f "deploy/docker-compose.yml" ]; then
    cp deploy/docker-compose.yml "$DEPLOY_DIR/"
    cp deploy/Dockerfile.streamlit "$DEPLOY_DIR/"
    cp deploy/.dockerignore "$DEPLOY_DIR/"
fi
if [ -f "deploy/DEPLOYMENT_RAG.md" ]; then
    cp deploy/DEPLOYMENT_RAG.md "$DEPLOY_DIR/"
fi

chmod +x "$DEPLOY_DIR"/*.sh

# 5. База знаний (KB)
echo "  → База знаний (KB)..."
cp -r kb_billing/*.json "$DEPLOY_DIR/kb_billing/" 2>/dev/null || true
cp -r kb_billing/*.md "$DEPLOY_DIR/kb_billing/" 2>/dev/null || true
cp -r kb_billing/tables/*.json "$DEPLOY_DIR/kb_billing/tables/" 2>/dev/null || true
cp -r kb_billing/views/*.json "$DEPLOY_DIR/kb_billing/views/" 2>/dev/null || true
cp -r kb_billing/training_data/*.json "$DEPLOY_DIR/kb_billing/training_data/" 2>/dev/null || true

# 5.1. RAG модули
echo "  → RAG модули..."
cp -r kb_billing/rag/*.py "$DEPLOY_DIR/kb_billing/rag/" 2>/dev/null || true
cp -r kb_billing/rag/*.md "$DEPLOY_DIR/kb_billing/rag/" 2>/dev/null || true

# 6. Документация (минимальная)
echo "  → Документация..."
cp README.md "$DEPLOY_DIR/"
cp docs/ORACLE_TUNNEL.md "$DEPLOY_DIR/docs/" 2>/dev/null || true
cp docs/SYNC_TO_ORACLE.md "$DEPLOY_DIR/docs/" 2>/dev/null || true

# 7. Создаем .gitignore для деплоя
cat > "$DEPLOY_DIR/.gitignore" << 'EOF'
# Конфигурация с паролями
config.env

# Данные (загружаются через интерфейс)
data/SPNet reports/*
data/STECCOMLLCRussiaSBD.AccessFees_reports/*
!data/SPNet reports/.gitkeep
!data/STECCOMLLCRussiaSBD.AccessFees_reports/.gitkeep

# Логи и временные файлы
*.log
*.pid
__pycache__/
*.pyc
*.pyo
*.pyd
.Python

# База данных пользователей (может быть синхронизирована отдельно)
users.db

# Streamlit кэш
.streamlit/
EOF

# Создаем .gitkeep для пустых директорий данных
touch "$DEPLOY_DIR/data/SPNet reports/.gitkeep"
touch "$DEPLOY_DIR/data/STECCOMLLCRussiaSBD.AccessFees_reports/.gitkeep"

# Создаем README для деплоя
cat > "$DEPLOY_DIR/DEPLOY_README.md" << 'EOF'
# Минимальный деплой ai_report

Это минимальная версия проекта для деплоя на сервер.

## Быстрая установка

1. **Создайте config.env** (скопируйте из config.env.example и заполните):
```bash
cp config.env.example config.env
nano config.env  # Заполните реальные значения Oracle
```

2. **Установите зависимости**:
```bash
pip install -r requirements.txt
```

3. **Установите Oracle структуру** (если еще не установлена):
```bash
cd oracle/tables
sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @install_all_tables.sql

cd ../data
sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @tariff_plans_data.sql

cd ../functions
sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @calculate_overage.sql

cd ../views
sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @install_all_views.sql
```

4. **Запустите приложение**:
```bash
./run_streamlit_background.sh
```

## Что включено

- ✅ Oracle скрипты (tables, views, functions, data)
- ✅ Python приложение (Streamlit + загрузчики данных)
- ✅ База знаний (KB) для AI агента
- ✅ Скрипты управления (запуск/остановка/статус)
- ✅ Минимальная документация

## Что НЕ включено

- ❌ Архивы (archive/)
- ❌ Тесты (tests/)
- ❌ Временные файлы (__pycache__/, *.pyc)
- ❌ Данные (data/ - загружаются через интерфейс)
- ❌ Oracle тестовые/отладочные скрипты (testing/, test/, queries/)

## Размер деплоя

Проверьте размер:
```bash
du -sh deploy/
```

Ожидаемый размер: ~2-5 MB (без данных)
EOF

echo ""
echo "✅ Деплой подготовлен в директории: $DEPLOY_DIR"
echo ""
echo "📊 Статистика:"
du -sh "$DEPLOY_DIR" 2>/dev/null || echo "  Размер: проверьте вручную"
echo ""
echo "📝 Следующие шаги (перезапуск — только на сервере, после синхронизации):"
echo "  1. Синхронизируйте deploy на сервер (rsync/scp или ваш sync_* скрипт)"
echo "  2. На сервере: cd <REMOTE_DIR> && ./restart_streamlit.sh"
echo "  Локальный перезапуск не нужен."
echo ""
echo "  Проверка содержимого: ls -la $DEPLOY_DIR"
echo "  Архив: tar -czf ai_report_deploy.tar.gz $DEPLOY_DIR"
echo ""
echo "📖 Подробнее: см. docs/deploy.md"




