#!/bin/bash
# Синхронизация deploy директории на сервер
# Использование: ./sync_deploy.sh [user@]host [ssh_command]
# Примеры:
#   ./sync_deploy.sh root@82.114.2.2
#   SSH_CMD="ssh -p 1194" ./sync_deploy.sh root@82.114.2.2
#   SSH_CMD="vz2" ./sync_deploy.sh root@82.114.2.2

set -e

if [ -z "$1" ]; then
    echo "Использование: $0 [user@]host [ssh_command]"
    echo "Примеры:"
    echo "  $0 root@82.114.2.2"
    echo "  SSH_CMD='ssh -p 1194' $0 root@82.114.2.2"
    echo "  SSH_CMD='vz2' $0 root@82.114.2.2"
    exit 1
fi

SERVER="$1"
SSH_CMD="${SSH_CMD:-${2:-ssh}}"
REMOTE_DIR="/usr/local/projects/ai_report"
LOCAL_DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$LOCAL_DEPLOY_DIR/.." && pwd)"

echo "========================================"
echo "Синхронизация deploy на сервер"
echo "========================================"
echo "Сервер: $SERVER"
echo "SSH команда: $SSH_CMD"
echo "Удаленная директория: $REMOTE_DIR"
echo "Локальная директория: $LOCAL_DEPLOY_DIR"
echo ""

# Проверка подключения
echo "Проверка подключения..."
if ! $SSH_CMD "$SERVER" "echo 'OK'" >/dev/null 2>&1; then
    echo "❌ Не удалось подключиться к серверу"
    echo ""
    echo "Если используете кастомный порт или алиас:"
    echo "  SSH_CMD='ssh -p 1194' $0 $SERVER"
    echo "  или"
    echo "  SSH_CMD='vz2' $0 $SERVER"
    exit 1
fi
echo "✅ Подключение успешно"

# Проверка rsync
if ! command -v rsync >/dev/null 2>&1; then
    echo "❌ rsync не установлен. Установите: sudo apt-get install rsync"
    exit 1
fi
echo "✅ rsync доступен"
echo ""

# Создание удаленной директории
echo "Создание структуры директорий на сервере..."
$SSH_CMD "$SERVER" "mkdir -p $REMOTE_DIR/{kb_billing/rag,oracle/{tables,views,functions,data},python,data/{SPNet\ reports,STECCOMLLCRussiaSBD.AccessFees_reports}}"
echo "✅ Директории созданы"
echo ""

# Синхронизация файлов
echo "Синхронизация файлов..."

# Определение опций rsync с SSH
if [[ "$SSH_CMD" == "ssh" ]]; then
    RSYNC_OPTS="-avz --progress"
else
    RSYNC_OPTS="-avz --progress -e \"$SSH_CMD\""
fi

# Функция для выполнения rsync с правильной SSH командой
rsync_cmd() {
    if [[ "$SSH_CMD" == "ssh" ]]; then
        rsync -avz --progress "$@"
    else
        rsync -avz --progress -e "$SSH_CMD" "$@"
    fi
}

# Основные файлы приложения
echo "  → Основные файлы..."
rsync_cmd \
    "$LOCAL_DEPLOY_DIR/streamlit_report_oracle_backup.py" \
    "$LOCAL_DEPLOY_DIR/streamlit_data_loader.py" \
    "$LOCAL_DEPLOY_DIR/db_connection.py" \
    "$LOCAL_DEPLOY_DIR/auth_db.py" \
    "$LOCAL_DEPLOY_DIR/create_user.py" \
    "$SERVER:$REMOTE_DIR/"

# Скрипты управления
echo "  → Скрипты управления..."
rsync_cmd \
    "$LOCAL_DEPLOY_DIR/"*.sh \
    "$SERVER:$REMOTE_DIR/"

# RAG модули
echo "  → RAG модули..."
rsync_cmd \
    "$PROJECT_ROOT/kb_billing/rag/"*.py \
    "$PROJECT_ROOT/kb_billing/rag/"*.md \
    "$SERVER:$REMOTE_DIR/kb_billing/rag/" 2>/dev/null || true

# KB данные
echo "  → База знаний (KB)..."
rsync_cmd \
    "$PROJECT_ROOT/kb_billing/"*.json \
    "$PROJECT_ROOT/kb_billing/"*.md \
    "$SERVER:$REMOTE_DIR/kb_billing/" 2>/dev/null || true

rsync_cmd \
    "$PROJECT_ROOT/kb_billing/tables/"*.json \
    "$SERVER:$REMOTE_DIR/kb_billing/tables/" 2>/dev/null || true

rsync_cmd \
    "$PROJECT_ROOT/kb_billing/views/"*.json \
    "$SERVER:$REMOTE_DIR/kb_billing/views/" 2>/dev/null || true

rsync_cmd \
    "$PROJECT_ROOT/kb_billing/training_data/"*.json \
    "$SERVER:$REMOTE_DIR/kb_billing/training_data/" 2>/dev/null || true

# Oracle скрипты
echo "  → Oracle скрипты..."
rsync_cmd \
    "$PROJECT_ROOT/oracle/tables/"*.sql \
    "$SERVER:$REMOTE_DIR/oracle/tables/" 2>/dev/null || true

rsync_cmd \
    "$PROJECT_ROOT/oracle/views/"*.sql \
    "$SERVER:$REMOTE_DIR/oracle/views/" 2>/dev/null || true

rsync_cmd \
    "$PROJECT_ROOT/oracle/functions/"*.sql \
    "$SERVER:$REMOTE_DIR/oracle/functions/" 2>/dev/null || true

rsync_cmd \
    "$PROJECT_ROOT/oracle/data/"*.sql \
    "$SERVER:$REMOTE_DIR/oracle/data/" 2>/dev/null || true

# Python скрипты
echo "  → Python скрипты..."
rsync_cmd \
    "$PROJECT_ROOT/python/"*.py \
    "$SERVER:$REMOTE_DIR/python/" 2>/dev/null || true

# Конфигурация и зависимости
echo "  → Конфигурация..."
rsync_cmd \
    "$LOCAL_DEPLOY_DIR/config.env.example" \
    "$LOCAL_DEPLOY_DIR/requirements.txt" \
    "$SERVER:$REMOTE_DIR/"

# Docker файлы
echo "  → Docker файлы..."
rsync_cmd \
    "$LOCAL_DEPLOY_DIR/docker-compose.yml" \
    "$LOCAL_DEPLOY_DIR/Dockerfile.streamlit" \
    "$LOCAL_DEPLOY_DIR/.dockerignore" \
    "$SERVER:$REMOTE_DIR/" 2>/dev/null || true

# Документация
echo "  → Документация..."
rsync_cmd \
    "$LOCAL_DEPLOY_DIR/DEPLOYMENT_RAG.md" \
    "$LOCAL_DEPLOY_DIR/DEPLOYMENT_SUMMARY.md" \
    "$LOCAL_DEPLOY_DIR/README_DEPLOYMENT.md" \
    "$LOCAL_DEPLOY_DIR/QUICK_DEPLOY.md" \
    "$LOCAL_DEPLOY_DIR/DEPLOYMENT_SERVER.md" \
    "$LOCAL_DEPLOY_DIR/DEPLOYMENT_PLAN.md" \
    "$LOCAL_DEPLOY_DIR/DEPLOY_TO_SERVER.md" \
    "$LOCAL_DEPLOY_DIR/START_HERE.md" \
    "$SERVER:$REMOTE_DIR/" 2>/dev/null || true

# Установка прав на скрипты
echo "  → Установка прав на скрипты..."
$SSH_CMD "$SERVER" "chmod +x $REMOTE_DIR/*.sh 2>/dev/null || true"

echo ""
echo "✅ Синхронизация завершена"
echo ""
echo "Проверка на сервере:"
$SSH_CMD "$SERVER" "ls -la $REMOTE_DIR | head -20"

