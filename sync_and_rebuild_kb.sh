#!/bin/bash
# Синхронизация KB файлов на сервер и перестройка базы знаний
# Использование: ./sync_and_rebuild_kb.sh [user@]host [ssh_command]
# Примеры:
#   ./sync_and_rebuild_kb.sh root@82.114.2.2
#   SSH_CMD="ssh -p 1194" ./sync_and_rebuild_kb.sh root@82.114.2.2

set -e

if [ -z "$1" ]; then
    echo "Использование: $0 [user@]host [ssh_command]"
    echo "Примеры:"
    echo "  $0 root@82.114.2.2"
    echo "  SSH_CMD='ssh -p 1194' $0 root@82.114.2.2"
    exit 1
fi

SERVER="$1"
SSH_CMD="${SSH_CMD:-${2:-ssh -p 1194}}"
REMOTE_DIR="/usr/local/projects/ai_report"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "========================================"
echo "Синхронизация KB и перестройка базы знаний"
echo "========================================"
echo "Сервер: $SERVER"
echo "SSH команда: $SSH_CMD"
echo "Удаленная директория: $REMOTE_DIR"
echo "Локальная директория: $PROJECT_ROOT"
echo ""

# Проверка подключения
echo "Проверка подключения к серверу..."
if ! $SSH_CMD "$SERVER" "echo 'OK'" >/dev/null 2>&1; then
    echo "❌ Не удалось подключиться к серверу"
    exit 1
fi
echo "✅ Подключение успешно"
echo ""

# Проверка rsync
if ! command -v rsync >/dev/null 2>&1; then
    echo "❌ rsync не установлен. Установите: sudo apt-get install rsync"
    exit 1
fi
echo "✅ rsync доступен"
echo ""

# Создание удаленных директорий
echo "Создание структуры директорий на сервере..."
$SSH_CMD "$SERVER" "mkdir -p $REMOTE_DIR/kb_billing/{rag,tables,views,training_data}"
echo "✅ Директории созданы"
echo ""

# Функция для выполнения rsync с правильной SSH командой
rsync_cmd() {
    if [[ "$SSH_CMD" == "ssh"* ]]; then
        rsync -avz --progress -e "$SSH_CMD" "$@"
    else
        rsync -avz --progress "$@"
    fi
}

# Синхронизация KB файлов
echo "========================================"
echo "Синхронизация KB файлов..."
echo "========================================"

# RAG модули
echo "  → RAG модули..."
rsync_cmd \
    "$PROJECT_ROOT/kb_billing/rag/"*.py \
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

# ВАЖНО: Примеры SQL запросов
echo "  → Примеры SQL запросов (training_data)..."
rsync_cmd \
    "$PROJECT_ROOT/kb_billing/training_data/"*.json \
    "$SERVER:$REMOTE_DIR/kb_billing/training_data/" 2>/dev/null || true

echo ""
echo "✅ Синхронизация файлов завершена"
echo ""

# Перестройка базы знаний на сервере
echo "========================================"
echo "Перестройка базы знаний на сервере..."
echo "========================================"

# Проверка доступности Qdrant на сервере
echo "Проверка доступности Qdrant..."
if ! $SSH_CMD "$SERVER" "curl -s http://localhost:6333/health > /dev/null 2>&1"; then
    echo "⚠️  Qdrant недоступен на сервере, но продолжим..."
fi

# Запуск перестройки KB на сервере
echo "Запуск перестройки KB..."
$SSH_CMD "$SERVER" "cd $REMOTE_DIR && python3 kb_billing/rag/init_kb.py --recreate --host localhost --port 6333 --collection kb_billing"

echo ""
echo "========================================"
echo "✅ Синхронизация и перестройка завершены!"
echo "========================================"
echo ""
echo "База знаний обновлена на сервере $SERVER"
echo "Новые примеры запросов доступны в RAG-ассистенте"


