#!/bin/bash
# Перенос только тестового скрипта Confluence и config.env на сервер, запуск теста и вывод результата.
# Использование: ./deploy/transfer_and_test_confluence.sh [user@host]
#   ./deploy/transfer_and_test_confluence.sh
#   ./deploy/transfer_and_test_confluence.sh root@82.114.2.2

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

SERVER="${1:-root@82.114.2.2}"
SSH_CMD="${SSH_CMD:-ssh -p 1194}"
REMOTE_DIR="${REMOTE_DIR:-/usr/local/projects/ai_report}"

echo "=============================================="
echo "  Перенос теста Confluence на сервер"
echo "  Сервер: $SERVER"
echo "  Удалённая директория: $REMOTE_DIR"
echo "=============================================="

# Проверка подключения
if ! $SSH_CMD "$SERVER" "echo OK" 2>/dev/null | grep -q OK; then
    echo "❌ Не удалось подключиться к $SERVER (команда: $SSH_CMD)"
    exit 1
fi
echo "✅ Подключение к серверу OK"

# Создание структуры на сервере
$SSH_CMD "$SERVER" "mkdir -p $REMOTE_DIR/scripts $REMOTE_DIR/deploy $REMOTE_DIR/kb_billing/rag"

# Копирование только файлов для теста Confluence
echo ""
echo "Копирование файлов..."
rsync -avz -e "$SSH_CMD" \
    "$PROJECT_ROOT/scripts/test_confluence_connection.py" \
    "$SERVER:$REMOTE_DIR/scripts/"
rsync -avz -e "$SSH_CMD" \
    "$PROJECT_ROOT/kb_billing/rag/confluence_client.py" \
    "$SERVER:$REMOTE_DIR/kb_billing/rag/"
rsync -avz -e "$SSH_CMD" \
    "$PROJECT_ROOT/deploy/test_confluence_access.sh" \
    "$SERVER:$REMOTE_DIR/deploy/"
$SSH_CMD "$SERVER" "chmod +x $REMOTE_DIR/deploy/test_confluence_access.sh"

# config.env — копируем только если есть локально
if [ -f "$PROJECT_ROOT/config.env" ]; then
    echo "  → config.env (локальный)"
    rsync -avz -e "$SSH_CMD" \
        "$PROJECT_ROOT/config.env" \
        "$SERVER:$REMOTE_DIR/"
else
    echo "⚠️  Локальный config.env не найден. На сервере должен быть свой config.env с CONFLUENCE_URL и CONFLUENCE_TOKEN."
fi

echo ""
echo "=============================================="
echo "  Запуск теста на сервере..."
echo "=============================================="
$SSH_CMD "$SERVER" "cd $REMOTE_DIR && ./deploy/test_confluence_access.sh"
