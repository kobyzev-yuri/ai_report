#!/bin/bash
# Чистый перенос kb_billing на сервер: только необходимые файлы из deploy/.
# На сервере НЕ трогаем confluence_docs/ (пополняется Спутниковым библиотекарём).
# В подкаталогах rag/, tables/, views/, training_data/ делаем полное совпадение с deploy (--delete).
#
# Запуск (из корня репозитория или с указанием путей):
#   SSH_CMD="ssh -p 1194" ./scripts/deploy_kb_billing_clean.sh [сервер]
#   SERVER=root@82.114.2.2 SSH_CMD="ssh -p 1194" ./scripts/deploy_kb_billing_clean.sh
#
# Перед запуском обновите deploy: ./prepare_deployment.sh && cp -r kb_billing/rag deploy/kb_billing/  (если нужно)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEPLOY_KB="$PROJECT_ROOT/deploy/kb_billing"
REMOTE_DIR="${REMOTE_DIR:-/usr/local/projects/ai_report}"
SERVER="${1:-root@82.114.2.2}"
SSH_CMD="${SSH_CMD:-ssh}"

if [ ! -d "$DEPLOY_KB" ]; then
    echo "❌ Нет каталога deploy/kb_billing. Сначала выполните: ./prepare_deployment.sh"
    exit 1
fi

echo "========================================"
echo "Чистый перенос kb_billing на сервер"
echo "========================================"
echo "Источник: $DEPLOY_KB"
echo "Сервер:  $SERVER:$REMOTE_DIR/kb_billing/"
echo "Не трогаем на сервере: confluence_docs/"
echo ""

# Проверка подключения
if ! $SSH_CMD "$SERVER" "echo OK" >/dev/null 2>&1; then
    echo "❌ Не удалось подключиться к $SERVER"
    exit 1
fi

# 1) Подкаталоги — полное совпадение с deploy (лишнее на сервере удаляется)
echo "1) rag/ (код RAG) — полная замена..."
rsync -avz -e "$SSH_CMD" --delete \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    "$DEPLOY_KB/rag/" \
    "$SERVER:$REMOTE_DIR/kb_billing/rag/"

echo "2) tables/ — полная замена..."
rsync -avz -e "$SSH_CMD" --delete \
    "$DEPLOY_KB/tables/" \
    "$SERVER:$REMOTE_DIR/kb_billing/tables/"

echo "3) views/ — полная замена..."
rsync -avz -e "$SSH_CMD" --delete \
    "$DEPLOY_KB/views/" \
    "$SERVER:$REMOTE_DIR/kb_billing/views/"

echo "4) training_data/ — полная замена..."
rsync -avz -e "$SSH_CMD" --delete \
    "$DEPLOY_KB/training_data/" \
    "$SERVER:$REMOTE_DIR/kb_billing/training_data/"

# 2) Файлы в корне kb_billing (без --delete, чтобы не задеть confluence_docs)
echo "5) Корень kb_billing (metadata.json, *.md и т.д.)..."
rsync -avz -e "$SSH_CMD" \
    "$DEPLOY_KB"/*.json \
    "$SERVER:$REMOTE_DIR/kb_billing/" 2>/dev/null || true
rsync -avz -e "$SSH_CMD" \
    "$DEPLOY_KB"/*.md \
    "$SERVER:$REMOTE_DIR/kb_billing/" 2>/dev/null || true

# 3) Удалить на сервере мусор в kb_billing (__pycache__, *.pyc)
echo "6) Очистка __pycache__ и *.pyc на сервере..."
$SSH_CMD "$SERVER" "find $REMOTE_DIR/kb_billing -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; find $REMOTE_DIR/kb_billing -name '*.pyc' -delete 2>/dev/null" || true

echo ""
echo "✅ Готово. На сервере обновлены только: rag/, tables/, views/, training_data/, корневые *.json и *.md."
echo "   confluence_docs/ на сервере не изменялся."
echo ""
echo "Дальше: перезагрузить KB в Qdrant (если нужно) и перезапустить Streamlit:"
echo "  $SSH_CMD $SERVER 'cd $REMOTE_DIR && ./restart_streamlit.sh'"
