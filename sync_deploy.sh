#!/bin/bash
# Синхронизация deploy/ на сервер. Запускать из корня проекта после prepare_deployment.sh.
# Перезапуск Streamlit — на сервере: ./restart_streamlit.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DEPLOY_DIR="$SCRIPT_DIR/deploy"
SERVER="${1:-root@82.114.2.2}"
SSH_CMD="${SSH_CMD:-ssh -p 1194}"
REMOTE_DIR="/usr/local/projects/ai_report"

if [ ! -d "$DEPLOY_DIR" ]; then
  echo "Ошибка: директория deploy/ не найдена. Сначала выполните ./prepare_deployment.sh"
  exit 1
fi

echo "Синхронизация deploy/ на $SERVER:$REMOTE_DIR"
echo "SSH: $SSH_CMD"
echo ""

$SSH_CMD "$SERVER" "mkdir -p $REMOTE_DIR" 2>/dev/null || true

rsync -avz \
  --exclude='config.env' \
  --exclude='users.db' \
  --exclude='*.log' \
  --exclude='*.pid' \
  --exclude='.git' \
  --exclude='__pycache__' \
  -e "$SSH_CMD" \
  "$DEPLOY_DIR/" "$SERVER:$REMOTE_DIR/"

echo ""
echo "Готово. Перезапуск Streamlit на сервере:"
echo "  $SSH_CMD $SERVER 'cd $REMOTE_DIR && ./restart_streamlit.sh'"
