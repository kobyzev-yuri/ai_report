#!/bin/bash
# Синхронизация deploy/ на сервер. Запускать из корня проекта после prepare_deployment.sh.
# Перезапуск Streamlit — на сервере: ./restart_streamlit.sh
#
# SSH: по умолчанию просто «ssh -p 1194» — используется ваш ~/.ssh/config, ssh-agent и стандартный выбор ключей.
# Явный ключ только если сами зададите переменную (имя файла у каждого своё, в репозитории путь не задан):
#   export DEPLOY_SSH_KEY=~/.ssh/ВАШ_КЛЮЧ
#   ./sync_deploy.sh root@82.114.2.2
# Порт: DEPLOY_SSH_PORT (по умолчанию 1194).
# Полный контроль:
#   SSH_CMD="ssh -p 1194 -i \"$HOME/.ssh/ВАШ_КЛЮЧ\"" ./sync_deploy.sh root@82.114.2.2

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DEPLOY_DIR="$SCRIPT_DIR/deploy"
SERVER="${1:-root@82.114.2.2}"
REMOTE_DIR="/usr/local/projects/ai_report"

DEPLOY_SSH_PORT="${DEPLOY_SSH_PORT:-1194}"
USED_DEPLOY_SSH_KEY=0
if [ -z "${SSH_CMD:-}" ]; then
  SSH_CMD="ssh -p $DEPLOY_SSH_PORT"
  if [ -n "${DEPLOY_SSH_KEY:-}" ]; then
    KEY_PATH="${DEPLOY_SSH_KEY/#\~/$HOME}"
    if [ ! -r "$KEY_PATH" ]; then
      echo "Ошибка: приватный ключ не найден или нет прав на чтение: $KEY_PATH" >&2
      exit 1
    fi
    # Без IdentitiesOnly: иначе при «не том» ключе в DEPLOY_SSH_KEY SSH не пробует agent/config — только пароль.
    SSH_CMD="$SSH_CMD -i $KEY_PATH"
    USED_DEPLOY_SSH_KEY=1
  fi
fi

if [ ! -d "$DEPLOY_DIR" ]; then
  echo "Ошибка: директория deploy/ не найдена. Сначала выполните ./prepare_deployment.sh"
  exit 1
fi

echo "Синхронизация deploy/ на $SERVER:$REMOTE_DIR"
echo "SSH: $SSH_CMD"
if [ "$USED_DEPLOY_SSH_KEY" = 1 ]; then
  echo "" >&2
  echo "Подсказка: в окружении задана DEPLOY_SSH_KEY — добавлен -i …" >&2
  echo "  Запрос пароля: unset DEPLOY_SSH_KEY и снова ./sync_deploy.sh (или export DEPLOY_SSH_KEY=~/.ssh/верный_ключ)" >&2
fi
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
