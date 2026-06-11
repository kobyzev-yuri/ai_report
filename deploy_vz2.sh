#!/usr/bin/env bash
# Полный деплой на vz2: prepare → rsync deploy/ → опционально restart Streamlit
#
#   ./deploy_vz2.sh              — подготовить пакет и синхронизировать
#   ./deploy_vz2.sh --restart    — то же + перезапуск Streamlit на vz2
#   ./deploy_vz2.sh --no-prepare --restart   — только sync + restart
#
# Переменные по умолчанию: deploy.vz2.env

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# shellcheck disable=SC1091
[ -f "$SCRIPT_DIR/deploy.vz2.env" ] && source "$SCRIPT_DIR/deploy.vz2.env"

DEPLOY_SERVER="${DEPLOY_SERVER:-vz2}"
SSH_CMD="${SSH_CMD:-ssh}"
REMOTE_DIR="${REMOTE_DIR:-/usr/local/projects/ai_report}"

SKIP_PREPARE=0
DO_RESTART=0
for arg in "$@"; do
  case "$arg" in
    --no-prepare) SKIP_PREPARE=1 ;;
    --restart) DO_RESTART=1 ;;
    -h|--help)
      sed -n '2,8p' "$0"
      exit 0
      ;;
  esac
done

if [ "$SKIP_PREPARE" -eq 0 ]; then
  ./prepare_deployment.sh
fi

SSH_CMD="$SSH_CMD" ./sync_deploy.sh "$DEPLOY_SERVER"

if [ "$DO_RESTART" -eq 1 ]; then
  echo ""
  echo "Перезапуск Streamlit на $DEPLOY_SERVER..."
  $SSH_CMD "$DEPLOY_SERVER" "cd $REMOTE_DIR && ./restart_streamlit.sh"
fi

echo ""
echo "Готово → ${APP_URL:-http://192.168.3.22:8504/ai_report}"
if [ "$DO_RESTART" -eq 0 ]; then
  echo "Перезапуск: ./deploy_vz2.sh --no-prepare --restart"
fi
