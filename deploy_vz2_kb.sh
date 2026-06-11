#!/usr/bin/env bash
# Синхронизация KB на vz2 и переиндексация Qdrant
#
#   ./deploy_vz2_kb.sh
#   ./deploy_vz2_kb.sh --only-examples
#
# Переменные по умолчанию: deploy.vz2.env

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# shellcheck disable=SC1091
[ -f "$SCRIPT_DIR/deploy.vz2.env" ] && source "$SCRIPT_DIR/deploy.vz2.env"

DEPLOY_SERVER="${DEPLOY_SERVER:-vz2}"
SSH_CMD="${SSH_CMD:-ssh}"

exec env SSH_CMD="$SSH_CMD" ./sync_and_update_kb.sh "$@" "$DEPLOY_SERVER" "$SSH_CMD"
