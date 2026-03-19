#!/usr/bin/env bash
# SSH-туннель: локальный порт -> Oracle listener на стороне SSH-сервера (или за ним).
#
# Пример (Oracle слушает на самом jump-сервере):
#   ./scripts/oracle_ssh_tunnel.sh
#   sqlplus billing7/пароль@//127.0.0.1:1521/bm7
#
# Если локальный 1521 занят:
#   ORACLE_LOCAL_PORT=11521 ./scripts/oracle_ssh_tunnel.sh
#   sqlplus user/pass@//127.0.0.1:11521/bm7
#
# Если listener только на внутреннем хосте, доступном с сервера:
#   ORACLE_REMOTE_DB_HOST=10.0.0.5 ./scripts/oracle_ssh_tunnel.sh

set -e
SSH_CMD="${SSH_CMD:-ssh -p 1194}"
SERVER="${ORACLE_TUNNEL_SSH:-root@82.114.2.2}"
LPORT="${ORACLE_LOCAL_PORT:-1521}"
RHOST="${ORACLE_REMOTE_DB_HOST:-127.0.0.1}"
RPORT="${ORACLE_REMOTE_DB_PORT:-1521}"

case "${1:-start}" in
  start)
    echo "Туннель: 127.0.0.1:${LPORT} -> ${SERVER}:${RHOST}:${RPORT}"
    echo "Оставьте эту сессию открытой. В другом терминале — sqlplus или Python с ORACLE_HOST=127.0.0.1 ORACLE_PORT=${LPORT}"
    exec $SSH_CMD -o ExitOnForwardFailure=yes -L "${LPORT}:${RHOST}:${RPORT}" "$SERVER" -N
    ;;
  *)
    echo "Usage: $0 [start]"
    exit 1
    ;;
esac
