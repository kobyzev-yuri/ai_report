#!/bin/bash
# Быстрое обследование с автоматическим определением SSH команды
# Использование: ./quick_inspect.sh [user@]host

set -e

if [ -z "$1" ]; then
    echo "Использование: $0 [user@]host"
    echo "Пример: $0 root@82.114.2.2"
    exit 1
fi

SERVER="$1"

echo "========================================"
echo "Быстрое обследование сервера"
echo "========================================"
echo "Сервер: $SERVER"
echo ""

# Попытка определить правильную SSH команду
echo "Попытка подключения..."

# Проверка стандартного SSH
if timeout 3 ssh -o ConnectTimeout=2 "$SERVER" "echo 'OK'" 2>/dev/null; then
    echo "✅ Стандартный SSH работает"
    SSH_CMD="ssh"
elif command -v vz2 >/dev/null 2>&1; then
    echo "✅ Найден алиас vz2"
    SSH_CMD="vz2"
elif timeout 3 ssh -p 1194 -o ConnectTimeout=2 "$SERVER" "echo 'OK'" 2>/dev/null; then
    echo "✅ SSH на порту 1194 работает"
    SSH_CMD="ssh -p 1194"
else
    echo "❌ Не удалось определить SSH команду"
    echo ""
    echo "Попробуйте вручную:"
    echo "  SSH_CMD='ssh -p 1194' ./server_inspection.sh $SERVER"
    echo "  или"
    echo "  SSH_CMD='vz2' ./server_inspection.sh $SERVER"
    exit 1
fi

echo ""
echo "Используется SSH команда: $SSH_CMD"
echo ""

# Запуск полного обследования с определенной SSH командой
SSH_CMD="$SSH_CMD" ./server_inspection.sh "$SERVER"

