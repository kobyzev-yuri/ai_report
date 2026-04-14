#!/bin/bash
# Статус голосового интерфейса (voice_chat)
# Использование: ./status_voice_chat.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
if [ -f "config.env" ]; then
    set -a
    source "config.env"
    set +a
fi
PORT="${VOICE_CHAT_PORT:-5001}"
PID_FILE="$SCRIPT_DIR/voice_chat_${PORT}.pid"
LOG_FILE="$SCRIPT_DIR/voice_chat_${PORT}.log"

echo "Голосовой интерфейс (voice_chat), порт $PORT"
echo ""

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "   Статус: запущен"
        echo "   PID: $PID"
        echo "   Лог: $LOG_FILE"
        echo "   URL: http://localhost:$PORT"
    else
        echo "   Статус: не запущен (PID-файл устарел)"
    fi
else
    echo "   Статус: не запущен"
fi

echo ""
echo "Запуск:   ./run_voice_chat_background.sh"
echo "Остановка: ./stop_voice_chat.sh"
echo "Перезапуск: ./restart_voice_chat.sh"
