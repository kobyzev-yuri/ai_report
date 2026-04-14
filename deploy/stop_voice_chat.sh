#!/bin/bash
# Остановка голосового интерфейса (voice_chat)
# Использование: ./stop_voice_chat.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
if [ -f "config.env" ]; then
    set -a
    source "config.env"
    set +a
fi
PORT="${VOICE_CHAT_PORT:-5001}"
PID_FILE="$SCRIPT_DIR/voice_chat_${PORT}.pid"

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        kill "$PID"
        sleep 2
        if ps -p "$PID" > /dev/null 2>&1; then
            kill -9 "$PID"
        fi
        echo "✅ Голосовой интерфейс остановлен (PID: $PID)"
    else
        echo "⚠️  Процесс не найден (PID: $PID)"
    fi
    rm -f "$PID_FILE"
else
    if pkill -f "voice_chat.app" 2>/dev/null; then
        echo "✅ Процессы voice_chat остановлены"
    else
        echo "ℹ️  Голосовой интерфейс не запущен"
    fi
fi
