#!/bin/bash
# Перезапуск голосового интерфейса (voice_chat)
# Использование: ./restart_voice_chat.sh

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

echo "========================================"
echo "Перезапуск голосового интерфейса (voice_chat)"
echo "========================================"

# Остановка
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "Остановка процесса $PID..."
        kill "$PID" 2>/dev/null
        sleep 2
        if ps -p "$PID" > /dev/null 2>&1; then
            kill -9 "$PID"
        fi
    fi
    rm -f "$PID_FILE"
else
    pkill -f "voice_chat.app" 2>/dev/null || true
    sleep 1
fi

echo "Запуск..."
if [ -f "run_voice_chat_background.sh" ]; then
    ./run_voice_chat_background.sh
else
    echo "❌ run_voice_chat_background.sh не найден"
    exit 1
fi

echo ""
echo "========================================"
echo "Перезапуск завершён"
echo "========================================"
