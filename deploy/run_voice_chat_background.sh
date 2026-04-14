#!/bin/bash
# Запуск голосового интерфейса (voice_chat) в фоне
# Использование: ./run_voice_chat_background.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Активация venv
if [ -f "venv39/bin/activate" ]; then
    set -a
    source "venv39/bin/activate"
    set +a
elif [ -f "venv311/bin/activate" ]; then
    set -a
    source "venv311/bin/activate"
    set +a
elif [ -f "venv/bin/activate" ]; then
    set -a
    source "venv/bin/activate"
    set +a
fi

# Загрузка config.env (в т.ч. VOICE_CHAT_PORT)
if [ -f "$SCRIPT_DIR/config.env" ]; then
    set -a
    source "$SCRIPT_DIR/config.env"
    set +a
fi

PORT="${VOICE_CHAT_PORT:-5001}"
LOG_FILE="$SCRIPT_DIR/voice_chat_${PORT}.log"
PID_FILE="$SCRIPT_DIR/voice_chat_${PORT}.pid"

# Проверка: уже запущен?
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if ps -p "$OLD_PID" > /dev/null 2>&1; then
        echo "⚠️  Голосовой интерфейс уже запущен (PID: $OLD_PID)"
        echo "   Остановить: ./stop_voice_chat.sh"
        exit 1
    fi
    rm -f "$PID_FILE"
fi

echo "========================================"
echo "Запуск голосового интерфейса (voice_chat)"
echo "========================================"
echo "Порт: $PORT"
echo "Логи: $LOG_FILE"
echo ""

# Запуск в фоне (из корня проекта, чтобы подтянуть config.env и users.db)
nohup python -m voice_chat.app >> "$LOG_FILE" 2>&1 &
VOICE_PID=$!
echo $VOICE_PID > "$PID_FILE"

sleep 2
if ps -p "$VOICE_PID" > /dev/null 2>&1; then
    echo "✅ Голосовой интерфейс запущен (PID: $VOICE_PID)"
    echo "   Логи: tail -f $LOG_FILE"
    echo "   URL: http://localhost:$PORT (или через nginx /voice/)"
    echo "   Остановить: ./stop_voice_chat.sh"
else
    echo "❌ Ошибка запуска. Проверьте: cat $LOG_FILE"
    rm -f "$PID_FILE"
    exit 1
fi
