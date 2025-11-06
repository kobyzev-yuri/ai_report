#!/bin/bash
# Остановка Streamlit приложения

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PORT=8504
PID_FILE="$SCRIPT_DIR/streamlit_${PORT}.pid"

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "Остановка Streamlit (PID: $PID)..."
        kill "$PID"
        sleep 2
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "Принудительная остановка..."
            kill -9 "$PID"
        fi
        rm "$PID_FILE"
        echo "✅ Streamlit остановлен"
    else
        echo "⚠️  Процесс не найден, удаляю PID файл"
        rm "$PID_FILE"
    fi
else
    echo "⚠️  PID файл не найден: $PID_FILE"
    echo "Поиск процесса вручную..."
    pkill -f "streamlit.*8504" && echo "✅ Процесс остановлен" || echo "❌ Процесс не найден"
fi


