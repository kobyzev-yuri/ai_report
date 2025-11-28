#!/bin/bash
# Проверка статуса Streamlit приложения

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PORT=8504
PID_FILE="$SCRIPT_DIR/streamlit_${PORT}.pid"
LOG_FILE="$SCRIPT_DIR/streamlit_${PORT}.log"

echo "========================================"
echo "Статус Streamlit (порт $PORT)"
echo "========================================"
echo ""

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "✅ Streamlit запущен"
        echo "   PID: $PID"
        echo "   Логи: $LOG_FILE"
        echo ""
        echo "Последние строки лога:"
        tail -5 "$LOG_FILE" 2>/dev/null || echo "   (лог пуст)"
    else
        echo "❌ Процесс не найден (PID файл существует, но процесс не запущен)"
        echo "   Удалите PID файл: rm $PID_FILE"
    fi
else
    echo "⚠️  PID файл не найден"
    echo "   Проверка процессов вручную..."
    ps aux | grep -E "streamlit.*8504" | grep -v grep || echo "   Процесс не найден"
fi

echo ""
echo "Проверка доступности на порту $PORT:"
if command -v curl > /dev/null 2>&1; then
    if curl -s --max-time 2 "http://localhost:$PORT/_stcore/health" > /dev/null 2>&1; then
        echo "✅ Порт $PORT доступен"
    else
        echo "❌ Порт $PORT недоступен"
    fi
else
    echo "   (curl не установлен, проверка пропущена)"
fi


