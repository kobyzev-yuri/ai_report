#!/bin/bash
# Перезапуск и переинициализация KB

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================"
echo "Перезапуск KB"
echo "========================================"

# Загрузка конфигурации
if [ -f "config.env" ]; then
    source config.env
fi

# Переинициализация KB
echo "Переинициализация KB..."
./init_kb.sh --recreate

echo ""
echo "✅ KB переинициализирована"

# Перезапуск Streamlit (если запущен)
if [ -f "streamlit_8504.pid" ]; then
    PID=$(cat streamlit_8504.pid)
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "Перезапуск Streamlit..."
        kill "$PID"
        sleep 2
        ./run_streamlit_background.sh
    fi
fi

echo ""
echo "✅ Готово"


