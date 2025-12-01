#!/bin/bash
# Остановка всех сервисов (Qdrant и Streamlit)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================"
echo "Остановка всех сервисов"
echo "========================================"

# Остановка Streamlit
if [ -f "streamlit_8504.pid" ]; then
    PID=$(cat streamlit_8504.pid)
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "Остановка Streamlit (PID: $PID)..."
        kill "$PID"
        rm streamlit_8504.pid
        echo "✅ Streamlit остановлен"
    else
        echo "ℹ️  Streamlit не запущен"
        rm streamlit_8504.pid
    fi
else
    echo "ℹ️  Streamlit не запущен"
fi

# Остановка Qdrant (если запущен через Docker)
if command -v docker &> /dev/null; then
    if docker ps | grep -q ai_report_qdrant; then
        echo "Остановка Qdrant..."
        docker stop ai_report_qdrant
        echo "✅ Qdrant остановлен"
    else
        echo "ℹ️  Qdrant не запущен через Docker"
    fi
fi

# Остановка через Docker Compose (если используется)
if [ -f "docker-compose.yml" ]; then
    if command -v docker-compose &> /dev/null || command -v docker compose &> /dev/null; then
        echo "Остановка Docker Compose сервисов..."
        docker-compose down 2>/dev/null || docker compose down 2>/dev/null || true
        echo "✅ Docker Compose сервисы остановлены"
    fi
fi

echo ""
echo "✅ Все сервисы остановлены"


