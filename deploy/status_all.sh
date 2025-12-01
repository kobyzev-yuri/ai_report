#!/bin/bash
# Проверка статуса всех сервисов

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================"
echo "Статус сервисов RAG системы"
echo "========================================"
echo ""

# Статус Streamlit
echo "📊 Streamlit:"
if [ -f "streamlit_8504.pid" ]; then
    PID=$(cat streamlit_8504.pid)
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "  ✅ Запущен (PID: $PID)"
        echo "  📍 URL: http://localhost:8504"
        if [ -f "streamlit_8504.log" ]; then
            echo "  📝 Логи: tail -f streamlit_8504.log"
        fi
    else
        echo "  ❌ Не запущен (старый PID файл)"
    fi
else
    echo "  ❌ Не запущен"
fi

echo ""

# Статус Qdrant
echo "🔍 Qdrant:"
if command -v curl &> /dev/null; then
    if curl -s http://localhost:6333/health > /dev/null 2>&1; then
        echo "  ✅ Доступен на localhost:6333"
        
        # Информация о коллекции
        if [ -f "config.env" ]; then
            source config.env
            COLLECTION="${QDRANT_COLLECTION:-kb_billing}"
            COLLECTION_INFO=$(curl -s "http://localhost:6333/collections/$COLLECTION" 2>/dev/null)
            if [ $? -eq 0 ] && [ -n "$COLLECTION_INFO" ]; then
                POINTS_COUNT=$(echo "$COLLECTION_INFO" | grep -o '"points_count":[0-9]*' | grep -o '[0-9]*' || echo "0")
                echo "  📊 Коллекция '$COLLECTION': $POINTS_COUNT точек"
            else
                echo "  ⚠️  Коллекция '$COLLECTION' не найдена"
            fi
        fi
    else
        echo "  ❌ Недоступен на localhost:6333"
    fi
else
    echo "  ⚠️  curl не найден, проверка недоступна"
fi

# Проверка через Docker
if command -v docker &> /dev/null; then
    if docker ps | grep -q ai_report_qdrant; then
        echo "  🐳 Запущен через Docker (ai_report_qdrant)"
    fi
fi

echo ""

# Статус Docker Compose
if [ -f "docker-compose.yml" ]; then
    echo "🐳 Docker Compose:"
    if command -v docker-compose &> /dev/null || command -v docker compose &> /dev/null; then
        docker-compose ps 2>/dev/null || docker compose ps 2>/dev/null || echo "  ⚠️  Не удалось получить статус"
    else
        echo "  ⚠️  Docker Compose не установлен"
    fi
fi

echo ""
echo "========================================"


