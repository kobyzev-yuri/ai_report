#!/bin/bash
# Скрипт инициализации KB в Qdrant
# Использование: ./init_kb.sh [--recreate]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Загрузка конфигурации
if [ -f "config.env" ]; then
    source config.env
fi

QDRANT_HOST="${QDRANT_HOST:-localhost}"
QDRANT_PORT="${QDRANT_PORT:-6333}"
QDRANT_COLLECTION="${QDRANT_COLLECTION:-kb_billing}"
EMBEDDING_MODEL="${EMBEDDING_MODEL:-intfloat/multilingual-e5-base}"

RECREATE_FLAG=""
if [[ "$*" == *"--recreate"* ]]; then
    RECREATE_FLAG="--recreate"
fi

echo "========================================"
echo "Инициализация KB в Qdrant"
echo "========================================"
echo "Qdrant: $QDRANT_HOST:$QDRANT_PORT"
echo "Коллекция: $QDRANT_COLLECTION"
echo "Модель: $EMBEDDING_MODEL"
echo ""

# Проверка подключения к Qdrant
echo "Проверка подключения к Qdrant..."
if command -v curl &> /dev/null; then
    if curl -s "http://$QDRANT_HOST:$QDRANT_PORT/health" > /dev/null 2>&1; then
        echo "✅ Qdrant доступен"
    else
        echo "❌ Qdrant недоступен на $QDRANT_HOST:$QDRANT_PORT"
        echo "Убедитесь, что Qdrant запущен:"
        echo "  docker run -d -p 6333:6333 qdrant/qdrant"
        exit 1
    fi
else
    echo "⚠️  curl не найден, пропускаем проверку"
fi

# Проверка Python и зависимостей
echo ""
echo "Проверка зависимостей..."
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 не установлен!"
    exit 1
fi

# Проверка модулей
if ! python3 -c "import qdrant_client" 2>/dev/null; then
    echo "⚠️  qdrant-client не установлен. Установка..."
    pip install qdrant-client sentence-transformers torch transformers
fi

# Инициализация KB
echo ""
echo "Инициализация KB..."
python3 kb_billing/rag/init_kb.py \
    --host "$QDRANT_HOST" \
    --port "$QDRANT_PORT" \
    --collection "$QDRANT_COLLECTION" \
    --model "$EMBEDDING_MODEL" \
    $RECREATE_FLAG

echo ""
echo "✅ Инициализация KB завершена!"



