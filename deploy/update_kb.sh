#!/bin/bash
# Обновление KB без пересоздания коллекции (добавление новых данных)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================"
echo "Обновление KB"
echo "========================================"

# Загрузка конфигурации
if [ -f "config.env" ]; then
    source config.env
fi

QDRANT_HOST="${QDRANT_HOST:-localhost}"
QDRANT_PORT="${QDRANT_PORT:-6333}"
QDRANT_COLLECTION="${QDRANT_COLLECTION:-kb_billing}"
EMBEDDING_MODEL="${EMBEDDING_MODEL:-intfloat/multilingual-e5-base}"

echo "Qdrant: $QDRANT_HOST:$QDRANT_PORT"
echo "Коллекция: $QDRANT_COLLECTION"
echo ""

# Проверка подключения к Qdrant
echo "Проверка подключения к Qdrant..."
if ! curl -s "http://$QDRANT_HOST:$QDRANT_PORT/health" > /dev/null 2>&1; then
    echo "❌ Qdrant недоступен на $QDRANT_HOST:$QDRANT_PORT"
    exit 1
fi

# Обновление KB (без --recreate)
echo "Обновление KB..."
python3 kb_billing/rag/init_kb.py \
    --host "$QDRANT_HOST" \
    --port "$QDRANT_PORT" \
    --collection "$QDRANT_COLLECTION" \
    --model "$EMBEDDING_MODEL"

echo ""
echo "✅ KB обновлена"












