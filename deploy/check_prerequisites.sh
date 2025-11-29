#!/bin/bash
# Проверка предварительных требований для развертывания

echo "========================================"
echo "Проверка предварительных требований"
echo "========================================"
echo ""

ERRORS=0

# Проверка SSH
echo "1. Проверка SSH..."
if command -v ssh >/dev/null 2>&1; then
    echo "   ✅ SSH установлен"
else
    echo "   ❌ SSH не установлен"
    ERRORS=$((ERRORS + 1))
fi

# Проверка rsync
echo "2. Проверка rsync..."
if command -v rsync >/dev/null 2>&1; then
    echo "   ✅ rsync установлен"
else
    echo "   ❌ rsync не установлен (нужен для синхронизации)"
    echo "      Установите: sudo apt-get install rsync"
    ERRORS=$((ERRORS + 1))
fi

# Проверка Docker (локально, для тестирования)
echo "3. Проверка Docker (локально)..."
if command -v docker >/dev/null 2>&1; then
    echo "   ✅ Docker установлен локально"
    docker --version | sed 's/^/      /'
else
    echo "   ⚠️  Docker не установлен локально (не критично для развертывания)"
fi

# Проверка файлов
echo "4. Проверка необходимых файлов..."
REQUIRED_FILES=(
    "docker-compose.yml"
    "Dockerfile.streamlit"
    "deploy.sh"
    "init_kb.sh"
    "server_inspection.sh"
    "sync_deploy.sh"
    "safe_deploy.sh"
)

for file in "${REQUIRED_FILES[@]}"; do
    if [ -f "$file" ]; then
        echo "   ✅ $file"
    else
        echo "   ❌ $file не найден"
        ERRORS=$((ERRORS + 1))
    fi
done

# Проверка RAG модулей
echo "5. Проверка RAG модулей..."
RAG_MODULES=(
    "../kb_billing/rag/kb_loader.py"
    "../kb_billing/rag/rag_assistant.py"
    "../kb_billing/rag/streamlit_assistant.py"
    "../kb_billing/rag/config_sql4a.py"
    "../kb_billing/rag/init_kb.py"
)

for module in "${RAG_MODULES[@]}"; do
    if [ -f "$module" ]; then
        echo "   ✅ $(basename $module)"
    else
        echo "   ❌ $module не найден"
        ERRORS=$((ERRORS + 1))
    fi
done

echo ""
echo "========================================"
if [ $ERRORS -eq 0 ]; then
    echo "✅ Все проверки пройдены"
    echo "Готово к развертыванию!"
else
    echo "❌ Найдено ошибок: $ERRORS"
    echo "Исправьте ошибки перед развертыванием"
    exit 1
fi
echo "========================================"

