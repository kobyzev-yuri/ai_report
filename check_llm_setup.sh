#!/bin/bash
echo "=========================================="
echo "Проверка настройки LLM"
echo "=========================================="
echo ""

# Проверка библиотеки
echo "1. Проверка библиотеки openai:"
if python3 -c "from openai import OpenAI" 2>/dev/null; then
    echo "   ✅ Библиотека openai установлена"
else
    echo "   ❌ Библиотека openai не установлена"
    echo "   Установите: pip3 install openai"
fi
echo ""

# Проверка config.env
echo "2. Проверка config.env:"
if grep -q "^OPENAI_API_KEY=" config.env 2>/dev/null; then
    API_KEY=$(grep "^OPENAI_API_KEY=" config.env | cut -d'=' -f2)
    if [ -n "$API_KEY" ] && [ "$API_KEY" != "your-openai-api-key" ]; then
        echo "   ✅ OPENAI_API_KEY установлен"
    else
        echo "   ⚠️  OPENAI_API_KEY установлен, но не заполнен"
    fi
else
    echo "   ❌ OPENAI_API_KEY не найден в config.env"
fi

if grep -q "^OPENAI_API_BASE=" config.env 2>/dev/null; then
    API_BASE=$(grep "^OPENAI_API_BASE=" config.env | cut -d'=' -f2)
    if [ -n "$API_BASE" ] && [ "$API_BASE" != "" ]; then
        echo "   ✅ OPENAI_API_BASE установлен: $API_BASE"
    else
        echo "   ℹ️  OPENAI_API_BASE не установлен (будет использован стандартный OpenAI API)"
    fi
else
    echo "   ℹ️  OPENAI_API_BASE не найден в config.env"
fi
echo ""

echo "=========================================="
echo "Для настройки LLM добавьте в config.env:"
echo "OPENAI_API_KEY=your-api-key"
echo "OPENAI_API_BASE=https://api.proxyapi.ru/openai/v1  # опционально"
echo "=========================================="
