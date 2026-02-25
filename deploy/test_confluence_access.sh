#!/bin/bash
# Проверка доступа к docs.steccom.ru с сервера (vz2) после деплоя.
# Нужен CONFLUENCE_URL и CONFLUENCE_TOKEN в config.env.
# После успешного теста можно переходить к интеграции реальных разделов Confluence.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Корень проекта: родитель deploy/ или текущая директория, если мы уже в корне
if [[ "$SCRIPT_DIR" == *"/deploy" ]]; then
    PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
else
    PROJECT_ROOT="$(cd "$SCRIPT_DIR" && pwd)"
fi

cd "$PROJECT_ROOT"

# Конфиг: в корне проекта или в deploy/
if [ -f "$PROJECT_ROOT/config.env" ]; then
    CONFIG_ENV="$PROJECT_ROOT/config.env"
elif [ -f "$SCRIPT_DIR/config.env" ]; then
    CONFIG_ENV="$SCRIPT_DIR/config.env"
else
    echo "❌ config.env не найден в $PROJECT_ROOT и в $SCRIPT_DIR"
    echo "   Добавьте CONFLUENCE_URL и CONFLUENCE_TOKEN в config.env для теста Confluence."
    exit 1
fi

echo "=============================================="
echo "  Тест доступа к Confluence (docs.steccom.ru)"
echo "  Сервер: $(hostname 2>/dev/null || echo 'vz2')"
echo "  Конфиг: $CONFIG_ENV"
echo "=============================================="
set -a
source "$CONFIG_ENV"
set +a

if [ -z "$CONFLUENCE_URL" ]; then
    echo "⚠️  CONFLUENCE_URL не задан в config.env (например CONFLUENCE_URL=https://docs.steccom.ru)"
fi
if [ -z "$CONFLUENCE_TOKEN" ]; then
    echo "⚠️  CONFLUENCE_TOKEN не задан в config.env"
fi

echo ""
python3 "$PROJECT_ROOT/scripts/test_confluence_connection.py"
EXIT=$?

echo ""
if [ "$EXIT" -eq 0 ]; then
    echo "=============================================="
    echo "  ✅ Доступ к docs.steccom.ru с сервера есть."
    echo "  Дальше: тест интеграции реальных разделов"
    echo "  (вкладка «Спутниковый библиотекарь» или sync вручную)."
    echo "=============================================="
else
    echo "=============================================="
    echo "  ❌ Ошибка доступа. Проверьте:"
    echo "  - доступность docs.steccom.ru с vz2 (сеть, firewall)"
    echo "  - CONFLUENCE_TOKEN в config.env (актуальный PAT)"
    echo "=============================================="
fi
exit $EXIT
