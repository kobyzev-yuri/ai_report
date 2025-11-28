#!/bin/bash
# Создание архива для деплоя
# Использование: ./create_deploy_archive.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DEPLOY_DIR="deploy"
ARCHIVE_NAME="ai_report_deploy_$(date +%Y%m%d_%H%M%S).tar.gz"

# Проверяем, что деплой подготовлен
if [ ! -d "$DEPLOY_DIR" ]; then
    echo "❌ Директория деплоя не найдена: $DEPLOY_DIR"
    echo "   Запустите сначала: ./prepare_deployment.sh"
    exit 1
fi

echo "📦 Создание архива деплоя..."
echo "   Источник: $DEPLOY_DIR"
echo "   Архив: $ARCHIVE_NAME"
echo ""

# Создаем архив
tar -czf "$ARCHIVE_NAME" \
    --exclude='*.log' \
    --exclude='*.pid' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='config.env' \
    --exclude='users.db' \
    "$DEPLOY_DIR"

# Проверяем размер
ARCHIVE_SIZE=$(du -h "$ARCHIVE_NAME" | cut -f1)

echo "✅ Архив создан: $ARCHIVE_NAME"
echo "   Размер: $ARCHIVE_SIZE"
echo ""
echo "📤 Для загрузки на сервер:"
echo "   scp -P \$DEPLOY_SERVER_PORT \$ARCHIVE_NAME \$DEPLOY_SERVER_HOST:/tmp/"
echo ""
echo "📦 Для распаковки на сервере:"
echo "   ssh -p \$DEPLOY_SERVER_PORT \$DEPLOY_SERVER_HOST"
echo ""
echo "⚠️  Установите переменные окружения перед использованием:"
echo "   export DEPLOY_SERVER_HOST=\"user@your-server-ip\""
echo "   export DEPLOY_SERVER_PORT=\"22\""
echo "   cd /usr/local/projects"
echo "   tar -xzf /tmp/$ARCHIVE_NAME"
echo "   mv deploy ai_report  # или переименуйте как нужно"
echo ""

