#!/bin/bash
# Скрипт для синхронизации минимального деплоя на сервер
# Использование: ./sync_deploy_to_server.sh [server_path]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Параметры сервера (можно переопределить через переменные окружения)
# ВАЖНО: Установите переменные окружения перед запуском:
# export DEPLOY_SERVER_HOST="user@your-server-ip"
# export DEPLOY_SERVER_PORT="22"
SERVER_HOST="${DEPLOY_SERVER_HOST}"
SERVER_PORT="${DEPLOY_SERVER_PORT:-22}"
SERVER_PATH="${1:-/usr/local/projects/ai_report}"

# Проверка обязательных параметров
if [ -z "$SERVER_HOST" ]; then
    echo "❌ Ошибка: DEPLOY_SERVER_HOST не установлен"
    echo "   Установите: export DEPLOY_SERVER_HOST=\"user@your-server-ip\""
    exit 1
fi

DEPLOY_DIR="deploy"

# Проверяем, что деплой подготовлен
if [ ! -d "$DEPLOY_DIR" ]; then
    echo "❌ Директория деплоя не найдена: $DEPLOY_DIR"
    echo "   Запустите сначала: ./prepare_deployment.sh"
    exit 1
fi

echo "🚀 Синхронизация деплоя на сервер"
echo "   Сервер: $SERVER_HOST:$SERVER_PORT"
echo "   Путь: $SERVER_PATH"
echo "   Источник: $DEPLOY_DIR"
echo ""

# Подтверждение
read -p "Продолжить? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Отменено"
    exit 1
fi

# Создаем временный файл с исключениями для rsync
RSYNC_EXCLUDE_FILE=$(mktemp)
cat > "$RSYNC_EXCLUDE_FILE" << 'EOF'
# Конфигурация с паролями (не перезаписываем)
config.env
users.db

# Логи и временные файлы
*.log
*.pid
__pycache__/
*.pyc
*.pyo
*.pyd

# Данные (загружаются через интерфейс)
data/SPNet reports/*
data/STECCOMLLCRussiaSBD.AccessFees_reports/*
!data/SPNet reports/.gitkeep
!data/STECCOMLLCRussiaSBD.AccessFees_reports/.gitkeep
EOF

echo "📤 Синхронизация файлов..."
rsync -avz --progress \
    -e "ssh -p $SERVER_PORT" \
    --exclude-from="$RSYNC_EXCLUDE_FILE" \
    --delete \
    "$DEPLOY_DIR/" \
    "$SERVER_HOST:$SERVER_PATH/"

# Удаляем временный файл
rm "$RSYNC_EXCLUDE_FILE"

echo ""
echo "✅ Синхронизация завершена!"
echo ""
echo "📝 Следующие шаги на сервере:"
echo "  1. Подключитесь: ssh -p $SERVER_PORT $SERVER_HOST"
echo "  2. Перейдите: cd $SERVER_PATH"
echo "  3. Создайте config.env (если еще нет): cp config.env.example config.env"
echo "  4. Установите зависимости: pip install -r requirements.txt"
echo "  5. Запустите: ./run_streamlit_background.sh"
echo ""

