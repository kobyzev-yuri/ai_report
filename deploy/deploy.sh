#!/bin/bash
# Скрипт развертывания RAG системы с Qdrant и Streamlit
# Использование: ./deploy.sh [docker|manual]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DEPLOY_MODE="${1:-docker}"

echo "========================================"
echo "Развертывание RAG системы"
echo "========================================"
echo "Режим: $DEPLOY_MODE"
echo ""

# Проверка config.env
if [ ! -f "config.env" ]; then
    echo "❌ Файл config.env не найден!"
    echo "Создайте его из config.env.example:"
    echo "  cp config.env.example config.env"
    echo "  nano config.env"
    exit 1
fi

# Загрузка конфигурации с экспортом переменных
echo "Загрузка переменных окружения из config.env..."
set -a  # Автоматически экспортировать все переменные
source config.env
set +a  # Отключить автоматический экспорт
echo "✅ Переменные окружения загружены"

if [ "$DEPLOY_MODE" = "docker" ]; then
    echo "🐳 Развертывание через Docker Compose..."
    echo ""
    
    # Проверка Docker
    if ! command -v docker &> /dev/null; then
        echo "❌ Docker не установлен!"
        exit 1
    fi
    
    # Определение команды docker-compose
    if command -v docker-compose &> /dev/null; then
        DOCKER_COMPOSE_CMD="docker-compose"
    elif docker compose version &> /dev/null; then
        DOCKER_COMPOSE_CMD="docker compose"
    else
        echo "❌ Docker Compose не установлен!"
        exit 1
    fi
    
    echo "Используется: $DOCKER_COMPOSE_CMD"
    
    # Переменные окружения уже загружены выше из config.env
    echo "Переменные окружения Oracle (проверка):"
    echo "  ORACLE_USER=${ORACLE_USER:-не установлено}"
    echo "  ORACLE_HOST=${ORACLE_HOST:-не установлено}"
    echo "  ORACLE_SID=${ORACLE_SID:-не установлено}"
    
    # Остановка существующих контейнеров
    echo "Остановка существующих контейнеров..."
    $DOCKER_COMPOSE_CMD down 2>/dev/null || true
    
    # Сборка и запуск
    echo "Сборка и запуск контейнеров..."
    $DOCKER_COMPOSE_CMD up -d --build
    
    echo ""
    echo "✅ Развертывание завершено!"
    echo ""
    echo "Проверка статуса:"
    echo "  docker-compose ps"
    echo ""
    echo "Логи:"
    echo "  docker-compose logs -f streamlit"
    echo "  docker-compose logs -f qdrant"
    echo ""
    echo "Остановка:"
    echo "  docker-compose down"
    
elif [ "$DEPLOY_MODE" = "manual" ]; then
    echo "📋 Ручное развертывание..."
    echo ""
    
    # Проверка зависимостей
    echo "Проверка зависимостей..."
    if ! command -v python3 &> /dev/null; then
        echo "❌ Python3 не установлен!"
        exit 1
    fi
    
    if ! command -v streamlit &> /dev/null; then
        echo "⚠️  Streamlit не найден. Установка зависимостей..."
        pip install -r requirements.txt
    fi
    
    # Запуск Qdrant через Docker (если доступен)
    if command -v docker &> /dev/null; then
        echo "Запуск Qdrant через Docker..."
        if ! docker ps | grep -q ai_report_qdrant; then
            docker run -d \
                --name ai_report_qdrant \
                --restart unless-stopped \
                -p 6333:6333 \
                -p 6334:6334 \
                -v qdrant_storage:/qdrant/storage \
                qdrant/qdrant:latest
            echo "✅ Qdrant запущен"
        else
            echo "ℹ️  Qdrant уже запущен"
        fi
    else
        echo "⚠️  Docker не найден. Убедитесь, что Qdrant запущен на localhost:6333"
    fi
    
    # Инициализация KB
    echo ""
    echo "Инициализация KB в Qdrant..."
    python3 kb_billing/rag/init_kb.py \
        --host "${QDRANT_HOST:-localhost}" \
        --port "${QDRANT_PORT:-6333}" \
        --collection "${QDRANT_COLLECTION:-kb_billing}" \
        --recreate
    
    # Запуск Streamlit
    echo ""
    echo "Запуск Streamlit..."
    ./run_streamlit_background.sh
    
    echo ""
    echo "✅ Развертывание завершено!"
    echo ""
    echo "Проверка статуса:"
    echo "  ./status_streamlit.sh"
    echo ""
    echo "Логи:"
    echo "  tail -f streamlit_8504.log"
    
else
    echo "❌ Неизвестный режим: $DEPLOY_MODE"
    echo "Использование: ./deploy.sh [docker|manual]"
    exit 1
fi

