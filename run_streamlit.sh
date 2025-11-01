#!/bin/bash
# Запуск Streamlit приложения для отчета по Iridium M2M

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Загрузка конфигурации из config.env
if [ -f "$SCRIPT_DIR/config.env" ]; then
    echo "Загрузка конфигурации из config.env..."
    set -a  # автоматически export всех переменных
    source "$SCRIPT_DIR/config.env"
    set +a
    echo "✅ Конфигурация загружена"
else
    echo "⚠️  ВНИМАНИЕ: Файл config.env не найден!"
    echo "   Создайте config.env на основе config.env.example"
    echo "   cp config.env.example config.env"
    echo "   и заполните реальными значениями"
    exit 1
fi

echo "========================================"
echo "Запуск Streamlit приложения"
echo "========================================"
echo ""
echo "URL: http://localhost:8502"
echo ""
echo "Database: ${POSTGRES_USER}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"
echo ""

streamlit run streamlit_report.py --server.port 8502



