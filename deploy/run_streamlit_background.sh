#!/bin/bash
# Запуск Streamlit приложения в фоновом режиме на удаленной машине
# Использование: ./run_streamlit_background.sh [oracle|postgresql] [port]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Всегда Oracle на порту 8504
APP_FILE="streamlit_report_oracle_backup.py"
PORT=8504

# Загрузка конфигурации из config.env
if [ -f "$SCRIPT_DIR/config.env" ]; then
    echo "Загрузка конфигурации из config.env..."
    set -a
    source "$SCRIPT_DIR/config.env"
    set +a
    echo "✅ Конфигурация загружена"
else
    echo "⚠️  ВНИМАНИЕ: Файл config.env не найден!"
    exit 1
fi

# Всегда proxy режим
BASE_URL_PATH="/ai_report"

# Логи
LOG_FILE="$SCRIPT_DIR/streamlit_${PORT}.log"
PID_FILE="$SCRIPT_DIR/streamlit_${PORT}.pid"

# Проверяем, не запущен ли уже процесс
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if ps -p "$OLD_PID" > /dev/null 2>&1; then
        echo "⚠️  Streamlit уже запущен (PID: $OLD_PID)"
        echo "   Остановите его: kill $OLD_PID"
        echo "   Или удалите файл: rm $PID_FILE"
        exit 1
    else
        echo "Удаляю старый PID файл..."
        rm "$PID_FILE"
    fi
fi

# Параметры запуска - всегда с baseUrlPath для proxy
STREAMLIT_ARGS="--server.port $PORT --server.headless true --server.baseUrlPath=${BASE_URL_PATH} --server.enableCORS false --server.enableXsrfProtection false"

echo "🌐 Режим: Проксирование через nginx"
echo "   Base URL Path: ${BASE_URL_PATH}"

echo "========================================"
echo "Запуск Streamlit приложения (Oracle)"
echo "========================================"
echo ""
echo "Файл: $APP_FILE"
echo "Порт: $PORT"
echo "Логи: $LOG_FILE"
echo "PID файл: $PID_FILE"
echo ""

# Запуск в фоне
nohup streamlit run "$APP_FILE" ${STREAMLIT_ARGS} > "$LOG_FILE" 2>&1 &
STREAMLIT_PID=$!

# Сохраняем PID
echo $STREAMLIT_PID > "$PID_FILE"

# Ждем немного и проверяем, что процесс запустился
sleep 2
if ps -p "$STREAMLIT_PID" > /dev/null 2>&1; then
    echo "✅ Streamlit запущен успешно!"
    echo ""
    echo "PID: $STREAMLIT_PID"
    echo "Логи: tail -f $LOG_FILE"
    echo ""
    echo "Внешний URL: stat.steccom.ru:7776${BASE_URL_PATH}"
    echo ""
    echo "Остановить: kill $STREAMLIT_PID"
    echo "Или: ./stop_streamlit.sh $DB_TYPE $PORT"
else
    echo "❌ Ошибка запуска Streamlit!"
    echo "Проверьте логи: cat $LOG_FILE"
    rm "$PID_FILE"
    exit 1
fi


