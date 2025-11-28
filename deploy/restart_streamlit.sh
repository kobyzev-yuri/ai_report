#!/bin/bash
# Аккуратный перезапуск Streamlit с очисткой кэша
# Использование: ./restart_streamlit.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PORT=8504
PID_FILE="$SCRIPT_DIR/streamlit_${PORT}.pid"
LOG_FILE="$SCRIPT_DIR/streamlit_${PORT}.log"
CACHE_DIR="$HOME/.streamlit/cache"

echo "========================================"
echo "Перезапуск Streamlit с очисткой кэша"
echo "========================================"
echo ""

# Шаг 1: Проверка статуса
echo "1. Проверка текущего статуса..."
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "   ✅ Streamlit запущен (PID: $PID)"
    else
        echo "   ⚠️  Процесс не найден (PID файл существует)"
    fi
else
    echo "   ⚠️  Streamlit не запущен (PID файл не найден)"
fi
echo ""

# Шаг 2: Остановка Streamlit
echo "2. Остановка Streamlit..."
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "   Отправка сигнала SIGTERM процессу $PID..."
        kill "$PID"
        sleep 3
        
        # Проверяем, остановился ли процесс
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "   Процесс не остановился, отправка SIGKILL..."
            kill -9 "$PID"
            sleep 1
        fi
        
        rm "$PID_FILE"
        echo "   ✅ Streamlit остановлен"
    else
        echo "   ⚠️  Процесс не найден, удаляю PID файл"
        rm "$PID_FILE"
    fi
else
    echo "   ⚠️  PID файл не найден, проверка процессов..."
    pkill -f "streamlit.*8504" && echo "   ✅ Процессы остановлены" || echo "   ℹ️  Процессы не найдены"
fi
echo ""

# Шаг 3: Очистка кэша Streamlit
echo "3. Очистка кэша Streamlit..."
if [ -d "$CACHE_DIR" ]; then
    echo "   Удаление кэша из: $CACHE_DIR"
    rm -rf "$CACHE_DIR"/* 2>/dev/null
    echo "   ✅ Кэш очищен"
else
    echo "   ⚠️  Директория кэша не найдена: $CACHE_DIR"
fi

# Также очищаем кэш в текущей директории (если есть)
if [ -d ".streamlit/cache" ]; then
    echo "   Удаление локального кэша..."
    rm -rf ".streamlit/cache"/* 2>/dev/null
    echo "   ✅ Локальный кэш очищен"
fi
echo ""

# Шаг 4: Ожидание перед запуском
echo "4. Ожидание 2 секунды перед запуском..."
sleep 2
echo ""

# Шаг 5: Запуск Streamlit
echo "5. Запуск Streamlit..."
if [ -f "run_streamlit_background.sh" ]; then
    echo "   Использование скрипта run_streamlit_background.sh..."
    ./run_streamlit_background.sh
else
    echo "   ⚠️  Скрипт run_streamlit_background.sh не найден"
    echo "   Запуск вручную..."
    
    # Загрузка конфигурации
    if [ -f "config.env" ]; then
        echo "   Загрузка конфигурации из config.env..."
        set -a
        source "config.env"
        set +a
    fi
    
    BASE_URL_PATH="/ai_report"
    STREAMLIT_ARGS="--server.port $PORT --server.headless true --server.baseUrlPath=${BASE_URL_PATH} --server.enableCORS false --server.enableXsrfProtection false"
    
    nohup streamlit run streamlit_report_oracle_backup.py ${STREAMLIT_ARGS} > "$LOG_FILE" 2>&1 &
    STREAMLIT_PID=$!
    echo $STREAMLIT_PID > "$PID_FILE"
    
    sleep 2
    if ps -p "$STREAMLIT_PID" > /dev/null 2>&1; then
        echo "   ✅ Streamlit запущен (PID: $STREAMLIT_PID)"
    else
        echo "   ❌ Ошибка запуска Streamlit!"
        echo "   Проверьте логи: cat $LOG_FILE"
        rm "$PID_FILE"
        exit 1
    fi
fi
echo ""

# Шаг 6: Проверка статуса
echo "6. Проверка статуса после запуска..."
sleep 3
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "   ✅ Streamlit успешно запущен"
        echo "   PID: $PID"
        echo "   Логи: $LOG_FILE"
        echo ""
        echo "   Просмотр логов: tail -f $LOG_FILE"
        echo "   Проверка статуса: ./status_streamlit.sh"
        echo ""
        echo "   URL: stat.steccom.ru:7776/ai_report"
    else
        echo "   ❌ Процесс не запущен!"
        echo "   Проверьте логи: cat $LOG_FILE"
        exit 1
    fi
else
    echo "   ⚠️  PID файл не создан"
fi

echo ""
echo "========================================"
echo "Перезапуск завершен"
echo "========================================"

