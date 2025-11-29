#!/bin/bash
# Безопасное развертывание на сервере
# Использование: ./safe_deploy.sh [user@]host [ssh_command]
# Примеры:
#   ./safe_deploy.sh root@82.114.2.2
#   SSH_CMD="ssh -p 1194" ./safe_deploy.sh root@82.114.2.2
#   SSH_CMD="vz2" ./safe_deploy.sh root@82.114.2.2

set -e

if [ -z "$1" ]; then
    echo "Использование: $0 [user@]host [ssh_command]"
    echo "Примеры:"
    echo "  $0 root@82.114.2.2"
    echo "  SSH_CMD='ssh -p 1194' $0 root@82.114.2.2"
    echo "  SSH_CMD='vz2' $0 root@82.114.2.2"
    exit 1
fi

SERVER="$1"
SSH_CMD="${SSH_CMD:-${2:-ssh}}"
REMOTE_DIR="/usr/local/projects/ai_report"

echo "========================================"
echo "Безопасное развертывание RAG системы"
echo "========================================"
echo "Сервер: $SERVER"
echo "SSH команда: $SSH_CMD"
echo "Директория: $REMOTE_DIR"
echo ""

echo "========================================"
echo "Безопасное развертывание RAG системы"
echo "========================================"
echo "Сервер: $SERVER"
echo "Директория: $REMOTE_DIR"
echo ""

# Шаг 1: Обследование
echo "Шаг 1: Обследование сервера..."
echo "========================================"
if [ -f "server_inspection.sh" ]; then
    SSH_CMD="$SSH_CMD" ./server_inspection.sh "$SERVER"
else
    echo "⚠️  Скрипт server_inspection.sh не найден, пропускаем обследование"
fi

echo ""
read -p "Продолжить развертывание? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Развертывание отменено"
    exit 1
fi

# Шаг 2: Синхронизация
echo ""
echo "Шаг 2: Синхронизация файлов..."
echo "========================================"
if [ -f "sync_deploy.sh" ]; then
    SSH_CMD="$SSH_CMD" ./sync_deploy.sh "$SERVER"
else
    echo "⚠️  Скрипт sync_deploy.sh не найден"
    echo "Выполните синхронизацию вручную"
fi

# Шаг 3: Остановка старого Streamlit
echo ""
echo "Шаг 3: Остановка старого Streamlit..."
echo "========================================"
echo "Проверка запущенного Streamlit..."

STREAMLIT_STOPPED=false

# Проверка через PID файл
if $SSH_CMD "$SERVER" "[ -f '$REMOTE_DIR/streamlit_8504.pid' ]" 2>/dev/null; then
    PID=$($SSH_CMD "$SERVER" "cat $REMOTE_DIR/streamlit_8504.pid 2>/dev/null")
    if $SSH_CMD "$SERVER" "ps -p $PID >/dev/null 2>&1" 2>/dev/null; then
        echo "⚠️  Найден запущенный Streamlit через PID файл (PID: $PID)"
        echo "Остановка..."
        $SSH_CMD "$SERVER" "kill $PID && rm -f $REMOTE_DIR/streamlit_8504.pid" || true
        sleep 2
        STREAMLIT_STOPPED=true
        echo "✅ Streamlit остановлен"
    else
        echo "ℹ️  Процесс из PID файла не найден, удаляем старый PID файл"
        $SSH_CMD "$SERVER" "rm -f $REMOTE_DIR/streamlit_8504.pid" || true
    fi
fi

# Проверка процессов на порту 8504
if [ "$STREAMLIT_STOPPED" = false ]; then
    echo "Проверка процессов на порту 8504..."
    # Попытка найти процесс через lsof или fuser
    PID_ON_PORT=$($SSH_CMD "$SERVER" "lsof -ti :8504 2>/dev/null || fuser 8504/tcp 2>/dev/null | grep -o '[0-9]*' | head -1" || echo "")
    
    if [ -n "$PID_ON_PORT" ]; then
        # Проверяем, что это действительно Streamlit
        PROCESS_CMD=$($SSH_CMD "$SERVER" "ps -p $PID_ON_PORT -o cmd --no-headers 2>/dev/null" || echo "")
        if echo "$PROCESS_CMD" | grep -q "streamlit"; then
            echo "⚠️  Найден запущенный Streamlit на порту 8504 (PID: $PID_ON_PORT)"
            echo "Команда: $PROCESS_CMD"
            echo "Остановка..."
            $SSH_CMD "$SERVER" "kill $PID_ON_PORT" || true
            sleep 2
            STREAMLIT_STOPPED=true
            echo "✅ Streamlit остановлен"
        else
            echo "ℹ️  На порту 8504 найден другой процесс (не Streamlit)"
        fi
    fi
fi

if [ "$STREAMLIT_STOPPED" = false ]; then
    echo "✅ Streamlit не запущен или уже остановлен"
fi

# Проверка порта 8504
echo ""
echo "Проверка порта 8504..."
if $SSH_CMD "$SERVER" "netstat -tuln 2>/dev/null | grep ':8504' || ss -tuln 2>/dev/null | grep ':8504'" 2>/dev/null; then
    echo "⚠️  Порт 8504 все еще занят"
    echo "Проверьте процессы:"
    $SSH_CMD "$SERVER" "lsof -i :8504 2>/dev/null || fuser 8504/tcp 2>/dev/null || echo 'Не удалось определить процесс'"
    read -p "Продолжить? Порт может быть занят другим процессом (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
else
    echo "✅ Порт 8504 свободен"
fi

# Шаг 4: Проверка config.env
echo ""
echo "Шаг 4: Проверка конфигурации..."
echo "========================================"
if $SSH_CMD "$SERVER" "[ -f '$REMOTE_DIR/config.env' ]" 2>/dev/null; then
    echo "✅ config.env найден"
    echo "Проверка Oracle настроек..."
    $SSH_CMD "$SERVER" "grep -E '^ORACLE_' $REMOTE_DIR/config.env | sed 's/PASSWORD=.*/PASSWORD=***/' || echo 'Oracle настройки не найдены'"
else
    echo "⚠️  config.env не найден"
    echo "Создание из примера..."
    $SSH_CMD "$SERVER" "cd $REMOTE_DIR && cp config.env.example config.env"
    echo "⚠️  ВАЖНО: Отредактируйте config.env на сервере перед развертыванием!"
    echo "  ssh $SERVER 'nano $REMOTE_DIR/config.env'"
    read -p "Продолжить развертывание? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Шаг 5: Развертывание
echo ""
echo "Шаг 5: Развертывание контейнеров..."
echo "========================================"
echo "Выберите метод развертывания:"
echo "1) Docker Compose (рекомендуется)"
echo "2) Ручное развертывание"
read -p "Выбор (1/2): " -n 1 -r
echo

if [[ $REPLY =~ ^[1]$ ]]; then
    echo "Развертывание через Docker Compose..."
    echo "Запуск развертывания на сервере..."
    # deploy.sh сам загрузит config.env
    $SSH_CMD "$SERVER" "cd $REMOTE_DIR && ./deploy.sh docker"
elif [[ $REPLY =~ ^[2]$ ]]; then
    echo "Ручное развертывание..."
    echo "На сервере выполните:"
    echo "  ssh $SERVER"
    echo "  cd $REMOTE_DIR"
    echo "  ./deploy.sh manual"
else
    echo "Неверный выбор"
    exit 1
fi

# Шаг 6: Проверка развертывания
echo ""
echo "Шаг 6: Проверка развертывания..."
echo "========================================"
sleep 5

echo "Проверка контейнеров..."
$SSH_CMD "$SERVER" "docker ps --filter 'name=ai_report' --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'"

echo ""
echo "Проверка Qdrant..."
if $SSH_CMD "$SERVER" "curl -s http://localhost:6333/health >/dev/null 2>&1" 2>/dev/null; then
    echo "✅ Qdrant доступен"
else
    echo "⚠️  Qdrant недоступен"
fi

echo ""
echo "Проверка Streamlit..."
if $SSH_CMD "$SERVER" "curl -s http://localhost:8504/_stcore/health >/dev/null 2>&1" 2>/dev/null; then
    echo "✅ Streamlit доступен"
else
    echo "⚠️  Streamlit недоступен (может еще запускаться)"
fi

echo ""
echo "========================================"
echo "Развертывание завершено"
echo "========================================"
echo ""
echo "Проверка статуса:"
echo "  ssh $SERVER 'cd $REMOTE_DIR && ./status_all.sh'"
echo ""
echo "Логи:"
echo "  ssh $SERVER 'cd $REMOTE_DIR && docker-compose logs -f streamlit'"
echo ""
echo "Веб-интерфейс:"
echo "  http://82.114.2.2:8504 (или через nginx proxy)"
echo ""

