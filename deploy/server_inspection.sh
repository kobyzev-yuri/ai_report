#!/bin/bash
# Скрипт безопасного обследования сервера перед развертыванием
# Использование: ./server_inspection.sh [user@]host [ssh_command]
# Примеры:
#   ./server_inspection.sh root@82.114.2.2
#   ./server_inspection.sh root@82.114.2.2 "ssh -p 1194"
#   SSH_CMD="ssh -p 1194" ./server_inspection.sh root@82.114.2.2

set -e

if [ -z "$1" ]; then
    echo "Использование: $0 [user@]host [ssh_command]"
    echo "Примеры:"
    echo "  $0 root@82.114.2.2"
    echo "  $0 root@82.114.2.2 'ssh -p 1194'"
    echo "  SSH_CMD='ssh -p 1194' $0 root@82.114.2.2"
    echo ""
    echo "Если используете алиас (например, vz2='ssh -p 1194 root@82.114.2.2'):"
    echo "  SSH_CMD='vz2' $0 root@82.114.2.2"
    exit 1
fi

SERVER="$1"
# SSH_CMD может быть установлен через переменную окружения или второй параметр
# Если не установлен, используем стандартный ssh
if [ -z "$SSH_CMD" ] && [ -n "$2" ]; then
    SSH_CMD="$2"
elif [ -z "$SSH_CMD" ]; then
    SSH_CMD="ssh"
fi
REMOTE_DIR="/usr/local/projects/ai_report"

echo "========================================"
echo "Обследование сервера: $SERVER"
echo "========================================"
echo "SSH команда: $SSH_CMD"
echo ""
echo "💡 Если подключение зависает, используйте:"
echo "   SSH_CMD='ssh -p 1194' $0 $SERVER"
echo "   или"
echo "   SSH_CMD='vz2' $0 $SERVER"
echo ""

# Проверка подключения
echo "1. Проверка подключения к серверу..."
# Добавляем таймаут для SSH команды
if [[ "$SSH_CMD" == "ssh" ]]; then
    # Стандартный ssh с таймаутом
    if timeout 5 ssh -o ConnectTimeout=5 -o BatchMode=yes "$SERVER" "echo 'OK'" 2>/dev/null; then
        echo "✅ Подключение успешно"
    else
        echo "❌ Не удалось подключиться к серверу"
        echo "Проверьте SSH команду и доступ"
        echo ""
        echo "Если используете кастомный порт или алиас:"
        echo "  SSH_CMD='ssh -p 1194' $0 $SERVER"
        echo "  или"
        echo "  SSH_CMD='vz2' $0 $SERVER"
        exit 1
    fi
else
    # Кастомная SSH команда (может быть алиас или команда с портом)
    if timeout 5 $SSH_CMD "$SERVER" "echo 'OK'" 2>/dev/null; then
        echo "✅ Подключение успешно"
    else
        echo "❌ Не удалось подключиться к серверу"
        echo "Используемая SSH команда: $SSH_CMD"
        echo ""
        echo "Проверьте подключение вручную:"
        echo "  $SSH_CMD $SERVER 'echo OK'"
        exit 1
    fi
fi

echo ""

# Проверка Docker
echo "2. Проверка Docker..."
if $SSH_CMD "$SERVER" "command -v docker >/dev/null 2>&1 && docker --version" 2>/dev/null; then
    echo "✅ Docker установлен"
    $SSH_CMD "$SERVER" "docker --version"
    echo ""
    echo "Статус Docker:"
    $SSH_CMD "$SERVER" "systemctl status docker --no-pager -l | head -5" 2>/dev/null || $SSH_CMD "$SERVER" "docker info | head -10"
else
    echo "❌ Docker не найден"
    exit 1
fi

echo ""

# Проверка Docker Compose
echo "3. Проверка Docker Compose..."
if $SSH_CMD "$SERVER" "command -v docker-compose >/dev/null 2>&1" 2>/dev/null; then
    echo "✅ docker-compose установлен"
    $SSH_CMD "$SERVER" "docker-compose --version"
elif $SSH_CMD "$SERVER" "docker compose version >/dev/null 2>&1" 2>/dev/null; then
    echo "✅ docker compose (v2) установлен"
    $SSH_CMD "$SERVER" "docker compose version"
else
    echo "⚠️  Docker Compose не найден (будет использоваться docker compose v2)"
fi

echo ""

# Проверка текущего состояния
echo "4. Проверка текущего состояния..."
echo ""

# Проверка директории
if $SSH_CMD "$SERVER" "[ -d '$REMOTE_DIR' ]" 2>/dev/null; then
    echo "✅ Директория $REMOTE_DIR существует"
    echo "Содержимое:"
    $SSH_CMD "$SERVER" "ls -la $REMOTE_DIR | head -20"
else
    echo "⚠️  Директория $REMOTE_DIR не найдена (будет создана)"
fi

echo ""

# Проверка запущенных контейнеров
echo "5. Проверка запущенных контейнеров..."
echo "Контейнеры Qdrant:"
$SSH_CMD "$SERVER" "docker ps --filter 'name=qdrant' --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'" 2>/dev/null || echo "Нет контейнеров Qdrant"
echo ""
echo "Контейнеры Streamlit:"
$SSH_CMD "$SERVER" "docker ps --filter 'name=streamlit' --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'" 2>/dev/null || echo "Нет контейнеров Streamlit"
echo ""

# Проверка портов
echo "6. Проверка занятых портов..."
echo "Порт 8504 (Streamlit):"
$SSH_CMD "$SERVER" "netstat -tuln 2>/dev/null | grep ':8504' || ss -tuln 2>/dev/null | grep ':8504' || echo 'Порт 8504 свободен или проверка недоступна'"
echo ""
echo "Порт 6333 (Qdrant):"
$SSH_CMD "$SERVER" "netstat -tuln 2>/dev/null | grep ':6333' || ss -tuln 2>/dev/null | grep ':6333' || echo 'Порт 6333 свободен'"
echo ""

# Проверка процессов Streamlit
echo "7. Проверка процессов Streamlit..."
if $SSH_CMD "$SERVER" "[ -f '$REMOTE_DIR/streamlit_8504.pid' ]" 2>/dev/null; then
    PID=$($SSH_CMD "$SERVER" "cat $REMOTE_DIR/streamlit_8504.pid 2>/dev/null")
    if $SSH_CMD "$SERVER" "ps -p $PID >/dev/null 2>&1" 2>/dev/null; then
        echo "⚠️  Найден запущенный Streamlit (PID: $PID)"
        $SSH_CMD "$SERVER" "ps -p $PID -o pid,cmd --no-headers"
    else
        echo "ℹ️  Старый PID файл найден, но процесс не запущен"
    fi
else
    echo "✅ PID файл Streamlit не найден"
fi

echo ""

# Проверка дискового пространства
echo "8. Проверка дискового пространства..."
$SSH_CMD "$SERVER" "df -h $REMOTE_DIR 2>/dev/null || df -h / | tail -1"
echo ""

# Проверка Python
echo "9. Проверка Python..."
if $SSH_CMD "$SERVER" "command -v python3 >/dev/null 2>&1" 2>/dev/null; then
    echo "✅ Python3 установлен"
    $SSH_CMD "$SERVER" "python3 --version"
else
    echo "⚠️  Python3 не найден (понадобится для ручного развертывания)"
fi

echo ""

# Проверка Oracle подключения (если config.env существует)
echo "10. Проверка конфигурации..."
if $SSH_CMD "$SERVER" "[ -f '$REMOTE_DIR/config.env' ]" 2>/dev/null; then
    echo "✅ config.env найден"
    echo "Проверка Oracle настроек..."
    $SSH_CMD "$SERVER" "grep -E '^ORACLE_' $REMOTE_DIR/config.env | sed 's/PASSWORD=.*/PASSWORD=***/' || echo 'Oracle настройки не найдены'"
else
    echo "⚠️  config.env не найден (нужно будет создать)"
fi

echo ""

# Проверка существующих volumes
echo "11. Проверка Docker volumes..."
$SSH_CMD "$SERVER" "docker volume ls | grep -E 'qdrant|streamlit' || echo 'Volumes не найдены'"
echo ""

# Итоговый отчет
echo "========================================"
echo "Итоговый отчет"
echo "========================================"
echo ""
echo "✅ Готово к развертыванию:"
echo "  - Docker: установлен"
echo "  - Директория: $REMOTE_DIR"
echo ""
echo "⚠️  Требуется внимание:"
if $SSH_CMD "$SERVER" "[ -f '$REMOTE_DIR/streamlit_8504.pid' ] && ps -p \$(cat $REMOTE_DIR/streamlit_8504.pid) >/dev/null 2>&1" 2>/dev/null; then
    echo "  - Запущен Streamlit на порту 8504 (нужно остановить)"
fi
if $SSH_CMD "$SERVER" "docker ps --filter 'name=qdrant' --format '{{.Names}}' | grep -q ." 2>/dev/null; then
    echo "  - Найден контейнер Qdrant (проверьте конфликты портов)"
fi
echo ""
echo "📋 Следующие шаги:"
echo "  1. Синхронизация deploy директории"
echo "  2. Остановка старого Streamlit"
echo "  3. Развертывание новых контейнеров"
echo ""

