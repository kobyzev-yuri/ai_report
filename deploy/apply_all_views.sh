#!/bin/bash
# Скрипт для применения всех Oracle VIEW на сервере
# Использование: 
#   Локально: SSH_CMD="ssh -p 1194" ./deploy/apply_all_views.sh root@82.114.2.2
#   На сервере: ./apply_all_views.sh

set -e

if [ -z "$1" ]; then
    # Если запущено на сервере без параметров
    if [ -f "/usr/local/projects/ai_report/config.env" ]; then
        REMOTE_DIR="/usr/local/projects/ai_report"
        source "$REMOTE_DIR/config.env"
    else
        echo "❌ Не найден config.env. Используйте: $0 [user@]host [ssh_command]"
        exit 1
    fi
else
    # Если запущено локально с параметрами
    SERVER="$1"
    SSH_CMD="${SSH_CMD:-${2:-ssh}}"
    REMOTE_DIR="/usr/local/projects/ai_report"
    
    echo "========================================"
    echo "Применение всех Oracle VIEW на сервере"
    echo "========================================"
    echo "Сервер: $SERVER"
    echo "SSH команда: $SSH_CMD"
    echo "Удаленная директория: $REMOTE_DIR"
    echo ""
    
    # Проверка подключения
    if ! $SSH_CMD "$SERVER" "echo 'OK'" >/dev/null 2>&1; then
        echo "❌ Не удалось подключиться к серверу"
        exit 1
    fi
    echo "✅ Подключение успешно"
    echo ""
    
    # Выполнение на сервере
    $SSH_CMD "$SERVER" "bash -s" < "$0"
    exit $?
fi

# Код, выполняемый на сервере
echo "========================================"
echo "Применение всех Oracle VIEW"
echo "========================================"
echo ""

# Проверка наличия config.env
if [ ! -f "$REMOTE_DIR/config.env" ]; then
    echo "❌ Не найден config.env в $REMOTE_DIR"
    exit 1
fi

source "$REMOTE_DIR/config.env"

# Проверка переменных окружения
if [ -z "$ORACLE_USER" ] || [ -z "$ORACLE_PASSWORD" ] || [ -z "$ORACLE_HOST" ]; then
    echo "❌ Не заданы переменные Oracle в config.env"
    echo "   Требуются: ORACLE_USER, ORACLE_PASSWORD, ORACLE_HOST"
    exit 1
fi

ORACLE_DSN="${ORACLE_HOST}:${ORACLE_PORT:-1521}/${ORACLE_SERVICE:-bm7}"
VIEWS_DIR="$REMOTE_DIR/oracle/views"

if [ ! -d "$VIEWS_DIR" ]; then
    echo "❌ Директория VIEW не найдена: $VIEWS_DIR"
    exit 1
fi

cd "$VIEWS_DIR"

echo "Применение всех представлений из install_all_views.sql..."
echo "Подключение: ${ORACLE_USER}@${ORACLE_DSN}"
echo ""

# Применение всех VIEW с таймаутом
timeout 300 sqlplus -S "${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_DSN}" @install_all_views.sql 2>&1 | tee /tmp/apply_views.log

EXIT_CODE=${PIPESTATUS[0]}

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Все представления применены успешно!"
    echo ""
    echo "Проверка последних строк лога:"
    tail -20 /tmp/apply_views.log
else
    echo "❌ Ошибка при применении представлений (код: $EXIT_CODE)"
    echo ""
    echo "Последние строки лога с ошибками:"
    tail -30 /tmp/apply_views.log | grep -i "error\|ora-\|недопустимый" || tail -30 /tmp/apply_views.log
    exit $EXIT_CODE
fi


