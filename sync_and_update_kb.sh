#!/bin/bash
# Синхронизация KB файлов и обновление базы знаний БЕЗ пересоздания коллекции

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Параметры подключения
SERVER="${1:-root@82.114.2.2}"
SSH_CMD="${2:-ssh -p 1194}"
REMOTE_DIR="/usr/local/projects/ai_report"
LOCAL_DIR="$SCRIPT_DIR"

echo "========================================"
echo "Синхронизация KB и обновление базы знаний"
echo "========================================"
echo "Сервер: $SERVER"
echo "SSH команда: $SSH_CMD"
echo "Удаленная директория: $REMOTE_DIR"
echo "Локальная директория: $LOCAL_DIR"
echo ""

# Проверка подключения к серверу
echo "Проверка подключения к серверу..."
if ! $SSH_CMD "$SERVER" "echo 'OK'" > /dev/null 2>&1; then
    echo "❌ Не удалось подключиться к серверу $SERVER"
    exit 1
fi
echo "✅ Подключение успешно"

# Проверка наличия rsync
if ! command -v rsync &> /dev/null; then
    echo "❌ rsync не найден. Установите rsync для синхронизации файлов"
    exit 1
fi
echo "✅ rsync доступен"

# Создание структуры директорий на сервере
echo ""
echo "Создание структуры директорий на сервере..."
$SSH_CMD "$SERVER" "mkdir -p $REMOTE_DIR/kb_billing/{rag,tables,views,training_data}" || true
echo "✅ Директории созданы"

# Синхронизация файлов
echo ""
echo "========================================"
echo "Синхронизация файлов..."
echo "========================================"

# Streamlit приложение (если есть в deploy/)
if [ -f "$LOCAL_DIR/deploy/streamlit_report_oracle_backup.py" ]; then
    echo "  → Streamlit приложение..."
    rsync -avz \
        -e "$SSH_CMD" \
        "$LOCAL_DIR/deploy/streamlit_report_oracle_backup.py" \
        "$SERVER:$REMOTE_DIR/streamlit_report_oracle_backup.py" 2>/dev/null || true
fi

# RAG модули
echo "  → RAG модули..."
rsync -avz --delete \
    -e "$SSH_CMD" \
    "$LOCAL_DIR/kb_billing/rag/" \
    "$SERVER:$REMOTE_DIR/kb_billing/rag/"

# База знаний (KB)
echo "  → База знаний (KB)..."
rsync -avz --delete \
    -e "$SSH_CMD" \
    "$LOCAL_DIR/kb_billing/tables/" \
    "$SERVER:$REMOTE_DIR/kb_billing/tables/"

rsync -avz --delete \
    -e "$SSH_CMD" \
    "$LOCAL_DIR/kb_billing/views/" \
    "$SERVER:$REMOTE_DIR/kb_billing/views/"

rsync -avz \
    -e "$SSH_CMD" \
    "$LOCAL_DIR/kb_billing/metadata.json" \
    "$SERVER:$REMOTE_DIR/kb_billing/metadata.json" 2>/dev/null || true

rsync -avz \
    -e "$SSH_CMD" \
    "$LOCAL_DIR/kb_billing/SUMMARY.md" \
    "$SERVER:$REMOTE_DIR/kb_billing/SUMMARY.md" 2>/dev/null || true

# Примеры SQL запросов (training_data)
echo "  → Примеры SQL запросов (training_data)..."
rsync -avz \
    -e "$SSH_CMD" \
    "$LOCAL_DIR/kb_billing/training_data/" \
    "$SERVER:$REMOTE_DIR/kb_billing/training_data/"

echo ""
echo "✅ Синхронизация файлов завершена"

# Обновление KB на сервере (без пересоздания коллекции)
echo ""
echo "========================================"
echo "Обновление базы знаний на сервере..."
echo "========================================"
echo "Проверка доступности Qdrant..."
$SSH_CMD "$SERVER" "curl -s http://localhost:6333/health > /dev/null 2>&1 || (echo '❌ Qdrant недоступен' && exit 1)"

echo "Запуск обновления KB..."
$SSH_CMD "$SERVER" "cd $REMOTE_DIR && python3 kb_billing/rag/init_kb.py --host localhost --port 6333 --collection kb_billing --model intfloat/multilingual-e5-base"

# Информация о перезапуске Streamlit (если синхронизировали Streamlit файлы)
if [ -f "$LOCAL_DIR/deploy/streamlit_report_oracle_backup.py" ]; then
    echo ""
    echo "⚠️  ВАЖНО: Синхронизирован Streamlit файл!"
    echo "   Для применения изменений в интерфейсе перезапустите Streamlit:"
    echo "   $SSH_CMD $SERVER 'cd $REMOTE_DIR && ./restart_streamlit.sh'"
fi

echo ""
echo "========================================"
echo "✅ Синхронизация и обновление завершены!"
echo "========================================"
echo ""
echo "База знаний обновлена на сервере $SERVER"
echo "Новые примеры запросов доступны в RAG-ассистенте"
if [ -f "$LOCAL_DIR/deploy/streamlit_report_oracle_backup.py" ]; then
    echo ""
    echo "⚠️  ВАЖНО: Если синхронизировали Streamlit файлы, перезапустите Streamlit:"
    echo "  ssh -p 1194 $SERVER 'cd $REMOTE_DIR && ./restart_streamlit.sh'"
fi





