#!/bin/bash
# Синхронизация KB файлов и обновление базы знаний БЕЗ пересоздания коллекции
# Использование:
#   ./sync_and_update_kb.sh [SERVER] [SSH_CMD]     — полная синхронизация и перестройка всей KB
#   ./sync_and_update_kb.sh --only-examples [SERVER] [SSH_CMD]  — синхронизация + перезагрузка только Q/A примеров (быстро)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

ONLY_EXAMPLES=false
if [ "$1" = "--only-examples" ]; then
  ONLY_EXAMPLES=true
  shift
fi

# Параметры подключения
SERVER="${1:-root@82.114.2.2}"
SSH_CMD="${2:-ssh -p 1194}"
REMOTE_DIR="/usr/local/projects/ai_report"
LOCAL_DIR="$SCRIPT_DIR"

echo "========================================"
if [ "$ONLY_EXAMPLES" = true ]; then
  echo "Синхронизация + обновление только Q/A примеров (без перестройки таблиц/views/Confluence)"
else
  echo "Синхронизация KB и обновление базы знаний"
fi
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

# RAG модули (код). confluence_docs/ на сервере не трогаем — пополняется через Спутниковый библиотекарь
echo "  → RAG модули (код)..."
rsync -avz \
    -e "$SSH_CMD" \
    "$LOCAL_DIR/kb_billing/rag/" \
    "$SERVER:$REMOTE_DIR/kb_billing/rag/"

# База знаний (KB): код и таблицы/примеры. confluence_docs/ НЕ синхронизируем — пополняется на сервере (Спутниковый библиотекарь)
echo "  → База знаний (KB: tables, views, training_data)..."
rsync -avz \
    -e "$SSH_CMD" \
    "$LOCAL_DIR/kb_billing/tables/" \
    "$SERVER:$REMOTE_DIR/kb_billing/tables/"

rsync -avz \
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
if [ "$ONLY_EXAMPLES" = true ]; then
  echo "Перезагрузка только Q/A примеров на сервере..."
else
  echo "Обновление базы знаний на сервере..."
fi
echo "========================================"
echo "Проверка доступности Qdrant..."
$SSH_CMD "$SERVER" "curl -s http://localhost:6333/health > /dev/null 2>&1 || (echo '❌ Qdrant недоступен' && exit 1)"

if [ "$ONLY_EXAMPLES" = true ]; then
  echo "Запуск перезагрузки только примеров (sql_examples + user_added_examples)..."
  $SSH_CMD "$SERVER" "cd $REMOTE_DIR && python3 kb_billing/rag/init_kb.py --host localhost --port 6333 --collection kb_billing --only-examples"
else
  echo "Запуск обновления KB (все источники)..."
  $SSH_CMD "$SERVER" "cd $REMOTE_DIR && python3 kb_billing/rag/init_kb.py --host localhost --port 6333 --collection kb_billing --model intfloat/multilingual-e5-base"
fi

# Информация о перезапуске Streamlit (если синхронизировали Streamlit файлы)
if [ -f "$LOCAL_DIR/deploy/streamlit_report_oracle_backup.py" ]; then
    echo ""
    echo "⚠️  ВАЖНО: Синхронизирован Streamlit файл!"
    echo "   Для применения изменений в интерфейсе перезапустите Streamlit на сервере:"
    echo "   ssh -p 1194 $SERVER 'cd $REMOTE_DIR && ./restart_streamlit.sh'"
fi

echo ""
echo "========================================"
echo "✅ Синхронизация и обновление завершены!"
echo "========================================"
echo ""
echo "База знаний обновлена на сервере $SERVER"
if [ "$ONLY_EXAMPLES" = true ]; then
  echo "Обновлены только Q/A примеры (таблицы, представления, Confluence не перестраивались)."
fi
echo "Новые примеры запросов доступны в RAG-ассистенте"
if [ -f "$LOCAL_DIR/deploy/streamlit_report_oracle_backup.py" ]; then
    echo ""
    echo "⚠️  Для обновления кода приложения: SSH_CMD=\"ssh -p 1194\" ./sync_deploy.sh $SERVER"
    echo "   Затем перезапуск: ssh -p 1194 $SERVER 'cd $REMOTE_DIR && ./restart_streamlit.sh'"
fi
if [ "$ONLY_EXAMPLES" = false ]; then
    echo ""
    echo "💡 Если изменили только примеры (sql_examples.json), в следующий раз используйте:"
    echo "   ./sync_and_update_kb.sh --only-examples $SERVER"
    echo "   (перезагружаются только Q/A примеры, без перестройки таблиц/views — быстрее)"
fi





