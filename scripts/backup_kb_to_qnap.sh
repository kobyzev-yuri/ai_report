#!/bin/bash
# Регулярный бэкап базы знаний (KB) в архив на QNAP — /qnap/kb
#
# Варианты запуска:
#  A) На сервере (где лежит KB): QNAP смонтирован на сервере в /qnap/kb
#     ./backup_kb_to_qnap.sh
#  B) С другой машины (где смонтирован /qnap/kb): бэкап с сервера по SSH
#     REMOTE_SERVER=root@82.114.2.2 SSH_CMD="ssh -p 1194" ./backup_kb_to_qnap.sh
#
# Регулярный запуск: cron (см. cursor.md, раздел «Бэкап KB»).

set -e

# Сервер с KB (если пусто — бэкап локальный, скрипт должен быть на сервере)
REMOTE_SERVER="${REMOTE_SERVER:-}"
SSH_CMD="${SSH_CMD:-ssh}"
REMOTE_KB_DIR="${REMOTE_KB_DIR:-/usr/local/projects/ai_report}"

# Корень проекта с KB при локальном запуске (на сервере)
KB_PROJECT_ROOT="${KB_PROJECT_ROOT:-/usr/local/projects/ai_report}"
# Куда класть бэкапы (архив на QNAP)
BACKUP_ROOT="${BACKUP_ROOT:-/qnap/kb}"
# Поддиректория с датой (USE_DATE=0 → всё в BACKUP_ROOT/latest)
USE_DATE="${USE_DATE:-1}"
# Сколько суточных копий хранить (0 = не удалять старые)
KEEP_DAYS="${KEEP_DAYS:-30}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "========================================"
echo "Бэкап KB в $BACKUP_ROOT"
echo "========================================"

if [ -n "$REMOTE_SERVER" ]; then
    echo "Режим: с сервера $REMOTE_SERVER"
    SRC="$REMOTE_SERVER:$REMOTE_KB_DIR/kb_billing/"
    if ! $SSH_CMD "$REMOTE_SERVER" "test -d $REMOTE_KB_DIR/kb_billing" 2>/dev/null; then
        echo "❌ На сервере не найдена директория KB: $REMOTE_KB_DIR/kb_billing"
        exit 1
    fi
else
    echo "Режим: локальный (источник $KB_PROJECT_ROOT)"
    if [ ! -d "$KB_PROJECT_ROOT/kb_billing" ]; then
        echo "❌ Директория KB не найдена: $KB_PROJECT_ROOT/kb_billing"
        exit 1
    fi
    SRC="$KB_PROJECT_ROOT/kb_billing/"
fi

if [ ! -d "$BACKUP_ROOT" ] && ! mkdir -p "$BACKUP_ROOT"; then
    echo "❌ Не удалось использовать каталог бэкапов: $BACKUP_ROOT"
    echo "   Проверьте, что QNAP смонтирован и путь доступен."
    exit 1
fi

if [ "$USE_DATE" = "1" ]; then
    DATE_SUBDIR="$(date +%Y-%m-%d)"
    DEST="$BACKUP_ROOT/$DATE_SUBDIR"
else
    DEST="$BACKUP_ROOT/latest"
fi

mkdir -p "$DEST"

echo "Назначение: $DEST"
echo "Копирование kb_billing (confluence_docs, tables, views, training_data, metadata, rag)..."
if [ -n "$REMOTE_SERVER" ]; then
    rsync -avz -e "$SSH_CMD" \
        --exclude='__pycache__' \
        --exclude='*.pyc' \
        "$SRC" \
        "$DEST/kb_billing/"
else
    rsync -a \
        --exclude='__pycache__' \
        --exclude='*.pyc' \
        "$SRC" \
        "$DEST/kb_billing/"
fi

echo "$(date -Iseconds)" > "$DEST/backup_ts.txt"
echo "✅ Бэкап записан: $DEST"

if [ "$KEEP_DAYS" -gt 0 ] && [ "$USE_DATE" = "1" ]; then
    echo "Удаление бэкапов старше $KEEP_DAYS дней..."
    find "$BACKUP_ROOT" -maxdepth 1 -type d -name '20*' -mtime +$KEEP_DAYS -exec rm -rf {} + 2>/dev/null || true
fi

echo ""
echo "Готово. Каталоги в $BACKUP_ROOT:"
ls -la "$BACKUP_ROOT/" 2>/dev/null || true
