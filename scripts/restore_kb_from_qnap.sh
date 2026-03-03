#!/bin/bash
# Восстановление базы знаний (KB) из архива QNAP (/qnap/kb)
#
# Использование:
#   ./restore_kb_from_qnap.sh [дата|latest]   # дата = YYYY-MM-DD, например 2025-03-04
#   RESTORE_FROM=2025-03-04 ./restore_kb_from_qnap.sh
#
# Варианты:
#   A) На сервере: восстановить из /qnap/kb/YYYY-MM-DD в локальный kb_billing
#   B) С другой машины: восстановить из локального /qnap/kb на сервер по SSH
#      REMOTE_SERVER=root@82.114.2.2 SSH_CMD="ssh -p 1194" ./restore_kb_from_qnap.sh 2025-03-04
#
# Перед перезаписью будет запрос подтверждения (если не указан --yes).

set -e

BACKUP_ROOT="${BACKUP_ROOT:-/qnap/kb}"
# Откуда восстанавливать: дата (YYYY-MM-DD) или latest
RESTORE_FROM="${RESTORE_FROM:-}"
# Куда восстанавливать при локальном запуске (на сервере)
KB_PROJECT_ROOT="${KB_PROJECT_ROOT:-/usr/local/projects/ai_report}"
# Восстановление на удалённый сервер
REMOTE_SERVER="${REMOTE_SERVER:-}"
SSH_CMD="${SSH_CMD:-ssh}"
REMOTE_KB_DIR="${REMOTE_KB_DIR:-/usr/local/projects/ai_report}"
# Без подтверждения (для cron/скриптов)
FORCE="${FORCE:-0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# Первый аргумент — дата или latest
if [ -n "$1" ]; then
    RESTORE_FROM="$1"
fi
if [ "$1" = "--yes" ] || [ "$1" = "-y" ]; then
    FORCE=1
    RESTORE_FROM="${2:-$RESTORE_FROM}"
fi
if [ -n "$2" ] && [ "$1" != "--yes" ] && [ "$1" != "-y" ]; then
    RESTORE_FROM="$2"
fi

echo "========================================"
echo "Восстановление KB из архива"
echo "========================================"
echo "Каталог архивов: $BACKUP_ROOT"
echo ""

if [ -z "$RESTORE_FROM" ]; then
    echo "Доступные бэкапы:"
    if [ ! -d "$BACKUP_ROOT" ]; then
        echo "❌ Каталог архивов не найден: $BACKUP_ROOT"
        exit 1
    fi
    ls -la "$BACKUP_ROOT/" 2>/dev/null | head -20
    echo ""
    echo "Укажите дату (YYYY-MM-DD) или latest:"
    echo "  ./restore_kb_from_qnap.sh 2025-03-04"
    echo "  ./restore_kb_from_qnap.sh latest"
    exit 0
fi

SOURCE="$BACKUP_ROOT/$RESTORE_FROM/kb_billing"
if [ ! -d "$SOURCE" ]; then
    echo "❌ Не найдена директория бэкапа: $SOURCE"
    echo "   Проверьте RESTORE_FROM и BACKUP_ROOT."
    exit 1
fi

echo "Источник: $SOURCE"
if [ -f "$BACKUP_ROOT/$RESTORE_FROM/backup_ts.txt" ]; then
    echo "Время бэкапа: $(cat "$BACKUP_ROOT/$RESTORE_FROM/backup_ts.txt")"
fi
echo ""

if [ -n "$REMOTE_SERVER" ]; then
    DEST_DESC="сервер $REMOTE_SERVER:$REMOTE_KB_DIR/kb_billing/"
    DEST="$REMOTE_SERVER:$REMOTE_KB_DIR/"
else
    DEST_DESC="локально: $KB_PROJECT_ROOT/kb_billing/"
    DEST="$KB_PROJECT_ROOT/"
fi

echo "Назначение: $DEST_DESC"
echo ""

if [ "$FORCE" != "1" ]; then
    echo "Внимание: содержимое kb_billing будет заменено данными из бэкапа."
    echo "Продолжить? [y/N]"
    read -r ans
    if [ "$ans" != "y" ] && [ "$ans" != "Y" ]; then
        echo "Отменено."
        exit 0
    fi
fi

echo "Восстановление..."
if [ -n "$REMOTE_SERVER" ]; then
    rsync -avz -e "$SSH_CMD" \
        --delete \
        "$SOURCE/" \
        "$DEST/kb_billing/"
else
    mkdir -p "$KB_PROJECT_ROOT/kb_billing"
    rsync -a \
        --delete \
        "$SOURCE/" \
        "$DEST/kb_billing/"
fi

echo "✅ KB восстановлена из $RESTORE_FROM в $DEST_DESC"
echo ""
echo "Дальше: перезагрузите KB в Qdrant (в интерфейсе «Спутниковый библиотекарь» — кнопка «Перезагрузить KB в Qdrant») или на сервере:"
echo "  cd $KB_PROJECT_ROOT && python3 kb_billing/rag/init_kb.py --host localhost --port 6333 --collection kb_billing --model intfloat/multilingual-e5-base"
if [ -n "$REMOTE_SERVER" ]; then
    echo "  или: ssh -p 1194 $REMOTE_SERVER 'cd $REMOTE_KB_DIR && python3 kb_billing/rag/init_kb.py ...'"
fi
