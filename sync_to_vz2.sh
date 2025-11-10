#!/bin/bash
# ============================================================================
# Быстрая синхронизация на сервер vz2 (82.114.2.2)
# Использование: ./sync_to_vz2.sh
# ============================================================================

# Используем алиас или прямое подключение
if command -v vz2 &> /dev/null; then
    SSH_CMD="vz2"
    REMOTE_USER="root"
    REMOTE_HOST="82.114.2.2"
else
    SSH_CMD="ssh -p 1194 root@82.114.2.2"
    REMOTE_USER="root"
    REMOTE_HOST="82.114.2.2"
fi

REMOTE_PATH="/usr/local/projects/ai_report"
LOCAL_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "============================================================================"
echo "Синхронизация проекта на сервер vz2 (Oracle)"
echo "============================================================================"
echo "Локальный путь:  $LOCAL_PATH"
echo "Удаленный сервер: $REMOTE_USER@$REMOTE_HOST"
echo "Удаленный путь:  $REMOTE_PATH"
echo ""

# Создаем директорию на удаленном сервере если не существует
echo "📁 Проверка удаленной директории..."
$SSH_CMD "mkdir -p $REMOTE_PATH"

# Синхронизация с rsync
echo ""
echo "🔄 Синхронизация файлов..."
rsync -avz --progress \
    -e "ssh -p 1194" \
    --exclude='.git/' \
    --exclude='.gitignore' \
    --exclude='__pycache__/' \
    --exclude='*.pyc' \
    --exclude='*.pyo' \
    --exclude='*.log' \
    --exclude='*.pid' \
    --exclude='data/' \
    --exclude='archive/' \
    --exclude='config.env' \
    --exclude='*.env' \
    --exclude='.env.*' \
    --exclude='*.swp' \
    --exclude='*.swo' \
    --exclude='*~' \
    --exclude='.DS_Store' \
    --exclude='Thumbs.db' \
    --exclude='.vscode/' \
    --exclude='.idea/' \
    --exclude='venv/' \
    --exclude='env/' \
    --exclude='ENV/' \
    --exclude='.venv' \
    --exclude='*.tmp' \
    --exclude='*.bak' \
    --exclude='*.backup' \
    --exclude='*.old' \
    --exclude='postgresql/' \
    --exclude='*.sql.gz' \
    --exclude='*.dump' \
    "$LOCAL_PATH/" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/"

echo ""
echo "✅ Синхронизация завершена!"
echo ""
echo "📋 Следующие шаги на удаленном сервере (vz2):"
echo "  1. Подключитесь: vz2"
echo "  2. Перейдите в директорию: cd $REMOTE_PATH"
echo "  3. Создайте config.env с настройками Oracle:"
echo "     cp config.env.example config.env"
echo "     nano config.env  # отредактируйте с вашими данными"
echo ""
echo "  4. Установите зависимости:"
echo "     pip install -r requirements.txt"
echo ""
echo "  5. Создайте директорию для данных:"
echo "     mkdir -p data/SPNet\\ reports"
echo "     mkdir -p data/STECCOMLLCRussiaSBD.AccessFees_reports"
echo ""
echo "  6. Проверьте базу данных пользователей (users.db):"
echo "     ls -lh users.db  # должна быть синхронизирована"
echo ""
echo "  7. Запустите Streamlit:"
echo "     streamlit run streamlit_report_oracle_backup.py --server.port 8501"

