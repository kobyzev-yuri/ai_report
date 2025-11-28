#!/bin/bash
# ============================================================================
# Упрощенная установка Oracle Instant Client и SQL*Plus
# Использование: ./install_sqlplus_simple.sh
# ============================================================================

set -e

INSTALL_DIR="/opt/oracle"
VERSION="23.26.0.0.0"

echo "============================================================================"
echo "Установка Oracle Instant Client и SQL*Plus (упрощенная версия)"
echo "============================================================================"
echo ""

# Проверка прав root
if [ "$EUID" -ne 0 ]; then 
    echo "⚠️  Требуются права root для установки"
    echo "   Запустите: sudo ./install_sqlplus_simple.sh"
    exit 1
fi

# Установка базовых зависимостей
echo "📦 Установка зависимостей..."
apt-get update
apt-get install -y unzip wget

# Проверка наличия libaio (может быть уже установлен или не нужен)
if ! dpkg -l | grep -q "^ii.*libaio"; then
    echo "⚠️  libaio не найден, но продолжим установку"
    echo "   Если возникнут проблемы с библиотеками, установите вручную:"
    echo "   apt-get install -y libaio1"
fi

# Создание директории
echo "📁 Создание директории $INSTALL_DIR..."
mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR"

echo ""
echo "============================================================================"
echo "Инструкция по ручной установке Oracle Instant Client"
echo "============================================================================"
echo ""
echo "1. Перейдите на страницу загрузки Oracle:"
echo "   https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html"
echo ""
echo "2. Скачайте следующие файлы (требуется регистрация на Oracle.com):"
echo "   - Basic Package (instantclient-basic-linux.x64-23.26.0.0.0.zip)"
echo "   - SQL*Plus Package (instantclient-sqlplus-linux.x64-23.26.0.0.0.zip)"
echo ""
echo "3. Сохраните файлы в: $INSTALL_DIR"
echo ""
echo "4. Выполните следующие команды:"
echo ""
echo "   cd $INSTALL_DIR"
echo "   sudo unzip instantclient-basic-linux.x64-23.26.0.0.0.zip"
echo "   sudo unzip instantclient-sqlplus-linux.x64-23.26.0.0.0.zip"
echo "   cd instantclient_21_1"
echo "   sudo ln -sf libclntsh.so.21.1 libclntsh.so"
echo ""
echo "5. Настройте переменные окружения:"
echo ""
echo "   echo 'export ORACLE_HOME=$INSTALL_DIR/instantclient_23_26' >> /etc/profile.d/oracle.sh"
echo "   echo 'export LD_LIBRARY_PATH=\$ORACLE_HOME:\$LD_LIBRARY_PATH' >> /etc/profile.d/oracle.sh"
echo "   echo 'export PATH=\$ORACLE_HOME:\$PATH' >> /etc/profile.d/oracle.sh"
echo "   chmod +x /etc/profile.d/oracle.sh"
echo "   source /etc/profile.d/oracle.sh"
echo ""
echo "6. Проверьте установку:"
echo "   sqlplus -V"
echo ""
echo "============================================================================"
echo "Альтернатива: Использование Python вместо SQL*Plus"
echo "============================================================================"
echo ""
echo "Если установка SQL*Plus проблематична, используйте Python скрипт:"
echo ""
echo "   # Установить oracledb (если еще не установлен)"
echo "   pip install oracledb"
echo ""
echo "   # Запустить тест через туннель"
echo "   ./oracle_tunnel.sh start"
echo "   python3 test_tunnel_python.py"
echo ""
echo "============================================================================"

