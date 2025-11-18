#!/bin/bash
# ============================================================================
# Установка Oracle Instant Client и SQL*Plus для Ubuntu/Debian
# Использование: ./install_sqlplus.sh
# ============================================================================

set -e

INSTALL_DIR="/opt/oracle"
VERSION="21.1.0.0.0"
ARCH="linux.x64-21.1.0.0.0"

echo "============================================================================"
echo "Установка Oracle Instant Client и SQL*Plus"
echo "============================================================================"
echo ""

# Проверка прав root
if [ "$EUID" -ne 0 ]; then 
    echo "⚠️  Требуются права root для установки"
    echo "   Запустите: sudo ./install_sqlplus.sh"
    exit 1
fi

# Установка зависимостей
echo "📦 Установка зависимостей..."
apt-get update

# Проверка и установка libaio (в Ubuntu 24.04 называется libaio1t64)
if apt-cache show libaio1t64 > /dev/null 2>&1; then
    LIB_AIO_PKG="libaio1t64"
elif apt-cache show libaio1 > /dev/null 2>&1; then
    LIB_AIO_PKG="libaio1"
elif apt-cache show libaio-dev > /dev/null 2>&1; then
    LIB_AIO_PKG="libaio-dev"
else
    LIB_AIO_PKG=""
fi

if [ -n "$LIB_AIO_PKG" ]; then
    apt-get install -y unzip wget $LIB_AIO_PKG
else
    echo "⚠️  libaio не найден, установка без него..."
    apt-get install -y unzip wget
fi

# Создание директории
echo "📁 Создание директории $INSTALL_DIR..."
mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR"

# Скачивание Oracle Instant Client
echo ""
echo "📥 Скачивание Oracle Instant Client..."
echo "   Примечание: Может потребоваться вручную скачать файлы"
echo "   Ссылки:"
echo "   - Basic: https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html"
echo "   - SQL*Plus: https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html"
echo ""

# Попытка скачать (может не сработать без регистрации)
BASIC_URL="https://download.oracle.com/otn_software/linux/instantclient/instantclient-basic-${ARCH}.zip"
SQLPLUS_URL="https://download.oracle.com/otn_software/linux/instantclient/instantclient-sqlplus-${ARCH}.zip"

echo "Попытка скачать с Oracle..."
if wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" \
    "$BASIC_URL" -O instantclient-basic.zip 2>/dev/null; then
    echo "✅ Basic скачан"
else
    echo "⚠️  Автоматическое скачивание не удалось"
    echo "   Пожалуйста, скачайте вручную:"
    echo "   1. Перейдите на https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html"
    echo "   2. Скачайте 'Basic Package' и 'SQL*Plus Package'"
    echo "   3. Сохраните в $INSTALL_DIR"
    echo "   4. Запустите скрипт снова"
    exit 1
fi

if wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" \
    "$SQLPLUS_URL" -O instantclient-sqlplus.zip 2>/dev/null; then
    echo "✅ SQL*Plus скачан"
else
    echo "⚠️  Автоматическое скачивание SQL*Plus не удалось"
    echo "   Пожалуйста, скачайте вручную"
    exit 1
fi

# Распаковка
echo ""
echo "📦 Распаковка..."
unzip -q instantclient-basic.zip
unzip -q instantclient-sqlplus.zip
rm -f instantclient-*.zip

# Создание символической ссылки
cd instantclient_21_1
if [ ! -f libclntsh.so ]; then
    ln -sf libclntsh.so.21.1 libclntsh.so
fi

# Настройка переменных окружения для всех пользователей
echo ""
echo "⚙️  Настройка переменных окружения..."
cat >> /etc/profile.d/oracle.sh << 'EOF'
export ORACLE_HOME=/opt/oracle/instantclient_21_1
export LD_LIBRARY_PATH=$ORACLE_HOME:$LD_LIBRARY_PATH
export PATH=$ORACLE_HOME:$PATH
EOF

chmod +x /etc/profile.d/oracle.sh

# Применение переменных для текущей сессии
export ORACLE_HOME=/opt/oracle/instantclient_21_1
export LD_LIBRARY_PATH=$ORACLE_HOME:$LD_LIBRARY_PATH
export PATH=$ORACLE_HOME:$PATH

echo ""
echo "============================================================================"
echo "✅ Установка завершена!"
echo "============================================================================"
echo ""
echo "Проверка установки:"
sqlplus -V
echo ""
echo "Для применения переменных окружения выполните:"
echo "  source /etc/profile.d/oracle.sh"
echo "  или перезайдите в систему"
echo ""
echo "Тест подключения через туннель:"
echo "  ./oracle_tunnel.sh start"
echo "  sqlplus billing7/billing@localhost:15210/bm7"

