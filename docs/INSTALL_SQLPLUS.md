# Установка SQL*Plus (Oracle Instant Client)

## Для Linux (Ubuntu/Debian)

### Вариант 1: Oracle Instant Client (рекомендуется)

```bash
# 1. Скачать Oracle Instant Client Basic и SQL*Plus
cd /tmp
wget https://download.oracle.com/otn_software/linux/instantclient/instantclient-basic-linux.x64-21.1.0.0.0.zip
wget https://download.oracle.com/otn_software/linux/instantclient/instantclient-sqlplus-linux.x64-21.1.0.0.0.zip

# 2. Установить unzip если нет
sudo apt-get update
sudo apt-get install -y unzip libaio1

# 3. Создать директорию и распаковать
sudo mkdir -p /opt/oracle
cd /opt/oracle
sudo unzip /tmp/instantclient-basic-linux.x64-21.1.0.0.0.zip
sudo unzip /tmp/instantclient-sqlplus-linux.x64-21.1.0.0.0.zip

# 4. Создать символическую ссылку (если нужно)
cd /opt/oracle/instantclient_21_1
sudo ln -sf libclntsh.so.21.1 libclntsh.so

# 5. Настроить переменные окружения
echo 'export ORACLE_HOME=/opt/oracle/instantclient_21_1' >> ~/.bashrc
echo 'export LD_LIBRARY_PATH=$ORACLE_HOME:$LD_LIBRARY_PATH' >> ~/.bashrc
echo 'export PATH=$ORACLE_HOME:$PATH' >> ~/.bashrc
source ~/.bashrc

# 6. Проверить установку
sqlplus -V
```

### Вариант 2: Использование Docker (если не хотите устанавливать локально)

```bash
# Запустить Oracle Instant Client в Docker контейнере
docker run -it --rm \
  -v $(pwd):/workspace \
  -w /workspace \
  oraclelinux:8 \
  bash -c "yum install -y oracle-instantclient19.19-basic oracle-instantclient19.19-sqlplus && sqlplus -V"
```

### Вариант 3: Использование Python cx_Oracle вместо sqlplus

Если установка sqlplus проблематична, можно использовать Python скрипт:

```bash
# Установить cx_Oracle
pip install cx_Oracle

# Использовать Python скрипт для выполнения SQL
python3 -c "
import cx_Oracle
conn = cx_Oracle.connect('billing7/billing@localhost:15210/bm7')
cursor = conn.cursor()
cursor.execute('SELECT FINANCIAL_PERIOD, BILL_MONTH, IMEI, FEE_ADVANCE_CHARGE FROM V_CONSOLIDATED_REPORT_WITH_BILLING WHERE IMEI = :imei AND FINANCIAL_PERIOD = :period', {'imei': '301434061220930', 'period': '2025-10'})
for row in cursor:
    print(row)
conn.close()
"
```

## Для macOS

```bash
# Использовать Homebrew
brew install instantclient-basic instantclient-sqlplus

# Или скачать вручную
cd /tmp
curl -O https://download.oracle.com/otn_software/mac/instantclient/instantclient-basic-macos.x64-21.1.0.0.0.zip
curl -O https://download.oracle.com/otn_software/mac/instantclient/instantclient-sqlplus-macos.x64-21.1.0.0.0.zip

# Распаковать
cd /opt/oracle
unzip /tmp/instantclient-basic-macos.x64-21.1.0.0.0.zip
unzip /tmp/instantclient-sqlplus-macos.x64-21.1.0.0.0.zip

# Настроить переменные окружения
echo 'export ORACLE_HOME=/opt/oracle/instantclient_21_1' >> ~/.zshrc
echo 'export DYLD_LIBRARY_PATH=$ORACLE_HOME:$DYLD_LIBRARY_PATH' >> ~/.zshrc
echo 'export PATH=$ORACLE_HOME:$PATH' >> ~/.zshrc
source ~/.zshrc
```

## Быстрая установка через скрипт

Создайте скрипт `install_sqlplus.sh`:

```bash
#!/bin/bash
# Автоматическая установка Oracle Instant Client и SQL*Plus

set -e

INSTALL_DIR="/opt/oracle"
VERSION="21.1.0.0.0"
ARCH="linux.x64"

echo "Установка Oracle Instant Client..."

# Проверка ОС
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    ARCH="linux.x64"
    PKG_MGR="apt-get"
    INSTALL_CMD="sudo $PKG_MGR install -y"
elif [[ "$OSTYPE" == "darwin"* ]]; then
    ARCH="macos.x64"
    PKG_MGR="brew"
    INSTALL_CMD="$PKG_MGR install"
else
    echo "Неподдерживаемая ОС: $OSTYPE"
    exit 1
fi

# Установка зависимостей
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    $INSTALL_CMD unzip libaio1
fi

# Создание директории
sudo mkdir -p $INSTALL_DIR
cd $INSTALL_DIR

# Скачивание (требуется регистрация на oracle.com или использование прямых ссылок)
echo "Скачивание Oracle Instant Client..."
echo "Примечание: Может потребоваться вручную скачать файлы с oracle.com"
echo "Ссылки:"
echo "  Basic: https://www.oracle.com/database/technologies/instant-client/downloads.html"
echo "  SQL*Plus: https://www.oracle.com/database/technologies/instant-client/downloads.html"

# Альтернатива: использовать wget с cookie (требует регистрации)
# wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" \
#   "https://download.oracle.com/otn_software/linux/instantclient/instantclient-basic-${ARCH}-${VERSION}.zip"

echo ""
echo "После скачивания выполните:"
echo "  cd $INSTALL_DIR"
echo "  sudo unzip instantclient-basic-${ARCH}-${VERSION}.zip"
echo "  sudo unzip instantclient-sqlplus-${ARCH}-${VERSION}.zip"
echo ""
echo "Затем настройте переменные окружения:"
echo "  export ORACLE_HOME=$INSTALL_DIR/instantclient_21_1"
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo "  export LD_LIBRARY_PATH=\$ORACLE_HOME:\$LD_LIBRARY_PATH"
else
    echo "  export DYLD_LIBRARY_PATH=\$ORACLE_HOME:\$DYLD_LIBRARY_PATH"
fi
echo "  export PATH=\$ORACLE_HOME:\$PATH"
```

## Проверка установки

После установки проверьте:

```bash
# Проверка версии sqlplus
sqlplus -V

# Тест подключения через туннель
sqlplus billing7/billing@localhost:15210/bm7 <<EOF
SELECT 'Подключение успешно!' FROM DUAL;
EXIT;
EOF
```

## Альтернатива: Использование Python скрипта для тестирования

Если установка sqlplus проблематична, используйте Python:

```bash
# Установить cx_Oracle
pip install cx_Oracle

# Запустить тестовый скрипт
python3 test_tunnel_python.py
```

Создайте файл `test_tunnel_python.py`:

```python
#!/usr/bin/env python3
import cx_Oracle
import sys

try:
    # Подключение через туннель
    dsn = cx_Oracle.makedsn("localhost", 15210, service_name="bm7")
    conn = cx_Oracle.connect(user="billing7", password="billing", dsn=dsn)
    
    print("✅ Подключение к Oracle через SSH туннель успешно!")
    print()
    
    # Тестовый запрос
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            v.FINANCIAL_PERIOD,
            v.BILL_MONTH AS INVOICE_MONTH,
            v.IMEI,
            v.CONTRACT_ID,
            v.FEE_ADVANCE_CHARGE,
            v.FEE_PRORATED
        FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
        WHERE v.IMEI = :imei
          AND v.FINANCIAL_PERIOD = :period
        ORDER BY v.BILL_MONTH DESC
    """, {'imei': '301434061220930', 'period': '2025-10'})
    
    print("Результаты запроса:")
    print("-" * 80)
    for row in cursor:
        print(f"FINANCIAL_PERIOD: {row[0]}, BILL_MONTH: {row[1]}, IMEI: {row[2]}, "
              f"CONTRACT_ID: {row[3]}, ADVANCE_CHARGE: {row[4]}, PRORATED: {row[5]}")
    
    conn.close()
    print()
    print("✅ Тест завершен успешно!")
    
except cx_Oracle.Error as e:
    print(f"❌ Ошибка подключения к Oracle: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Ошибка: {e}")
    sys.exit(1)
```

