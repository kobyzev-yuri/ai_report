# SSH туннель для доступа к Oracle серверу

## Описание

Скрипт `oracle_tunnel.sh` создает SSH туннель для доступа к Oracle серверу в локальной сети через сервер vz2. Это позволяет подключаться к Oracle с локальной машины без необходимости прямого доступа к сети, где находится Oracle сервер.

## Архитектура

```
Локальная машина          SSH сервер (vz2)          Oracle сервер
     (ваш ПК)            (82.114.2.2:1194)        (192.168.3.35:1521)
         |                        |                        |
         |  SSH туннель           |                        |
         |------------------------|                        |
         |                        |  Локальная сеть       |
         |                        |------------------------|
         |                        |                        |
    localhost:15210  <-------->  192.168.3.35:1521
```

## Использование

### Запуск туннеля

```bash
./oracle_tunnel.sh start
```

### Проверка статуса

```bash
./oracle_tunnel.sh status
```

### Остановка туннеля

```bash
./oracle_tunnel.sh stop
```

### Перезапуск туннеля

```bash
./oracle_tunnel.sh restart
```

## Настройки

По умолчанию используются следующие настройки (можно изменить в скрипте):

- **Локальный порт**: `15210` (можно изменить если занят)
- **SSH сервер**: `root@82.114.2.2:1194`
- **Oracle сервер**: `192.168.3.35:1521`

### Изменение локального порта

Если порт `15210` занят, измените переменную `LOCAL_PORT` в скрипте:

```bash
LOCAL_PORT="15211"  # или другой свободный порт
```

## Подключение к Oracle через туннель

### SQL*Plus

```bash
sqlplus billing7/billing@localhost:15210/bm7
```

### Python (cx_Oracle)

```python
import cx_Oracle

connection = cx_Oracle.connect(
    user="billing7",
    password="your_password",
    dsn="localhost:15210/bm7"
)
```

### Streamlit / Python скрипты

Измените `config.env`:

```bash
ORACLE_HOST=localhost
ORACLE_PORT=15210
ORACLE_SERVICE=bm7
```

Или используйте переменные окружения:

```bash
export ORACLE_HOST=localhost
export ORACLE_PORT=15210
export ORACLE_SERVICE=bm7
```

## Автозапуск при старте системы

### Linux (systemd)

Создайте файл `/etc/systemd/system/oracle-tunnel.service`:

```ini
[Unit]
Description=Oracle SSH Tunnel
After=network.target

[Service]
Type=simple
User=your_username
ExecStart=/path/to/ai_report/oracle_tunnel.sh start
ExecStop=/path/to/ai_report/oracle_tunnel.sh stop
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Затем:

```bash
sudo systemctl daemon-reload
sudo systemctl enable oracle-tunnel.service
sudo systemctl start oracle-tunnel.service
```

### macOS (launchd)

Создайте файл `~/Library/LaunchAgents/com.oracle.tunnel.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.oracle.tunnel</string>
    <key>ProgramArguments</key>
    <array>
        <string>/path/to/ai_report/oracle_tunnel.sh</string>
        <string>start</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
</dict>
</plist>
```

Затем:

```bash
launchctl load ~/Library/LaunchAgents/com.oracle.tunnel.plist
```

## Устранение неполадок

### Туннель не запускается

1. Проверьте доступность SSH сервера:
   ```bash
   ssh -p 1194 root@82.114.2.2
   ```

2. Проверьте логи:
   ```bash
   cat /tmp/oracle_tunnel.log
   ```

3. Проверьте, не занят ли локальный порт:
   ```bash
   lsof -i :15210
   ```

### Туннель обрывается

Скрипт настроен на автоматическое поддержание соединения:
- `ServerAliveInterval=60` - проверка каждые 60 секунд
- `ServerAliveCountMax=3` - максимум 3 неудачных проверки

Если туннель все равно обрывается, проверьте:
- Стабильность интернет-соединения
- Настройки файрвола на SSH сервере
- Таймауты на SSH сервере

### Порт уже занят

Измените `LOCAL_PORT` в скрипте на другой свободный порт (например, 15211, 15212 и т.д.)

### Не удается подключиться к Oracle через туннель

1. Убедитесь, что туннель запущен:
   ```bash
   ./oracle_tunnel.sh status
   ```

2. Проверьте доступность локального порта:
   ```bash
   telnet localhost 15210
   ```

3. Проверьте, что Oracle сервер доступен с SSH сервера:
   ```bash
   ssh -p 1194 root@82.114.2.2 "telnet 192.168.3.35 1521"
   ```

## Безопасность

- SSH туннель использует шифрование SSH для защиты данных
- Пароли Oracle не передаются в открытом виде
- Рекомендуется использовать SSH ключи вместо паролей для SSH подключения

## Производительность

SSH туннель добавляет небольшую задержку (обычно < 50ms) из-за шифрования/дешифрования. Для большинства задач это не критично.

## Альтернативы

Если нужен постоянный доступ, рассмотрите:
- VPN подключение к сети с Oracle сервером
- Прямое подключение к Oracle серверу (если возможно)
- Использование Oracle Cloud Infrastructure (OCI) для размещения Oracle

