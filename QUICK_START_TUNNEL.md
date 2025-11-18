# Быстрый старт: SSH туннель для Oracle

## Запуск и тестирование

```bash
# 1. Запустить туннель
./oracle_tunnel.sh start

# 2. Выполнить тест
python3 test_tunnel_python.py

# 3. Остановить туннель (когда не нужен)
./oracle_tunnel.sh stop
```

## Проверка статуса

```bash
./oracle_tunnel.sh status
```

## Использование в Python скриптах

```python
import oracledb
import os

# Подключение через туннель
dsn = f"{os.getenv('ORACLE_HOST', 'localhost')}:{os.getenv('ORACLE_PORT', '15210')}/{os.getenv('ORACLE_SERVICE', 'bm7')}"
conn = oracledb.connect(
    user=os.getenv('ORACLE_USER', 'billing7'),
    password=os.getenv('ORACLE_PASSWORD', 'billing'),
    dsn=dsn
)

# Выполнение запросов
cursor = conn.cursor()
cursor.execute("SELECT * FROM V_CONSOLIDATED_REPORT_WITH_BILLING WHERE IMEI = :imei", {'imei': '301434061220930'})
for row in cursor:
    print(row)
conn.close()
```

## Настройки по умолчанию

- **Локальный порт**: `15210`
- **SSH сервер**: `root@82.114.2.2:1194`
- **Oracle сервер**: `192.168.3.35:1521`
- **Сервис**: `bm7`
- **Пользователь**: `billing7`

## Полезные команды

```bash
# Просмотр логов туннеля
cat /tmp/oracle_tunnel.log

# Проверка занятости порта
lsof -i :15210

# Перезапуск туннеля
./oracle_tunnel.sh restart
```

## Примечания

- Туннель работает в фоновом режиме
- Автоматически поддерживает соединение (keep-alive)
- Логи сохраняются в `/tmp/oracle_tunnel.log`
- PID процесса сохраняется в `/tmp/oracle_tunnel.pid`

