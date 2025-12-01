# Примеры использования скриптов развертывания

## Использование с кастомным SSH портом

Если SSH работает на нестандартном порту (например, 1194):

### Вариант 1: Через переменную окружения

```bash
SSH_CMD="ssh -p 1194" ./server_inspection.sh root@82.114.2.2
SSH_CMD="ssh -p 1194" ./sync_deploy.sh root@82.114.2.2
SSH_CMD="ssh -p 1194" ./safe_deploy.sh root@82.114.2.2
```

### Вариант 2: Через алиас

Если у вас есть алиас `vz2='ssh -p 1194 root@82.114.2.2'`:

```bash
SSH_CMD="vz2" ./server_inspection.sh root@82.114.2.2
SSH_CMD="vz2" ./sync_deploy.sh root@82.114.2.2
SSH_CMD="vz2" ./safe_deploy.sh root@82.114.2.2
```

### Вариант 3: Экспорт переменной

```bash
export SSH_CMD="ssh -p 1194"
./server_inspection.sh root@82.114.2.2
./sync_deploy.sh root@82.114.2.2
./safe_deploy.sh root@82.114.2.2
```

---

## Полный пример развертывания

```bash
cd deploy

# 1. Проверка готовности
./check_prerequisites.sh

# 2. Обследование (с кастомным портом)
SSH_CMD="ssh -p 1194" ./server_inspection.sh root@82.114.2.2

# 3. Развертывание (автоматически использует SSH_CMD)
SSH_CMD="ssh -p 1194" ./safe_deploy.sh root@82.114.2.2
```

---

## Пошаговое развертывание

```bash
# Экспорт SSH команды один раз
export SSH_CMD="ssh -p 1194"

# Шаг 1: Обследование
./server_inspection.sh root@82.114.2.2

# Шаг 2: Синхронизация
./sync_deploy.sh root@82.114.2.2

# Шаг 3: Развертывание на сервере
$SSH_CMD root@82.114.2.2 "cd /usr/local/projects/ai_report && ./deploy.sh docker"
```

---

## Проверка после развертывания

```bash
export SSH_CMD="ssh -p 1194"

# Статус
$SSH_CMD root@82.114.2.2 "cd /usr/local/projects/ai_report && ./status_all.sh"

# Логи
$SSH_CMD root@82.114.2.2 "cd /usr/local/projects/ai_report && docker-compose logs -f streamlit"
```

---

## Troubleshooting

### Проблема: "Не удалось подключиться к серверу"

**Решение:**
```bash
# Проверка подключения вручную
ssh -p 1194 root@82.114.2.2 "echo OK"

# Использование правильной SSH команды
SSH_CMD="ssh -p 1194" ./server_inspection.sh root@82.114.2.2
```

### Проблема: rsync не работает с кастомным портом

**Решение:**
rsync автоматически использует SSH команду из `SSH_CMD`:
```bash
SSH_CMD="ssh -p 1194" ./sync_deploy.sh root@82.114.2.2
```

---

## Рекомендации

1. **Создайте алиас** для удобства:
   ```bash
   alias deploy_vz2='SSH_CMD="ssh -p 1194"'
   deploy_vz2 ./safe_deploy.sh root@82.114.2.2
   ```

2. **Используйте SSH ключи** вместо паролей:
   ```bash
   ssh-keygen -t rsa -b 4096
   ssh-copy-id -p 1194 root@82.114.2.2
   ```

3. **Сохраните SSH команду** в переменной окружения:
   ```bash
   echo 'export SSH_CMD="ssh -p 1194"' >> ~/.bashrc
   source ~/.bashrc
   ```


