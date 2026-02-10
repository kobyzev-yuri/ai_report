# 🚀 Быстрая справка: Развертывание ai_report

## ⚡ Самые частые команды

### Первое развертывание
```bash
cd deploy
SSH_CMD="ssh -p 1194" ./safe_deploy.sh root@82.114.2.2
```

### Обновление кода
```bash
cd deploy
SSH_CMD="ssh -p 1194" ./sync_deploy.sh root@82.114.2.2
ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && docker-compose restart streamlit"
```

### Обновление базы знаний
```bash
SSH_CMD="ssh -p 1194" ./sync_and_update_kb.sh root@82.114.2.2
```

### Проверка статуса
```bash
ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && ./status_all.sh"
```

### Просмотр логов
```bash
ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && docker-compose logs -f streamlit"
```

---

## 📋 Методы развертывания

| Метод | Команда | Время | Использование |
|-------|---------|-------|---------------|
| **Безопасное** ⭐ | `./safe_deploy.sh` | 5 мин | Первое развертывание |
| **Docker Compose** | `./deploy.sh docker` | 3 мин | Production |
| **Синхронизация** | `./sync_deploy.sh` | 1 мин | Обновление кода |
| **KB Update** | `./sync_and_update_kb.sh` | 2 мин | Обновление примеров |
| **Ручное** | `./deploy.sh manual` | 10 мин | Отладка |

---

## 🔧 Диагностика

### Health Checks
```bash
# Qdrant
ssh -p 1194 root@82.114.2.2 "curl http://localhost:6333/health"

# Streamlit
ssh -p 1194 root@82.114.2.2 "curl http://localhost:8504/_stcore/health"

# KB коллекция
ssh -p 1194 root@82.114.2.2 "curl http://localhost:6333/collections/kb_billing | jq '.result.points_count'"
```

### Перезапуск
```bash
# Streamlit
ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && docker-compose restart streamlit"

# Все сервисы
ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && docker-compose restart"
```

---

## 🌐 Доступ

- **Прямой:** http://82.114.2.2:8504
- **Nginx:** http://stat.steccom.ru:7776/ai_report

---

## 📖 Документация

- `DEPLOYMENT_METHODS_ANALYSIS.md` - Полный анализ
- `DEPLOYMENT_SUMMARY_RU.md` - Краткая сводка
- `DEPLOYMENT_VISUAL_GUIDE.md` - Визуальное руководство
- `deploy/START_HERE.md` - Начните отсюда
