# Быстрое развертывание RAG системы

## Вариант 1: Docker Compose (Рекомендуется) ⭐

### Быстрый старт

```bash
# 1. Настройка
cp config.env.example config.env
nano config.env  # Заполните Oracle настройки

# 2. Развертывание
./deploy.sh docker

# Готово! Приложение доступно на http://localhost:8504
```

### Что происходит:

1. ✅ Запускается Qdrant (векторная БД)
2. ✅ Собирается Docker образ Streamlit
3. ✅ Автоматически инициализируется KB в Qdrant
4. ✅ Запускается Streamlit с ассистентом

### Управление:

```bash
# Статус
./status_all.sh
docker-compose ps

# Логи
docker-compose logs -f streamlit
docker-compose logs -f qdrant

# Остановка
docker-compose down

# Перезапуск
docker-compose restart streamlit
```

---

## Вариант 2: Ручное развертывание

### Быстрый старт

```bash
# 1. Настройка
cp config.env.example config.env
nano config.env

# 2. Установка зависимостей
pip install -r requirements.txt

# 3. Запуск Qdrant
docker run -d \
  --name ai_report_qdrant \
  --restart unless-stopped \
  -p 6333:6333 \
  -v qdrant_storage:/qdrant/storage \
  qdrant/qdrant:latest

# 4. Инициализация KB
./init_kb.sh

# 5. Запуск Streamlit
./run_streamlit_background.sh

# Или все вместе
./deploy.sh manual
```

### Управление:

```bash
# Статус
./status_all.sh

# Остановка
./stop_all.sh

# Перезапуск KB
./init_kb.sh --recreate
```

---

## Проверка работы

### 1. Проверка Qdrant

```bash
curl http://localhost:6333/health
curl http://localhost:6333/collections/kb_billing
```

### 2. Проверка Streamlit

```bash
curl http://localhost:8504/_stcore/health
```

### 3. Проверка в браузере

Откройте: `http://localhost:8504` (или через nginx proxy)

Перейдите на закладку "🤖 Ассистент" и проверьте работу поиска.

---

## Troubleshooting

### Qdrant не запускается

```bash
# Проверка порта
netstat -tuln | grep 6333

# Перезапуск
docker restart ai_report_qdrant
```

### KB не инициализирована

```bash
# Проверка коллекции
curl http://localhost:6333/collections/kb_billing

# Пересоздание
./init_kb.sh --recreate
```

### Streamlit не запускается

```bash
# Проверка логов
tail -f streamlit_8504.log

# Проверка зависимостей
pip install -r requirements.txt
```

---

## Дополнительная информация

- Полная документация: `DEPLOYMENT_RAG.md`
- Конфигурация Oracle: `kb_billing/rag/ORACLE_CONFIG.md`
- Быстрый старт RAG: `kb_billing/rag/QUICK_START.md`


