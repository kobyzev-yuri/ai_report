# Развертывание RAG системы с Qdrant и Streamlit

## Обзор

Комплексное решение для развертывания RAG системы на сервере включает:
- **Qdrant** - векторная БД для KB
- **Streamlit** - веб-интерфейс с ассистентом
- **Инициализация KB** - автоматическая загрузка данных в Qdrant

## Варианты развертывания

### Вариант 1: Docker Compose (Рекомендуется)

Полностью автоматизированное развертывание через Docker Compose.

#### Преимущества:
- ✅ Автоматическая оркестрация сервисов
- ✅ Изоляция окружения
- ✅ Простое управление (запуск/остановка)
- ✅ Автоматическая инициализация KB
- ✅ Health checks и перезапуск при сбоях

#### Требования:
- Docker
- Docker Compose

#### Развертывание:

```bash
cd /path/to/deploy

# 1. Настройка конфигурации
cp config.env.example config.env
nano config.env  # Заполните Oracle настройки

# 2. Развертывание
./deploy.sh docker

# Или напрямую
docker-compose up -d --build
```

#### Управление:

```bash
# Статус
docker-compose ps
./status_all.sh

# Логи
docker-compose logs -f streamlit
docker-compose logs -f qdrant

# Остановка
docker-compose down

# Перезапуск
docker-compose restart streamlit
```

### Вариант 2: Ручное развертывание

Развертывание без Docker Compose (Qdrant через Docker, Streamlit напрямую).

#### Преимущества:
- ✅ Больше контроля
- ✅ Проще отладка
- ✅ Не требует Docker Compose

#### Развертывание:

```bash
cd /path/to/deploy

# 1. Настройка конфигурации
cp config.env.example config.env
nano config.env

# 2. Установка зависимостей
pip install -r requirements.txt

# 3. Запуск Qdrant (через Docker)
docker run -d \
  --name ai_report_qdrant \
  --restart unless-stopped \
  -p 6333:6333 \
  -p 6334:6334 \
  -v qdrant_storage:/qdrant/storage \
  qdrant/qdrant:latest

# 4. Инициализация KB
./init_kb.sh

# 5. Запуск Streamlit
./run_streamlit_background.sh

# Или все вместе
./deploy.sh manual
```

#### Управление:

```bash
# Статус
./status_all.sh

# Остановка
./stop_all.sh

# Перезапуск KB
./init_kb.sh --recreate
```

## Структура файлов

```
deploy/
├── docker-compose.yml          # Docker Compose конфигурация
├── Dockerfile.streamlit        # Dockerfile для Streamlit
├── deploy.sh                   # Скрипт развертывания
├── init_kb.sh                  # Инициализация KB
├── stop_all.sh                 # Остановка всех сервисов
├── status_all.sh               # Статус всех сервисов
├── config.env                  # Конфигурация (создать из example)
└── DEPLOYMENT_RAG.md           # Эта документация
```

## Конфигурация

### config.env

```bash
# Oracle Database
ORACLE_USER=billing7
ORACLE_PASSWORD=billing
ORACLE_HOST=192.168.3.35
ORACLE_PORT=1521
ORACLE_SID=bm7
ORACLE_SERVICE=bm7
ORACLE_SCHEMA=billing

# Qdrant
QDRANT_HOST=localhost  # или qdrant для Docker Compose
QDRANT_PORT=6333
QDRANT_COLLECTION=kb_billing

# Модель эмбеддингов
EMBEDDING_MODEL=intfloat/multilingual-e5-base

# Streamlit
BASE_URL_PATH=/ai_report  # для proxy через nginx
```

## Порты

- **8504** - Streamlit веб-интерфейс
- **6333** - Qdrant REST API
- **6334** - Qdrant gRPC (опционально)

## Volumes (Docker)

- `qdrant_storage` - данные Qdrant
- `qdrant_config` - конфигурация Qdrant
- `streamlit_data` - данные Streamlit
- `streamlit_cache` - кэш Streamlit

## Инициализация KB

KB автоматически инициализируется при первом запуске через Docker Compose.

Для ручного развертывания:

```bash
./init_kb.sh
```

Или напрямую:

```bash
python kb_billing/rag/init_kb.py \
  --host localhost \
  --port 6333 \
  --collection kb_billing \
  --model intfloat/multilingual-e5-base
```

## Мониторинг

### Проверка здоровья

```bash
# Qdrant
curl http://localhost:6333/health

# Streamlit
curl http://localhost:8504/_stcore/health

# Статус коллекции
curl http://localhost:6333/collections/kb_billing
```

### Логи

```bash
# Docker Compose
docker-compose logs -f streamlit
docker-compose logs -f qdrant

# Ручное развертывание
tail -f streamlit_8504.log
docker logs -f ai_report_qdrant
```

## Обновление

### Обновление KB

```bash
# Пересоздать коллекцию и загрузить данные
./init_kb.sh --recreate
```

### Обновление приложения

```bash
# Docker Compose
docker-compose pull
docker-compose up -d --build

# Ручное развертывание
git pull  # или обновить файлы
pip install -r requirements.txt
./restart_streamlit.sh
```

## Troubleshooting

### Qdrant недоступен

```bash
# Проверка
curl http://localhost:6333/health

# Перезапуск
docker restart ai_report_qdrant
# или
docker-compose restart qdrant
```

### KB не инициализирована

```bash
# Проверка коллекции
curl http://localhost:6333/collections/kb_billing

# Инициализация
./init_kb.sh --recreate
```

### Streamlit не запускается

```bash
# Проверка логов
tail -f streamlit_8504.log

# Проверка порта
netstat -tuln | grep 8504

# Перезапуск
./restart_streamlit.sh
```

### Проблемы с Oracle подключением

```bash
# Проверка настроек
source config.env
echo $ORACLE_HOST:$ORACLE_PORT

# Тест подключения
sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE
```

## Безопасность

### Рекомендации:

1. **Не экспонируйте Qdrant наружу**
   - Используйте только внутри Docker сети
   - Или ограничьте доступ через firewall

2. **Защитите config.env**
   ```bash
   chmod 600 config.env
   ```

3. **Используйте reverse proxy (nginx)**
   - Для Streamlit через BASE_URL_PATH
   - SSL/TLS для HTTPS

4. **Регулярные бэкапы**
   ```bash
   # Бэкап Qdrant
   docker exec ai_report_qdrant tar czf /qdrant/storage/backup.tar.gz /qdrant/storage
   ```

## Производительность

### Оптимизация Qdrant

```yaml
# В docker-compose.yml можно добавить:
environment:
  - QDRANT__SERVICE__MAX_REQUEST_SIZE_MB=32
  - QDRANT__SERVICE__MAX_WORKERS=4
```

### Оптимизация Streamlit

```bash
# Увеличить лимиты памяти для модели эмбеддингов
export PYTORCH_CUDA_ALLOC_CONF=max_split_size_mb:128
```

## Масштабирование

Для production окружения:

1. **Qdrant Cloud** - вместо локального Qdrant
2. **Load Balancer** - для нескольких Streamlit инстансов
3. **Кэширование** - Redis для результатов поиска
4. **Мониторинг** - Prometheus + Grafana

## Дополнительная информация

- Конфигурация Oracle: `kb_billing/rag/ORACLE_CONFIG.md`
- Конфигурация sql4A: `kb_billing/rag/SQL4A_CONFIG.md`
- Быстрый старт: `kb_billing/rag/QUICK_START.md`












