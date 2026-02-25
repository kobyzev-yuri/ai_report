# Чеклист деплоя ai_report на сервер

> **Примечание:** Замените `your-server-ip` и другие плейсхолдеры на реальные значения перед использованием.

## ✅ Выполнено

1. ✅ Старая директория переименована: `ai_report` → `ai_old_report`
2. ✅ Новая пустая директория создана: `ai_report`
3. ✅ Минимальный деплой синхронизирован (63 файла, ~696KB)
4. ✅ `config.env` и `users.db` скопированы из старой директории

## 📋 Следующие шаги на сервере

### 1. Проверка файлов

```bash
cd /usr/local/projects/ai_report
ls -la
```

Должны быть:
- ✅ `streamlit_report_oracle_backup.py`
- ✅ `config.env` (скопирован из ai_old_report)
- ✅ `users.db` (скопирован из ai_old_report)
- ✅ `requirements.txt`
- ✅ `oracle/` (tables, views, functions, data)
- ✅ `python/` (загрузчики данных)
- ✅ `kb_billing/` (база знаний)

### 2. Остановка старого Streamlit (если запущен)

```bash
cd /usr/local/projects/ai_old_report
./stop_streamlit.sh
# или
kill $(cat streamlit_8504.pid 2>/dev/null) 2>/dev/null || echo "Процесс не найден"
```

### 3. Проверка зависимостей

```bash
cd /usr/local/projects/ai_report
pip install -r requirements.txt
```

### 4. Проверка подключения к Oracle

```bash
cd /usr/local/projects/ai_report
source config.env
sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE << EOF
SELECT COUNT(*) FROM SPNET_TRAFFIC;
EXIT;
EOF
```

### 5. Запуск нового приложения

```bash
cd /usr/local/projects/ai_report
./run_streamlit_background.sh
```

### 6. Проверка статуса

```bash
cd /usr/local/projects/ai_report
./status_streamlit.sh
tail -f streamlit_8504.log
```

### 7. Проверка в браузере

Откройте: `stat.steccom.ru:7776/ai_report`

### 8. Проверка доступа к Confluence (docs.steccom.ru) на vz2

После деплоя нужно убедиться, что с сервера (vz2) доступен docs.steccom.ru и токен в config.env работает. Затем можно тестировать интеграцию реальных разделов (вкладка «Спутниковый библиотекарь»).

```bash
cd /usr/local/projects/ai_report
# Убедитесь, что в config.env есть CONFLUENCE_URL и CONFLUENCE_TOKEN
./deploy/test_confluence_access.sh
```

При успехе: «✅ Доступ к docs.steccom.ru с сервера есть». При ошибке — проверить сеть/firewall и актуальность токена.

## 🔍 Проверка миграции данных

### Проверка таблиц Oracle

```bash
cd /usr/local/projects/ai_report
source config.env
sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE << EOF
-- Проверка таблиц
SELECT COUNT(*) as spnet_count FROM SPNET_TRAFFIC;
SELECT COUNT(*) as steccom_count FROM STECCOM_EXPENSES;
SELECT COUNT(*) as logs_count FROM LOAD_LOGS;

-- Проверка представлений
SELECT COUNT(*) as v_overage FROM V_SPNET_OVERAGE_ANALYSIS;
SELECT COUNT(*) as v_consolidated FROM V_CONSOLIDATED_OVERAGE_REPORT;
SELECT COUNT(*) as v_billing FROM V_CONSOLIDATED_REPORT_WITH_BILLING;

EXIT;
EOF
```

### Проверка загрузки данных через интерфейс

1. Откройте `stat.steccom.ru:7776/ai_report`
2. Перейдите на вкладку "Data Loader"
3. Проверьте список файлов в директориях:
   - `data/SPNet reports/`
   - `data/STECCOMLLCRussiaSBD.AccessFees_reports/`

## 🐛 Возможные проблемы

### Проблема: Streamlit не запускается

```bash
# Проверьте логи
tail -50 streamlit_8504.log

# Проверьте порт
netstat -tlnp | grep 8504

# Проверьте конфигурацию
cat config.env
```

### Проблема: Ошибка подключения к Oracle

```bash
# Проверьте переменные окружения
source config.env
echo $ORACLE_USER
echo $ORACLE_HOST
echo $ORACLE_SERVICE

# Проверьте подключение
sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE
```

### Проблема: Модули не найдены

```bash
# Переустановите зависимости
pip install -r requirements.txt --upgrade
```

## 📊 Размеры директорий

```bash
du -sh /usr/local/projects/ai_report
du -sh /usr/local/projects/ai_old_report
```

Ожидаемый размер новой директории: ~1-2 MB (без данных)

## 🗑️ Очистка после успешной миграции

После успешной проверки работы можно удалить старую директорию:

```bash
# ВНИМАНИЕ: Убедитесь, что все работает!
cd /usr/local/projects
rm -rf ai_old_report
```

## 📝 Логи

Все логи находятся в:
- `streamlit_8504.log` - логи Streamlit приложения
- `streamlit_8504.pid` - PID файл процесса

