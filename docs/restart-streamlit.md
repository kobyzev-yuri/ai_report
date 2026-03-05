# Перезапуск Streamlit и очистка кэша

## Быстрый способ

```bash
# Аккуратный перезапуск с очисткой кэша
./restart_streamlit.sh
```

## Пошаговый способ

### 1. Остановка Streamlit

```bash
# Использование скрипта
./stop_streamlit.sh

# Или вручную
kill $(cat streamlit_8504.pid)
```

### 2. Очистка кэша

#### Кэш на сервере (автоматически при использовании restart_streamlit.sh)

```bash
# Кэш Streamlit
rm -rf ~/.streamlit/cache/*

# Локальный кэш (если есть)
rm -rf .streamlit/cache/*
```

#### Кэш в браузере

**Chrome/Edge:**
1. Откройте DevTools (F12)
2. Правый клик на кнопку обновления страницы
3. Выберите "Очистить кэш и жесткая перезагрузка" (Empty Cache and Hard Reload)

**Или через настройки:**
1. Ctrl+Shift+Delete (Windows/Linux) или Cmd+Shift+Delete (Mac)
2. Выберите "Кэшированные изображения и файлы"
3. Нажмите "Очистить данные"

**Firefox:**
1. Ctrl+Shift+Delete (Windows/Linux) или Cmd+Shift+Delete (Mac)
2. Выберите "Кэш"
3. Нажмите "Очистить сейчас"

### 3. Запуск Streamlit

```bash
./run_streamlit_background.sh
```

## Проверка статуса

```bash
# Проверка статуса
./status_streamlit.sh

# Просмотр логов
tail -f streamlit_8504.log
```

## Очистка session_state в Streamlit

Если нужно очистить кэш без перезапуска, можно добавить кнопку в интерфейс:

```python
if st.button("🔄 Очистить кэш"):
    st.cache_data.clear()
    if 'last_report_df' in st.session_state:
        del st.session_state['last_report_df']
    if 'last_report_key' in st.session_state:
        del st.session_state['last_report_key']
    st.success("Кэш очищен!")
    st.rerun()
```

## Типы кэша в Streamlit

1. **`@st.cache_data`** - кэш данных (TTL = 5 минут)
   - Очищается автоматически через 5 минут
   - Можно очистить через `st.cache_data.clear()`

2. **`st.session_state`** - состояние сессии
   - Очищается при перезапуске Streamlit
   - Хранит: `last_report_df`, `last_report_key`, `selected_period_index`

3. **Кэш браузера** - кэш статических файлов
   - Очищается через настройки браузера или DevTools

## После правок в RAG (rag_assistant.py и др.)

Чтобы на сервере подхватился обновлённый код ассистента:

1. **Локально** обновите deploy из текущего кода (если деплой идёт через `sync_deploy.sh`):
   ```bash
   ./prepare_deployment.sh
   ```

2. **Синхронизация на сервер:**
   - через deploy: `./sync_deploy.sh`
   - или только KB + RAG: `./sync_and_update_kb.sh` (или `--only-examples` для быстрого обновления примеров)

3. **На сервере** перезапуск с очисткой кэша (в т.ч. `kb_billing/rag/__pycache__`):
   ```bash
   cd /usr/local/projects/ai_report   # или ваш REMOTE_DIR
   ./restart_streamlit.sh
   ```

4. В браузере: жёсткое обновление страницы (Ctrl+F5) или очистка кэша страницы.

Если после этого ассистент всё ещё выдаёт старый SQL — проверьте на сервере, что в коде есть правка:
   ```bash
   grep -n "Убираю MAX" kb_billing/rag/rag_assistant.py
   ```
   Должна быть строка с текстом про ORA-00934.

---

## Устранение проблем

### Проблема: Данные не обновляются после изменений в базе

**Решение:**
1. Перезапустите Streamlit: `./restart_streamlit.sh`
2. Очистите кэш браузера (Ctrl+Shift+Delete)
3. Обновите страницу (Ctrl+F5 или Cmd+Shift+R)

### Проблема: Streamlit не запускается

**Решение:**
1. Проверьте, не запущен ли уже процесс:
   ```bash
   ./status_streamlit.sh
   ```
2. Если процесс завис, остановите принудительно:
   ```bash
   ./stop_streamlit.sh
   # Или
   pkill -9 -f "streamlit.*8504"
   ```
3. Удалите PID файл:
   ```bash
   rm streamlit_8504.pid
   ```
4. Запустите заново:
   ```bash
   ./run_streamlit_background.sh
   ```

### Проблема: Порт занят

**Решение:**
```bash
# Найти процесс на порту 8504
lsof -i :8504

# Остановить процесс
kill $(lsof -t -i :8504)
```

## Автоматическая очистка кэша при изменении данных

Если нужно автоматически очищать кэш при загрузке новых данных, можно добавить в скрипты загрузки:

```python
# После успешной загрузки данных
import streamlit as st
if hasattr(st, 'cache_data'):
    st.cache_data.clear()
```

