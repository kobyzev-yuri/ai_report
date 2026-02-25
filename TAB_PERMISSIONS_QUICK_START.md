# 🔐 Система прав доступа - Краткая сводка

## ✅ Что сделано

1. ✅ **auth_db_v2.py** - обновленный модуль аутентификации с поддержкой прав на вкладки
2. ✅ **create_user_v2.py** - CLI для управления пользователями и их правами
3. ✅ **TAB_PERMISSIONS_GUIDE.md** - полное руководство по использованию

## 🚀 Быстрый старт

### 1. Миграция базы данных
```bash
python3 -c "from auth_db_v2 import init_db; init_db()"
```

### 2. Создание пользователей
```bash
# Администратор (все вкладки)
python create_user_v2.py create --username admin --password secret123 --superuser

# Аналитик (ассистент + отчеты)
python create_user_v2.py create --username analyst --password pass123 --tabs assistant kb_expansion report revenue analytics

# Бухгалтер (только отчеты)
python create_user_v2.py create --username accountant --password pass123 --tabs report revenue

# Оператор (загрузка данных)
python create_user_v2.py create --username operator --password pass123 --tabs loader report
```

### 3. Просмотр пользователей
```bash
python create_user_v2.py list
```

### 4. Обновление прав
```bash
python create_user_v2.py update-permissions --username analyst --tabs assistant report revenue analytics loader
```

## 📊 Доступные вкладки

| Ключ | Название |
|------|----------|
| `assistant` | 🤖 Ассистент |
| `kb_expansion` | 📚 Расширение KB |
| `report` | 💰 Расходы Иридиум |
| `revenue` | 💰 Доходы |
| `analytics` | 📋 Счета за период |
| `loader` | 📥 Data Loader |

## 🔧 Интеграция в Streamlit

### Минимальные изменения в `streamlit_report_oracle_backup.py`:

1. **Импорт** (строка ~13):
```python
from auth_db_v2 import (
    init_db, create_user, list_users, change_password, 
    delete_user, is_superuser, authenticate_user,
    update_user_permissions, get_user_permissions, AVAILABLE_TABS
)
```

2. **Аутентификация** (функция `show_login_page`, строка ~1018):
```python
success, username, is_super, allowed_tabs = authenticate_user(login_username, login_password)
if success:
    st.session_state.authenticated = True
    st.session_state.username = username
    st.session_state.is_superuser = is_super
    st.session_state.allowed_tabs = allowed_tabs  # НОВОЕ
```

3. **Инициализация** (функция `main`, строка ~1088):
```python
if 'allowed_tabs' not in st.session_state:
    st.session_state.allowed_tabs = []
```

4. **Фильтрация вкладок** (функция `main`, строка ~1292):
```python
# Получаем разрешенные вкладки
allowed_tabs = st.session_state.get('allowed_tabs', [])

# Формируем список вкладок динамически
tab_configs = []
if 'assistant' in allowed_tabs:
    tab_configs.append(('tab_assistant', '🤖 Ассистент'))
if 'kb_expansion' in allowed_tabs:
    tab_configs.append(('tab_kb_expansion', '📚 Расширение KB'))
if 'report' in allowed_tabs:
    tab_configs.append(('tab_report', '💰 Расходы Иридиум'))
if 'revenue' in allowed_tabs:
    tab_configs.append(('tab_revenue', '💰 Доходы'))
if 'analytics' in allowed_tabs:
    tab_configs.append(('tab_analytics', '📋 Счета за период'))
if 'loader' in allowed_tabs:
    tab_configs.append(('tab_loader', '📥 Data Loader'))

# Проверка доступа
if not tab_configs:
    st.error("❌ У вас нет доступа ни к одной вкладке. Обратитесь к администратору.")
    st.stop()

# Создаем вкладки динамически
tab_names = [name for _, name in tab_configs]
tabs = st.tabs(tab_names)

# Отображаем содержимое
for i, (tab_key, tab_name) in enumerate(tab_configs):
    with tabs[i]:
        if tab_key == 'tab_assistant':
            # Код вкладки Ассистент (строки 1304-1322)
            try:
                os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'
                from kb_billing.rag.streamlit_assistant import show_assistant_tab
                show_assistant_tab()
            except ImportError as e:
                st.error(f"❌ Ошибка импорта: {e}")
        
        elif tab_key == 'tab_kb_expansion':
            # Код вкладки Расширение KB (строки 1324-1343)
            try:
                os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'
                from kb_billing.rag.streamlit_kb_expansion import show_kb_expansion_tab
                show_kb_expansion_tab()
            except ImportError as e:
                st.error(f"❌ Ошибка импорта: {e}")
        
        elif tab_key == 'tab_report':
            # Код вкладки Расходы (строки 1346-1556)
            # ... (весь существующий код)
            pass
        
        elif tab_key == 'tab_revenue':
            # Код вкладки Доходы (строки 1558-1688)
            # ... (весь существующий код)
            pass
        
        elif tab_key == 'tab_analytics':
            # Код вкладки Счета за период (строки 1690+)
            # ... (весь существующий код)
            pass
        
        elif tab_key == 'tab_loader':
            # Код вкладки Data Loader
            # ... (весь существующий код)
            pass
```

5. **Управление пользователями** (функция `show_user_management`, строка ~1028):
```python
# Добавить выбор вкладок при создании пользователя
if not new_is_super:
    st.write("Разрешенные вкладки:")
    selected_tabs = []
    for tab_key, tab_name in AVAILABLE_TABS.items():
        if st.checkbox(tab_name, key=f"new_tab_{tab_key}"):
            selected_tabs.append(tab_key)
else:
    selected_tabs = None

# При создании
success, message = create_user(
    new_username, 
    new_password, 
    is_superuser=new_is_super,
    allowed_tabs=selected_tabs,  # НОВОЕ
    created_by=st.session_state.username
)
```

## 📦 Развертывание

```bash
# 1. Синхронизация файлов
cd deploy
SSH_CMD="ssh -p 1194" ./sync_deploy.sh root@82.114.2.2

# 2. На сервере: миграция
ssh -p 1194 root@82.114.2.2
cd /usr/local/projects/ai_report
python3 -c "from auth_db_v2 import init_db; init_db()"

# 3. Создание пользователей
python3 create_user_v2.py create --username admin --password secret --superuser

# 4. Перезапуск
docker-compose restart streamlit
```

## 📝 Примеры ролей

### Администратор
```bash
python create_user_v2.py create --username admin --password secret --superuser
```
**Доступ:** Все вкладки

### Финансовый аналитик
```bash
python create_user_v2.py create --username analyst --password pass \
  --tabs assistant kb_expansion report revenue analytics
```
**Доступ:** Ассистент, Расширение KB, Расходы, Доходы, Счета

### Бухгалтер
```bash
python create_user_v2.py create --username accountant --password pass \
  --tabs report revenue
```
**Доступ:** Расходы, Доходы

### Оператор данных
```bash
python create_user_v2.py create --username operator --password pass \
  --tabs loader report
```
**Доступ:** Data Loader, Расходы

## 🔍 Проверка

```bash
# Список всех пользователей с правами
python create_user_v2.py list

# Показать все доступные вкладки
python create_user_v2.py show-tabs

# Обновить права пользователя
python create_user_v2.py update-permissions --username analyst \
  --tabs assistant kb_expansion report revenue analytics loader
```

## 📚 Документация

- **TAB_PERMISSIONS_GUIDE.md** - полное руководство
- **auth_db_v2.py** - API документация в docstrings
- **create_user_v2.py** - help через `--help`

## ⚠️ Важно

1. **Миграция автоматическая** - при первом импорте `auth_db_v2`
2. **Суперпользователи** - всегда имеют доступ ко всем вкладкам
3. **По умолчанию** - новые пользователи получают доступ к `report` и `revenue`
4. **Безопасность** - нельзя удалить суперпользователя или изменить его права

## 🎯 Следующие шаги

1. Протестировать миграцию на локальной копии базы данных
2. Создать тестовых пользователей с разными правами
3. Интегрировать изменения в `streamlit_report_oracle_backup.py`
4. Развернуть на тестовом сервере
5. Развернуть на production сервере
