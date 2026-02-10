# 🔐 Система прав доступа к вкладкам - Руководство

## 📋 Обзор

Новая система прав доступа позволяет администратору контролировать, какие вкладки доступны каждому пользователю в Streamlit приложении.

## 🎯 Основные возможности

1. **Гранулярный контроль доступа** - каждому пользователю можно назначить доступ к конкретным вкладкам
2. **Суперпользователи** - имеют доступ ко всем вкладкам автоматически
3. **Управление через CLI** - простое управление правами через командную строку
4. **Управление через UI** - интерфейс для администраторов в Streamlit

## 📊 Доступные вкладки

| Ключ | Название | Описание |
|------|----------|----------|
| `assistant` | 🤖 Ассистент | RAG-ассистент для генерации SQL запросов |
| `kb_expansion` | 📚 Расширение KB | Добавление примеров в базу знаний |
| `report` | 💰 Расходы Иридиум | Основной отчет по расходам |
| `revenue` | 💰 Доходы | Отчет по доходам из счетов-фактур |
| `analytics` | 📋 Счета за период | Отчет по счетам из ANALYTICS |
| `loader` | 📥 Data Loader | Загрузка CSV/Excel файлов |
| `ifindex` | 7206_ifindex | Технический отчет по ifindex |
| `ifindex_mapping` | 🔀 Маппинг индексов 7206 | Технический отчет по маппингу |

## 🚀 Быстрый старт

### 1. Миграция базы данных

Обновите существующую базу данных пользователей:

```bash
# Автоматическая миграция при первом импорте
python3 -c "from auth_db_v2 import init_db; init_db()"
```

Это добавит колонку `allowed_tabs` в таблицу `users` и обновит права существующих суперпользователей.

### 2. Создание пользователей с правами

```bash
# Суперпользователь (доступ ко всем вкладкам)
python create_user_v2.py create --username admin --password secret123 --superuser

# Пользователь только с отчетами
python create_user_v2.py create --username accountant --password pass123 --tabs report revenue

# Аналитик с доступом к ассистенту и отчетам
python create_user_v2.py create --username analyst --password pass123 --tabs assistant kb_expansion report revenue analytics

# Оператор загрузки данных
python create_user_v2.py create --username operator --password pass123 --tabs loader report
```

### 3. Просмотр пользователей и их прав

```bash
python create_user_v2.py list
```

Вывод:
```
================================================================================
СПИСОК ПОЛЬЗОВАТЕЛЕЙ С ПРАВАМИ ДОСТУПА
================================================================================

👤 Имя пользователя: admin [SUPERUSER]
   Создан: 2025-12-28T18:00:00
   Последний вход: 2025-12-28T18:30:00
   Разрешенные вкладки:
      👑 ВСЕ ВКЛАДКИ (суперпользователь)

👤 Имя пользователя: analyst
   Создан: 2025-12-28T18:05:00
   Последний вход: 2025-12-28T18:25:00
   Разрешенные вкладки:
      • assistant: 🤖 Ассистент
      • kb_expansion: 📚 Расширение KB
      • report: 💰 Расходы Иридиум
      • revenue: 💰 Доходы
      • analytics: 📋 Счета за период
```

### 4. Обновление прав пользователя

```bash
# Добавить доступ к новым вкладкам
python create_user_v2.py update-permissions --username analyst --tabs assistant kb_expansion report revenue analytics loader

# Ограничить доступ только отчетами
python create_user_v2.py update-permissions --username analyst --tabs report revenue
```

### 5. Просмотр всех доступных вкладок

```bash
python create_user_v2.py show-tabs
```

## 🔧 Интеграция в Streamlit приложение

### Изменения в `streamlit_report_oracle_backup.py`

#### 1. Импорт обновленного модуля

```python
# Заменить
from auth_db import (
    init_db, create_user, list_users, change_password, 
    delete_user, is_superuser, authenticate_user
)

# На
from auth_db_v2 import (
    init_db, create_user, list_users, change_password, 
    delete_user, is_superuser, authenticate_user,
    update_user_permissions, get_user_permissions, AVAILABLE_TABS
)
```

#### 2. Обновление функции аутентификации

```python
def show_login_page():
    """Отображение страницы входа"""
    st.title("🔐 Система отчетов по Iridium M2M")
    st.markdown("---")
    
    st.info("💡 Для создания учетной записи обратитесь к администратору или используйте скрипт `create_user_v2.py`")
    
    st.subheader("Вход")
    with st.form("login_form"):
        login_username = st.text_input("Имя пользователя", key="login_username")
        login_password = st.text_input("Пароль", type="password", key="login_password")
        login_submitted = st.form_submit_button("Войти", use_container_width=True)
        
        if login_submitted:
            if not login_username or not login_password:
                st.error("Заполните все поля")
            else:
                # ИЗМЕНЕНО: authenticate_user теперь возвращает 4 значения
                success, username, is_super, allowed_tabs = authenticate_user(login_username, login_password)
                if success:
                    st.session_state.authenticated = True
                    st.session_state.username = username
                    st.session_state.is_superuser = is_super
                    st.session_state.allowed_tabs = allowed_tabs  # НОВОЕ
                    st.success(f"Добро пожаловать, {username}! 👋")
                    st.rerun()
                else:
                    st.error("Неверное имя пользователя или пароль")
```

#### 3. Инициализация session state

```python
def main():
    """Основная функция приложения"""
    
    # Инициализация session state для авторизации
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    if 'username' not in st.session_state:
        st.session_state.username = None
    if 'is_superuser' not in st.session_state:
        st.session_state.is_superuser = False
    if 'allowed_tabs' not in st.session_state:  # НОВОЕ
        st.session_state.allowed_tabs = []
```

#### 4. Фильтрация вкладок по правам доступа

```python
# Получаем разрешенные вкладки для пользователя
allowed_tabs = st.session_state.get('allowed_tabs', [])

# Определяем, какие вкладки показывать
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
if 'ifindex' in allowed_tabs:
    tab_configs.append(('tab_ifindex', '7206_ifindex'))
if 'ifindex_mapping' in allowed_tabs:
    tab_configs.append(('tab_ifindex_mapping', '🔀 Маппинг индексов 7206'))

# Если нет доступных вкладок, показываем сообщение
if not tab_configs:
    st.error("❌ У вас нет доступа ни к одной вкладке. Обратитесь к администратору.")
    st.stop()

# Создаем вкладки динамически
tab_names = [name for _, name in tab_configs]
tabs = st.tabs(tab_names)

# Отображаем содержимое вкладок
for i, (tab_key, tab_name) in enumerate(tab_configs):
    with tabs[i]:
        if tab_key == 'tab_assistant':
            # Код для вкладки Ассистент
            pass
        elif tab_key == 'tab_kb_expansion':
            # Код для вкладки Расширение KB
            pass
        # ... и т.д. для остальных вкладок
```

#### 5. Обновление функции управления пользователями

```python
def show_user_management():
    """Отображение управления пользователями (только для суперпользователей)"""
    if not st.session_state.get('is_superuser', False):
        return
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("👥 Управление пользователями")
    
    # Создание нового пользователя
    with st.sidebar.expander("➕ Создать пользователя"):
        with st.form("create_user_form"):
            new_username = st.text_input("Имя пользователя")
            new_password = st.text_input("Пароль", type="password")
            new_is_super = st.checkbox("Суперпользователь")
            
            # НОВОЕ: Выбор разрешенных вкладок
            if not new_is_super:
                st.write("Разрешенные вкладки:")
                selected_tabs = []
                for tab_key, tab_name in AVAILABLE_TABS.items():
                    if st.checkbox(tab_name, key=f"new_tab_{tab_key}"):
                        selected_tabs.append(tab_key)
            else:
                selected_tabs = None  # Суперпользователь получит все вкладки
            
            create_submitted = st.form_submit_button("Создать")
            
            if create_submitted:
                success, message = create_user(
                    new_username, 
                    new_password, 
                    is_superuser=new_is_super,
                    allowed_tabs=selected_tabs,  # НОВОЕ
                    created_by=st.session_state.username
                )
                if success:
                    st.sidebar.success(message)
                    st.rerun()
                else:
                    st.sidebar.error(message)
    
    # Список пользователей с управлением правами
    with st.sidebar.expander("📋 Список пользователей"):
        users = list_users()
        if users:
            for user in users:
                superuser_mark = " 👑" if user['is_superuser'] else ""
                st.write(f"**{user['username']}**{superuser_mark}")
                if user['last_login']:
                    st.caption(f"Последний вход: {user['last_login'][:10]}")
                
                # Показываем разрешенные вкладки
                if not user['is_superuser']:
                    allowed = user.get('allowed_tabs', [])
                    if allowed:
                        st.caption(f"Вкладки: {', '.join([AVAILABLE_TABS.get(t, t) for t in allowed[:3]])}" + 
                                 (f" +{len(allowed)-3}" if len(allowed) > 3 else ""))
                    else:
                        st.caption("⚠️ Нет доступа")
                
                # Кнопка редактирования прав (кроме текущего пользователя и суперпользователей)
                if user['username'] != st.session_state.username and not user['is_superuser']:
                    if st.button("✏️ Изменить права", key=f"edit_{user['username']}"):
                        st.session_state.editing_user = user['username']
                        st.session_state.editing_tabs = user.get('allowed_tabs', [])
                        st.rerun()
                    
                    if st.button("🗑️ Удалить", key=f"delete_{user['username']}"):
                        success, message = delete_user(user['username'])
                        if success:
                            st.sidebar.success(message)
                            st.rerun()
                        else:
                            st.sidebar.error(message)
        else:
            st.write("Пользователи не найдены")
    
    # Редактирование прав пользователя
    if st.session_state.get('editing_user'):
        with st.sidebar.expander(f"✏️ Редактирование: {st.session_state.editing_user}", expanded=True):
            with st.form("edit_permissions_form"):
                st.write("Выберите разрешенные вкладки:")
                selected_tabs = []
                current_tabs = st.session_state.get('editing_tabs', [])
                
                for tab_key, tab_name in AVAILABLE_TABS.items():
                    if st.checkbox(tab_name, value=(tab_key in current_tabs), key=f"edit_tab_{tab_key}"):
                        selected_tabs.append(tab_key)
                
                col1, col2 = st.columns(2)
                with col1:
                    save_submitted = st.form_submit_button("💾 Сохранить")
                with col2:
                    cancel_submitted = st.form_submit_button("❌ Отмена")
                
                if save_submitted:
                    success, message = update_user_permissions(
                        st.session_state.editing_user,
                        selected_tabs
                    )
                    if success:
                        st.sidebar.success(message)
                        del st.session_state.editing_user
                        del st.session_state.editing_tabs
                        st.rerun()
                    else:
                        st.sidebar.error(message)
                
                if cancel_submitted:
                    del st.session_state.editing_user
                    del st.session_state.editing_tabs
                    st.rerun()
```

## 📝 Примеры использования

### Создание пользователей для разных ролей

#### Бухгалтер (только отчеты)
```bash
python create_user_v2.py create \
  --username accountant \
  --password secure123 \
  --tabs report revenue
```

#### Аналитик (отчеты + ассистент)
```bash
python create_user_v2.py create \
  --username analyst \
  --password secure123 \
  --tabs assistant kb_expansion report revenue analytics
```

#### Оператор данных (загрузка + просмотр)
```bash
python create_user_v2.py create \
  --username operator \
  --password secure123 \
  --tabs loader report
```

#### Администратор (все вкладки)
```bash
python create_user_v2.py create \
  --username admin \
  --password secure123 \
  --superuser
```

### Обновление прав существующего пользователя

```bash
# Добавить доступ к ассистенту
python create_user_v2.py update-permissions \
  --username analyst \
  --tabs assistant kb_expansion report revenue analytics loader

# Ограничить доступ только отчетами
python create_user_v2.py update-permissions \
  --username analyst \
  --tabs report revenue
```

## 🔄 Миграция существующих пользователей

Если у вас уже есть пользователи в базе данных:

1. **Автоматическая миграция** - при первом запуске `auth_db_v2.py`:
   - Добавляется колонка `allowed_tabs`
   - Суперпользователи получают доступ ко всем вкладкам
   - Обычные пользователи получают доступ к базовым вкладкам (`report`, `revenue`)

2. **Ручная настройка прав**:
```bash
# Обновить права для существующих пользователей
python create_user_v2.py update-permissions --username user1 --tabs report revenue analytics
python create_user_v2.py update-permissions --username user2 --tabs assistant kb_expansion report
```

## 🔒 Безопасность

1. **Суперпользователи** - нельзя изменить их права или удалить
2. **Валидация вкладок** - система проверяет, что все указанные вкладки существуют
3. **Защита от самоудаления** - пользователь не может удалить сам себя
4. **Хеширование паролей** - используется bcrypt

## 📊 Структура базы данных

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    is_superuser INTEGER DEFAULT 0,
    allowed_tabs TEXT DEFAULT NULL,  -- JSON массив разрешенных вкладок
    created_at TEXT NOT NULL,
    created_by TEXT,
    last_login TEXT
);
```

Пример `allowed_tabs`:
```json
["assistant", "kb_expansion", "report", "revenue", "analytics"]
```

## 🚀 Развертывание на сервер

```bash
# 1. Синхронизация новых файлов
cd deploy
SSH_CMD="ssh -p 1194" ./sync_deploy.sh root@82.114.2.2

# 2. На сервере: миграция базы данных
ssh -p 1194 root@82.114.2.2
cd /usr/local/projects/ai_report
python3 -c "from auth_db_v2 import init_db; init_db()"

# 3. Создание пользователей с правами
python3 create_user_v2.py create --username admin --password secret --superuser
python3 create_user_v2.py create --username analyst --password pass --tabs assistant report revenue

# 4. Перезапуск Streamlit
docker-compose restart streamlit
```

## 📖 Дополнительная информация

- **Файлы**:
  - `auth_db_v2.py` - обновленный модуль аутентификации
  - `create_user_v2.py` - CLI для управления пользователями
  - `users.db` - SQLite база данных пользователей

- **Совместимость**:
  - Обратная совместимость с `auth_db.py`
  - Автоматическая миграция существующей базы данных

- **Поддержка**:
  - Используйте `python create_user_v2.py --help` для справки
  - Используйте `python create_user_v2.py show-tabs` для списка вкладок
