# Система авторизации для отчетов по Iridium M2M

## Описание

Добавлена простая система управления пользователями с авторизацией через SQLite базу данных.

## Компоненты

1. **auth_db.py** - модуль для работы с базой данных пользователей (SQLite)
2. **create_user.py** - скрипт командной строки для управления пользователями
3. **Интеграция в Streamlit** - авторизация добавлена в оба приложения:
   - `streamlit_report_oracle_backup.py`
   - `streamlit_report_postgresql_backup.py`

## Установка зависимостей

```bash
pip install bcrypt>=4.0.0
```

Или установить все зависимости:
```bash
pip install -r requirements.txt
```

## Создание первого пользователя (суперпользователя)

```bash
python create_user.py create --username admin --password your_password --superuser
```

## Управление пользователями через CLI

### Создать пользователя
```bash
python create_user.py create --username user1 --password pass123
```

### Создать суперпользователя
```bash
python create_user.py create --username admin --password admin123 --superuser
```

### Список всех пользователей
```bash
python create_user.py list
```

### Изменить пароль
```bash
python create_user.py change-password --username user1 --password newpass123
```

### Удалить пользователя
```bash
python create_user.py delete --username user1
```

## Использование в Streamlit

1. При первом запуске приложения отобразится страница авторизации
2. **Регистрация через интерфейс отключена** - пользователи создаются только через скрипт `create_user.py`
3. После входа доступен основной интерфейс отчетов
4. Суперпользователи могут создавать новых пользователей через боковую панель

## База данных

База данных SQLite создается автоматически в файле `users.db` в корне проекта при первом запуске.

Структура таблицы:
- `id` - уникальный идентификатор
- `username` - имя пользователя (уникальное)
- `password_hash` - хеш пароля (bcrypt)
- `is_superuser` - флаг суперпользователя (0/1)
- `created_at` - дата создания
- `created_by` - кто создал пользователя
- `last_login` - дата последнего входа

## Безопасность

- Пароли хранятся в виде bcrypt хешей
- Суперпользователи не могут быть удалены
- Текущий пользователь не может удалить сам себя
- Автоматическое отслеживание последнего входа

## Примеры использования

### Первоначальная настройка

```bash
# 1. Создать суперпользователя
python create_user.py create --username admin --password admin123 --superuser

# 2. Создать обычных пользователей
python create_user.py create --username user1 --password pass123
python create_user.py create --username user2 --password pass456

# 3. Проверить список
python create_user.py list
```

### В Streamlit интерфейсе

1. Запустить приложение
2. На странице авторизации можно:
   - Войти существующим пользователем
   - Зарегистрировать нового пользователя
3. Суперпользователи видят в боковой панели:
   - Раздел "Управление пользователями"
   - Возможность создавать новых пользователей
   - Список всех пользователей с возможностью удаления

