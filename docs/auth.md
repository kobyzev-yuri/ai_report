# Авторизация и пользователи

Управление пользователями через SQLite (`users.db`). На сервере используется приложение **Oracle** (`streamlit_report_oracle_backup.py`).

## Компоненты

- **auth_db.py** / **auth_db_v2.py** — работа с БД пользователей (SQLite), права на вкладки (v2)
- **create_user.py** / **create_user_v2.py** — CLI: создание пользователей и назначение прав (v2)

## Зависимости

```bash
pip install bcrypt>=4.0.0
# или
pip install -r requirements.txt
```

## Первый пользователь (суперпользователь)

```bash
python create_user_v2.py create --username admin --password your_password --superuser
```

## CLI (create_user_v2.py)

```bash
# Список пользователей
python create_user_v2.py list

# Пользователь с доступом к вкладкам
python create_user_v2.py create --username analyst --password pass --tabs assistant report revenue analytics

# Обновить права
python create_user_v2.py update-permissions --username analyst --tabs assistant report revenue analytics loader bills campaigns

# Список доступных вкладок
python create_user_v2.py show-tabs
```

## В Streamlit

- Регистрация через интерфейс отключена; пользователи создаются через CLI.
- Суперпользователь видит в боковой панели «Управление пользователями» и может назначать права на вкладки (админка).

## База users.db

Создаётся автоматически в корне проекта. Пароли — bcrypt. Права на вкладки хранятся в колонке `allowed_tabs` (auth_db_v2).
