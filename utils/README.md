# utils

Утилиты для деплоя и работы приложения:

- **auth_db.py**, **auth_db_v2.py** — SQLite-база пользователей и прав доступа к вкладкам.
- **db_connection.py** — подключение к Oracle (config.env).
- **queries.py** — запросы к Oracle для отчётов и загрузки.
- **create_user.py**, **create_user_v2.py** — CLI для создания пользователей и прав (`python -m utils.create_user_v2 ...`).
- **check_customer_addresses.py** — проверка адресов клиентов в Oracle.
- **apply_oracle_view_fix.py** — применение правок к представлениям Oracle.

При деплое копируются в `deploy/utils/`. Точка входа Streamlit импортирует `from utils.auth_db`, `from utils.db_connection`, `from utils.queries`.
