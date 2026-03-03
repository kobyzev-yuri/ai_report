# Права доступа к вкладкам

Управление тем, какие вкладки видит пользователь. Настраивается в админке (суперпользователь) или через `create_user_v2.py`.

## Доступные вкладки (актуально на сервере)

| Ключ | Название |
|------|----------|
| `assistant` | 🤖 Ассистент |
| `kb_expansion` | 📚 Расширение KB |
| `confluence_librarian` | 🛰️ Спутниковый библиотекарь |
| `report` | 💰 Расходы Иридиум |
| `revenue` | 💰 Доходы |
| `analytics` | 📋 Счета за период |
| `loader` | 📥 Data Loader |
| `bills` | 📄 Рассылка счетов |
| `campaigns` | 📧 Кампании |

## Миграция БД (один раз)

```bash
python3 -c "from auth_db_v2 import init_db; init_db()"
```

## Назначение прав через CLI

```bash
# Суперпользователь
python create_user_v2.py create --username admin --password secret --superuser

# Пользователь с выбранными вкладками
python create_user_v2.py create --username operator --password pass --tabs loader bills campaigns

# Обновить права
python create_user_v2.py update-permissions --username operator --tabs loader report bills campaigns
```

## После смены кода (tabs/auth)

Синхронизировать deploy на сервер и перезапустить Streamlit (см. [deploy.md](deploy.md)).
