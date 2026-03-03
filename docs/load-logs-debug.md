# Отладка загрузки CSV/XLSX (LOAD_LOGS)

На сервере используется **Oracle**. Таблица `LOAD_LOGS` учитывает успешно загруженные файлы; при отсутствии записей система может пытаться загрузить файлы заново.

## Контроль количества записей

- Подсчёт записей в файле (CSV/XLSX) до загрузки
- Проверка количества записей в БД после загрузки
- Обнаружение неполных загрузок и автоматическая перезагрузка при следующем импорте

В интерфейсе Data Loader: колонки **Records in File** и **Records in DB**.

## Проверка и восстановление истории

**Через веб:** вкладка **Data Loader** → **🔧 Debug Tools** → «Проверить отсутствующие записи» / «Восстановить историю загрузок».

**Через CLI (Oracle):**

```bash
cd python
# Проверка
python restore_load_history.py --check-only --db-type oracle

# Восстановление (сначала dry-run)
python restore_load_history.py --dry-run --db-type oracle
python restore_load_history.py --db-type oracle
```

## Структура LOAD_LOGS (Oracle)

- `FILE_NAME`, `TABLE_NAME` — файл и таблица (SPNET_TRAFFIC, STECCOM_EXPENSES)
- `RECORDS_LOADED`, `LOAD_STATUS` (SUCCESS, FAILED, PARTIAL)
- `LOAD_START_TIME`, `LOAD_END_TIME`, `LOADED_BY`

## Рекомендации

1. Перед первой загрузкой — проверить отсутствующие записи.
2. После восстановления — проверить статистику на вкладке Debug Tools.
3. При проблемах — смотреть **Load History** в интерфейсе.
