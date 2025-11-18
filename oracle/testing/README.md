# Директория тестирования и диагностики

Эта директория содержит скрипты для тестирования, отладки и диагностики представлений и данных.

## Структура

```
testing/
├── checks/          # Скрипты проверки данных и представлений
├── debug/           # Скрипты отладки проблем
├── tests/           # Тестовые запросы
├── diagnostics/     # Диагностические скрипты
└── find/            # Скрипты поиска данных
```

## Категории скриптов

### checks/ - Проверки данных

Скрипты для проверки корректности данных в представлениях и таблицах:

- `check_all_fees_columns.sql` - Проверка всех колонок fees
- `check_all_periods.sql` - Проверка всех периодов
- `check_bill_month_format.sql` - Проверка формата BILL_MONTH
- `check_contract_id_mismatch.sql` - Проверка несоответствий CONTRACT_ID
- `check_fees_columns.sql` - Проверка колонок fees
- `check_fees_data.sql` - Проверка данных fees
- `check_imei_in_view.sql` - Проверка IMEI в представлении
- `check_imei_period.sql` - Проверка IMEI по периодам
- `check_invoice_data.sql` - Проверка данных инвойсов
- `check_join_fees.sql` - Проверка JOIN с fees
- `check_join_for_period.sql` - Проверка JOIN для периода
- `check_previous_month_data.sql` - Проверка данных предыдущего месяца
- `check_prorated_record.sql` - Проверка записей Prorated
- `check_record_in_view.sql` - Проверка записей в представлении
- `check_record_source.sql` - Проверка источника записей
- `check_specific_case.sql` - Проверка конкретного случая
- `check_specific_record.sql` - Проверка конкретной записи
- `quick_check.sql` - Быстрая проверка

### debug/ - Отладка

Скрипты для отладки проблем:

- `debug_bill_month_format.sql` - Отладка формата BILL_MONTH
- `debug_fees_join.sql` - Отладка JOIN с fees
- `debug_november_case.sql` - Отладка случая с ноябрем
- `debug_previous_month.sql` - Отладка предыдущего месяца

### tests/ - Тесты

Тестовые запросы для проверки функциональности:

- `test_fix.sql` - Тест исправлений
- `test_join_format.sql` - Тест формата JOIN
- `test_streamlit_query.sql` - Тест запросов Streamlit
- `test_streamlit_query_simple.sql` - Упрощенный тест Streamlit
- `test_subquery.sql` - Тест подзапросов
- `verify_new_column.sql` - Проверка новой колонки

### diagnostics/ - Диагностика

Диагностические скрипты для анализа проблем:

- `diagnose_view.sql` - Диагностика представления

### find/ - Поиск данных

Скрипты для поиска конкретных данных:

- `find_02_source.sql` - Поиск источника .02
- `find_contract_id_source.sql` - Поиск источника CONTRACT_ID
- `find_imei_november.sql` - Поиск IMEI в ноябре
- `find_october_in_source.sql` - Поиск октября в исходных данных
- `find_one_october_record.sql` - Поиск одной октябрьской записи

## Использование

### Пример: Проверка данных для конкретного IMEI

```bash
sqlplus billing7/billing@bm7 @testing/checks/check_imei_period.sql
```

### Пример: Отладка JOIN

```bash
sqlplus billing7/billing@bm7 @testing/debug/debug_fees_join.sql
```

### Пример: Поиск данных

```bash
sqlplus billing7/billing@bm7 @testing/find/find_imei_november.sql
```

## Примечания

- Все скрипты предназначены для диагностики и тестирования
- Некоторые скрипты могут требовать модификации параметров (IMEI, периоды и т.д.)
- Скрипты не изменяют данные, только читают их
- Для production использования см. `queries/` директорию

