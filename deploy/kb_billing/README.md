# База знаний для схемы billing - Iridium M2M

База знаний для обучения агента генерации SQL запросов по схеме billing. Создана в формате проекта sql4A для использования с RAG (Retrieval-Augmented Generation) системой.

## Структура

```
kb_billing/
├── metadata.json                    # Метаданные схемы billing
├── tables/                          # Описания таблиц
│   ├── SPNET_TRAFFIC.json
│   ├── STECCOM_EXPENSES.json
│   ├── TARIFF_PLANS.json
│   └── IRIDIUM_SERVICES_INFO.json
├── views/                           # Описания представлений (VIEW)
│   ├── V_SPNET_OVERAGE_ANALYSIS.json
│   ├── V_CONSOLIDATED_OVERAGE_REPORT.json
│   ├── V_IRIDIUM_SERVICES_INFO.json
│   └── V_CONSOLIDATED_REPORT_WITH_BILLING.json
└── training_data/                   # Q/A примеры для обучения
    └── sql_examples.json
```

## Описание компонентов

### Таблицы

1. **SPNET_TRAFFIC** - Данные трафика SPNet (трафик, события, суммы)
2. **STECCOM_EXPENSES** - Расходы STECCOM (планы, fees, активации)
3. **TARIFF_PLANS** - Справочник тарифных планов с ценами превышения
4. **IRIDIUM_SERVICES_INFO** - Информация о сервисах и клиентах из биллинга

### Представления (VIEW)

1. **V_SPNET_OVERAGE_ANALYSIS** - Базовый анализ превышения трафика
2. **V_CONSOLIDATED_OVERAGE_REPORT** - Консолидированный отчет SPNet + STECCOM
3. **V_IRIDIUM_SERVICES_INFO** - Информация о сервисах с данными клиентов
4. **V_CONSOLIDATED_REPORT_WITH_BILLING** - Расширенный отчет с данными клиентов и fees

## Использование

### Загрузка в векторную БД (Qdrant)

Векторы хранятся в **Qdrant** (коллекция `kb_billing`). Загрузка через `kb_billing/rag/kb_loader.py`:

```python
from kb_billing.rag.kb_loader import KBLoader

loader = KBLoader()
loader.load_all(recreate=False)  # или recreate=True для полной перезаписи
```

Данные берутся из: `training_data/sql_examples.json`, `tables/*.json`, `views/*.json`, `metadata.json`, `confluence_docs/*.json`. См. docs/kb-billing-vs-presales.md.

### Формат JSON файлов

#### Таблицы и VIEW (tables/*.json, views/*.json)
```json
{
  "table_name": "SPNET_TRAFFIC",
  "schema": "billing",
  "description": "Описание таблицы...",
  "ddl": "CREATE TABLE ...",
  "key_columns": {
    "COLUMN_NAME": "Описание колонки"
  },
  "business_rules": ["Правило 1", "Правило 2"],
  "relationships": [...],
  "usage_notes": ["Примечание 1"]
}
```

#### Q/A примеры (training_data/sql_examples.json)
```json
[
  {
    "question": "Вопрос на естественном языке",
    "sql": "SELECT ...",
    "context": "Контекст запроса",
    "business_entity": "services",
    "complexity": 1,
    "category": "Сервисы"
  }
]
```

## Бизнес-логика

### Ключевые концепции

1. **BILL_MONTH** - Месяц биллинга в формате YYYYMM (например, 202510 для октября 2025)
2. **Превышение трафика (Overage)** - Рассчитывается как TOTAL_USAGE_KB - INCLUDED_KB
3. **Стоимость превышения** - Рассчитывается через функцию `calculate_overage(PLAN_NAME, USAGE_BYTES)` с учетом ступеней тарификации
4. **Fees** - Типы: Activation Fee, Advance Charge, Credit, Credited, Prorated
5. **Financial Period** - Отчетный период для финансистов (bill_month - 1 месяц)

### Важные правила

- **Периоды НЕ суммируются** - каждая строка в отчетах = отдельный период (BILL_MONTH)
- **Трафик vs События** - только USAGE_TYPE = 'SBD Data Usage' считается трафиком
- **Связи** - CONTRACT_ID связывается с SERVICES.LOGIN, IMEI связывается с SERVICES.VSAT
- **Активные сервисы** - STATUS = 10 AND CONTRACT_ID NOT LIKE '%-clone-%'

## Примеры запросов

См. файл `training_data/sql_examples.json` для полного списка примеров Q/A пар.

### Базовые запросы

- Поиск активных сервисов
- Поиск по IMEI или CONTRACT_ID
- Анализ превышения трафика за период
- Поиск сервисов клиента по коду 1С

### Аналитические запросы

- Агрегация превышения по организациям
- Статистика по тарифным планам
- Выявление сервисов с высоким превышением
- Расхождения в биллинге

### Финансовые алерты

- Сервисы без трафика
- Дубликаты IMEI
- Сервисы без кода 1С
- Расхождения между рассчитанным и выставленным превышением

## Обновление базы знаний

При добавлении новых таблиц, VIEW или запросов:

1. Создайте JSON файл с описанием в соответствующей директории
2. Добавьте метаданные в `metadata.json`
3. Добавьте Q/A примеры в `training_data/sql_examples.json`
4. Загрузите в векторную БД через API или CLI

## Связь с проектом sql4A

Эта база знаний создана в формате проекта sql4A (`/mnt/ai/cnn/sql4A`) и может быть загружена в векторную базу знаний через:

- API эндпоинты (`/training/ddl`, `/training/documentation`, `/training/question_sql`)
- CLI инструменты (`kb_training_client.py`)
- Загрузка через `KBLoader.load_all()` (Qdrant). См. docs/kb-billing-vs-presales.md

## Дополнительные ресурсы

- Исходные SQL файлы: `../oracle/` и `../postgresql/`
- Документация по отчетам: `../README.md`
- Примеры запросов: `../oracle/queries/`
















