# Iridium M2M Overage Report - Summary

## ✅ Готово

### Приложение
- **URL:** http://localhost:8502
- **Статус:** Запущено
- **Тип:** Упрощенный отчет (без статистик, без редактора)

### Фокус
- **Только SBD-1 и SBD-10** (реальные цены из ТЗ)
- **Названия колонок:** на английском как в CSV
- **Данные:** только из загруженных CSV файлов
- **Конвертация:** 1 KB = 1000 bytes

## 📊 Данные

### Текущая статистика
- **Записей:** 4,225 (для SBD-10)
- **Устройств (IMEI):** 1,213
- **Превышение:** $26,529.07

### Тарифы

#### SBD-1 (SBD Tiered 1250 1K)
```
Включено: 1 KB
  1-10 KB:  $1.50/KB
 10-25 KB:  $0.75/KB
    25+ KB:  $0.50/KB
```

#### SBD-10 (SBD Tiered 1250 10K)
```
Включено: 10 KB
 10-25 KB:  $0.30/KB
 25-50 KB:  $0.20/KB
    50+ KB:  $0.10/KB
```

## 🔍 Фильтры

- **Period** - выбор месяца (All Periods / 2025-09 / 2025-08 / ...)
- **Plan** - выбор тарифа (All Plans / SBD-1 / SBD-10)

## 📥 Экспорт

- **CSV** - текстовый формат
- **Excel** - форматированный XLSX (требует openpyxl)

## 📋 Колонки отчета

1. **IMEI** - идентификатор устройства
2. **Contract ID** - номер контракта
3. **Plan Name** - название тарифа
4. **Bill Month** - период (YYYY-MM)
5. **Usage (KB)** - использовано
6. **Included (KB)** - включено в тариф
7. **Overage (KB)** - превышение
8. **Calculated Overage ($)** - расчетная стоимость превышения
9. **SPNet Total Amount ($)** - сумма из SPNet
10. **Advance Charge ($)** - абонплата
11. **STECCOM Total Amount ($)** - общая сумма STECCOM
12. **Overage Diff ($)** - разница (Calculated - SPNet)
13. **Total Diff ($)** - разница (Calculated + Advance - STECCOM)

## 🗄️ База данных

**PostgreSQL (тестирование)**
```
Host:     localhost
Port:     5432
Database: billing
User:     postgres
Password: 1234
```

## 🚀 Команды

### Запуск приложения
```bash
cd /mnt/ai/cnn/ai_report
streamlit run streamlit_report.py --server.port 8502
```

### Остановка
```bash
pkill -f streamlit
```

### Перезагрузка данных
```bash
python3 load_data_postgres.py
```

### Проверка данных
```bash
PGPASSWORD=1234 psql -h localhost -p 5432 -U postgres -d billing -c "
SELECT COUNT(*), PLAN_NAME FROM V_CONSOLIDATED_OVERAGE_REPORT 
WHERE PLAN_NAME IN ('SBD Tiered 1250 1K', 'SBD Tiered 1250 10K')
GROUP BY PLAN_NAME;
"
```

## 📁 Файлы

### Основные
- `streamlit_report.py` - web-приложение
- `setup_postgres.sql` - схема БД PostgreSQL
- `load_data_postgres.py` - загрузка данных

### Документация
- `README.md` - общая документация
- `TZ.md` - техническое задание
- `CHANGELOG.md` - история изменений
- `SUMMARY.md` - этот файл

### Для Oracle (продакшен)
- `oracle_tables.sql` - схема Oracle
- `create_tariff_plans.sql` - тарифы Oracle
- `load_spnet_traffic.py` - загрузка SPNet в Oracle
- `load_steccom_expenses.py` - загрузка STECCOM в Oracle
- `streamlit_report_oracle.py` - приложение для Oracle

## ⚠️ Важно

1. **Только SBD-1 и SBD-10** - другие тарифы не рассчитываются
2. **1 KB = 1000 bytes** - десятичная система (не 1024)
3. **Названия на английском** - соответствуют CSV
4. **Никаких выдуманных данных** - только из CSV файлов

## 📧 Контакты

Для вопросов по данным или отчету обращайтесь к администратору системы.


