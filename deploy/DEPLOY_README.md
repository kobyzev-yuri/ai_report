# Минимальный деплой ai_report

Это минимальная версия проекта для деплоя на сервер.

## Быстрая установка

1. **Создайте config.env** (скопируйте из config.env.example и заполните):
```bash
cp config.env.example config.env
nano config.env  # Заполните реальные значения Oracle
```

2. **Установите зависимости**:
```bash
pip install -r requirements.txt
```

2.1. **Системные утилиты (рекомендуется):** для вкладки «Счета 1С» / распаковки **.7z** на сервере нужна команда `7z` (Debian/Ubuntu: `apt install -y p7zip-full`). Для RAR — `unrar` или `unar`.

3. **Установите Oracle структуру** (если еще не установлена):
```bash
cd oracle/tables
sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @install_all_tables.sql

cd ../data
sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @tariff_plans_data.sql

cd ../functions
sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @calculate_overage.sql

cd ../views
sqlplus $ORACLE_USER/$ORACLE_PASSWORD@$ORACLE_SERVICE @install_all_views.sql
```

4. **Запустите приложение**:
```bash
./run_streamlit_background.sh
```

## Что включено

- ✅ Oracle скрипты (tables, views, functions, data)
- ✅ Python приложение (Streamlit + загрузчики данных)
- ✅ База знаний (KB) для AI агента
- ✅ Скрипты управления (запуск/остановка/статус)
- ✅ Тесты (tests/) — запуск на сервере: `python -m tests.test_billing_assistant_top5`
- ✅ Минимальная документация

## Что НЕ включено

- ❌ Архивы (archive/)
- ❌ Временные файлы (__pycache__/, *.pyc)
- ❌ Данные (data/ - загружаются через интерфейс)
- ❌ Oracle тестовые/отладочные скрипты (testing/, test/, queries/)

## Размер деплоя

Проверьте размер:
```bash
du -sh deploy/
```

Ожидаемый размер: ~2-5 MB (без данных)
