# Инструкция по обновлению V_CONSOLIDATED_OVERAGE_REPORT в Oracle

## Шаг 1: Синхронизация файлов на сервер

### Вариант A: Используя скрипт синхронизации (рекомендуется)

```bash
# Из локальной директории проекта
./sync_to_vz2.sh
```

### Вариант B: Ручная синхронизация через rsync

```bash
rsync -avz --progress \
    -e "ssh -p 1194" \
    oracle/views/02_v_consolidated_overage_report.sql \
    root@82.114.2.2:/usr/local/projects/ai_report/oracle/views/
```

## Шаг 2: Подключение к Oracle серверу

```bash
# Подключитесь к серверу
ssh -p 1194 root@82.114.2.2

# Перейдите в директорию проекта
cd /usr/local/projects/ai_report
```

## Шаг 3: Применение обновления представления

### Вариант A: Обновить только одно представление (быстро)

```bash
# Подключитесь к Oracle и выполните обновление
sqlplus billing7/billing@bm7 @oracle/views/02_v_consolidated_overage_report.sql
```

### Вариант B: Обновить все представления (если нужно)

```bash
# Если нужно пересоздать все представления в правильном порядке
cd oracle/views
sqlplus billing7/billing@bm7 @install_all_views.sql
```

## Шаг 4: Проверка обновления

После применения обновления проверьте, что представление работает корректно:

```sql
-- Подключитесь к Oracle
sqlplus billing7/billing@bm7

-- Проверьте, что записи за октябрь 2025 теперь имеют план
SELECT 
    imei, 
    contract_id, 
    bill_month, 
    plan_name 
FROM V_CONSOLIDATED_OVERAGE_REPORT 
WHERE bill_month = '202510' 
  AND contract_id IN ('SUB-51636947303', 'SUB-51637724351', 'SUB-37896123618')
ORDER BY contract_id;

-- Должны быть планы: LBS 1 или LBS Demo (не NULL)
```

## Шаг 5: Проверка количества записей без плана

```sql
-- Проверьте, сколько записей осталось без плана за октябрь 2025
SELECT COUNT(*) 
FROM V_CONSOLIDATED_OVERAGE_REPORT 
WHERE bill_month = '202510' 
  AND plan_name IS NULL;

-- Должно быть 0 (или очень мало, если есть записи без данных вообще)
```

## Быстрая команда (все в одной строке)

Если у вас настроен доступ к Oracle через переменные окружения:

```bash
# Синхронизация
./sync_to_vz2.sh

# Применение обновления (с удаленного сервера)
ssh -p 1194 root@82.114.2.2 "cd /usr/local/projects/ai_report && sqlplus billing7/billing@bm7 @oracle/views/02_v_consolidated_overage_report.sql"
```

## Что изменилось

- ✅ Добавлен автоматический маппинг тарифных планов из других периодов
- ✅ Если PLAN_NAME = NULL в текущем периоде, система берет план из других периодов
- ✅ Приоритет: CONTRACT_ID → IMEI
- ✅ Полная обратная совместимость

## Откат изменений (если нужно)

Если нужно откатить изменения, используйте предыдущую версию файла из git:

```bash
# На сервере
cd /usr/local/projects/ai_report
git checkout HEAD~1 oracle/views/02_v_consolidated_overage_report.sql
sqlplus billing7/billing@bm7 @oracle/views/02_v_consolidated_overage_report.sql
```

## Устранение проблем

### Ошибка: "ORA-00942: table or view does not exist"
- Убедитесь, что таблицы SPNET_TRAFFIC и STECCOM_EXPENSES существуют
- Проверьте, что вы подключены к правильной схеме (billing7)

### Ошибка: "ORA-01031: insufficient privileges"
- Убедитесь, что у пользователя billing7 есть права на создание представлений
- Может потребоваться: `GRANT CREATE VIEW TO billing7;`

### Представление не обновляется
- Убедитесь, что файл синхронизирован: `ls -la oracle/views/02_v_consolidated_overage_report.sql`
- Проверьте синтаксис: `sqlplus billing7/billing@bm7 @oracle/views/02_v_consolidated_overage_report.sql` (должно выполниться без ошибок)

