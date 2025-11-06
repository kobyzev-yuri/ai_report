#!/usr/bin/env python3
"""
Импорт V_IRIDIUM_SERVICES_INFO.csv в PostgreSQL
Обрабатывает ошибки и пропускает битые строки
"""

import sys
import os
import psycopg2
from pathlib import Path

# Загружаем конфигурацию
script_dir = Path(__file__).parent
root_dir = script_dir.parent.parent
config_file = root_dir / 'config.env'

if config_file.exists():
    with open(config_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip().strip('"\'')
                if key.startswith('POSTGRES_'):
                    os.environ[key] = value

# Параметры подключения
# Для TRUNCATE нужны права владельца, используем postgres если доступен
PG_HOST = os.getenv('POSTGRES_HOST', 'localhost')
PG_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
PG_DB = os.getenv('POSTGRES_DB', 'billing')
# Пробуем использовать postgres, если не указан другой пользователь
PG_USER = os.getenv('PGUSER') or os.getenv('POSTGRES_USER', 'postgres')
PG_PASSWORD = os.getenv('PGPASSWORD') or os.getenv('POSTGRES_PASSWORD', '')

# Путь к файлу
if len(sys.argv) > 1:
    CSV_FILE = sys.argv[1]
else:
    CSV_FILE = root_dir / 'oracle' / 'test' / 'V_IRIDIUM_SERVICES_INFO.csv'

if not os.path.exists(CSV_FILE):
    print(f"ERROR: Файл не найден: {CSV_FILE}")
    sys.exit(1)

print("=" * 80)
print("Импорт V_IRIDIUM_SERVICES_INFO.csv в PostgreSQL")
print("=" * 80)
print(f"\nФайл: {CSV_FILE}")
print(f"База данных: {PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DB}\n")

# Подключение к БД
try:
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cur = conn.cursor()
    
    # Проверяем и добавляем столбец is_suspended
    print("Проверка столбца is_suspended...")
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'iridium_services_info' 
                AND column_name = 'is_suspended'
            ) THEN
                ALTER TABLE iridium_services_info ADD COLUMN is_suspended VARCHAR(1);
                COMMENT ON COLUMN iridium_services_info.is_suspended IS 'Признак приостановки: Y=есть активная услуга приостановления (TYPE_ID=9008), N=нет';
            END IF;
        END $$;
    """)
    conn.commit()
    
    # Очистка таблицы
    print("Очистка таблицы...")
    cur.execute("TRUNCATE TABLE iridium_services_info;")
    conn.commit()
    
    # Импорт данных
    print("Импорт данных...")
    expected_cols = 18
    inserted = 0
    skipped = 0
    errors = []
    
    with open(CSV_FILE, 'r', encoding='utf-8', errors='ignore') as f:
        for line_num, line in enumerate(f, 1):
            line = line.rstrip('\n\r')
            if not line:
                continue
            
            parts = line.split('|')
            if len(parts) != expected_cols:
                skipped += 1
                if skipped <= 10:  # Показываем первые 10 пропущенных строк
                    errors.append(f"Строка {line_num}: {len(parts)} колонок вместо {expected_cols}")
                continue
            
            try:
                # Обрабатываем NULL значения и обрезаем длинные строки
                values = []
                # Максимальные длины полей (из структуры таблицы)
                max_lengths = [None, 50, 50, None, 200, 100, None, None, None, 500, 500, 500, None, None, None, None, 1, 100]
                for i, part in enumerate(parts):
                    part = part.strip()
                    if part == '':
                        values.append(None)
                    else:
                        # Обрезаем если превышает максимальную длину
                        max_len = max_lengths[i]
                        if max_len and len(part) > max_len:
                            part = part[:max_len]
                            if skipped < 5:  # Показываем первые обрезанные значения
                                errors.append(f"Строка {line_num}, колонка {i+1}: обрезано до {max_len} символов")
                        values.append(part)
                
                # Вставляем данные
                cur.execute("""
                    INSERT INTO iridium_services_info (
                        service_id, contract_id, imei, tariff_id, agreement_number,
                        order_number, status, actual_status, customer_id,
                        organization_name, person_name, customer_name,
                        create_date, start_date, stop_date, account_id,
                        is_suspended, code_1c
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, values)
                inserted += 1
                
                # Коммитим каждые 1000 строк
                if inserted % 1000 == 0:
                    conn.commit()
                    print(f"  Импортировано: {inserted} строк...")
                    
            except Exception as e:
                # Откатываем транзакцию при ошибке и продолжаем
                conn.rollback()
                skipped += 1
                if skipped <= 10:
                    errors.append(f"Строка {line_num}: {str(e)}")
                continue
    
    # Финальный коммит
    conn.commit()
    
    print(f"\nИмпорт завершен:")
    print(f"  Вставлено: {inserted} строк")
    print(f"  Пропущено: {skipped} строк")
    
    if errors:
        print(f"\nПервые ошибки:")
        for err in errors[:10]:
            print(f"  {err}")
    
    # Статистика
    print("\n" + "=" * 80)
    print("Статистика импорта:")
    print("=" * 80)
    cur.execute("""
        SELECT 
            COUNT(*) AS total_records,
            COUNT(DISTINCT service_id) AS unique_services,
            COUNT(*) FILTER (WHERE contract_id IS NOT NULL) AS with_contract_id,
            COUNT(*) FILTER (WHERE imei IS NOT NULL) AS with_imei,
            COUNT(*) FILTER (WHERE code_1c IS NOT NULL) AS with_code_1c,
            COUNT(*) FILTER (WHERE status = 10) AS active_status,
            COUNT(*) FILTER (WHERE is_suspended = 'Y') AS suspended
        FROM iridium_services_info;
    """)
    stats = cur.fetchone()
    print(f"\nВсего записей: {stats[0]}")
    print(f"Уникальных сервисов: {stats[1]}")
    print(f"С contract_id: {stats[2]}")
    print(f"С IMEI: {stats[3]}")
    print(f"С code_1c: {stats[4]}")
    print(f"Активных (status=10): {stats[5]}")
    print(f"Приостановленных: {stats[6]}")
    
    print("\nПримеры записей:")
    cur.execute("""
        SELECT service_id, contract_id, LEFT(imei, 20) AS imei, 
               tariff_id, status, is_suspended, code_1c
        FROM iridium_services_info 
        LIMIT 5;
    """)
    for row in cur.fetchall():
        print(f"  {row}")
    
    cur.close()
    conn.close()
    
    print("\n" + "=" * 80)
    print("✓ Импорт завершен успешно!")
    print("=" * 80)
    
except Exception as e:
    print(f"\nERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

