#!/usr/bin/env python3
"""
Запуск диагностического запроса для выявления причин отсутствия тарифных планов
"""

import sys
import os
from pathlib import Path

# Добавляем корневую директорию в путь
root_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(root_dir))

try:
    from db_connection import get_postgres_config, get_db_connection
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError as e:
    print(f"Ошибка импорта: {e}")
    print("Установите зависимости: pip install psycopg2-binary")
    sys.exit(1)

def load_config_env():
    """Загрузка config.env"""
    config_file = root_dir / 'config.env'
    if config_file.exists():
        with open(config_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

def run_diagnostic_query():
    """Запуск диагностического запроса"""
    load_config_env()
    
    # Получаем конфигурацию
    config = get_postgres_config()
    
    # Читаем SQL файл
    sql_file = Path(__file__).parent / 'diagnose_missing_tariff_plans.sql'
    with open(sql_file, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    # Находим основной запрос (до первого ; после SELECT)
    # Ищем последний SELECT перед ORDER BY и берем все до следующего ;
    lines = sql_content.split('\n')
    main_query_lines = []
    in_main_query = False
    select_count = 0
    
    for line in lines:
        if 'SELECT' in line.upper() and '--' not in line.split('SELECT')[0]:
            select_count += 1
            if select_count == 1:  # Первый SELECT - начало основного запроса
                in_main_query = True
        
        if in_main_query:
            main_query_lines.append(line)
            if line.strip().endswith(';') and 'ORDER BY' in '\n'.join(main_query_lines).upper():
                break
    
    main_query = '\n'.join(main_query_lines).strip()
    
    # Если не нашли, берем все до первого ;
    if not main_query or not main_query.endswith(';'):
        # Берем все до первого ; который не в комментарии
        parts = sql_content.split(';')
        main_query = parts[0] + ';' if parts else sql_content
    
    # Подключаемся к БД
    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=" * 80)
        print("ДИАГНОСТИКА ОТСУТСТВУЮЩИХ ТАРИФНЫХ ПЛАНОВ")
        print("=" * 80)
        print()
        
        # Выполняем основной запрос
        if main_query:
            print("Выполняется основной диагностический запрос...")
            print("-" * 80)
            cur.execute(main_query)
            results = cur.fetchall()
            
            if results:
                # Выводим заголовки
                if results:
                    headers = list(results[0].keys())
                    # Форматируем вывод
                    col_widths = {}
                    for header in headers:
                        max_len = len(str(header))
                        for row in results:
                            val_len = len(str(row[header] or ''))
                            max_len = max(max_len, val_len)
                        col_widths[header] = max(max_len, 10)
                    
                    # Выводим заголовки
                    header_line = " | ".join([str(h).ljust(col_widths[h]) for h in headers])
                    print(header_line)
                    print("-" * len(header_line))
                    
                    # Выводим данные
                    for row in results:
                        data_line = " | ".join([
                            str(row[h] or '').ljust(col_widths[h])[:col_widths[h]] 
                            for h in headers
                        ])
                        print(data_line)
                    
                    print()
                    print(f"Всего записей: {len(results)}")
                    print()
                    
                    # Группируем по диагнозу
                    print("=" * 80)
                    print("СВОДКА ПО ДИАГНОЗАМ:")
                    print("=" * 80)
                    diagnoses = {}
                    for row in results:
                        diag = row.get('diagnosis', 'Неизвестно')
                        diagnoses[diag] = diagnoses.get(diag, 0) + 1
                    
                    for diag, count in sorted(diagnoses.items(), key=lambda x: -x[1]):
                        print(f"  {diag}: {count} записей")
            else:
                print("Нет результатов")
        
        cur.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"Ошибка подключения к базе данных: {e}")
        print(f"Конфигурация: host={config.get('host')}, db={config.get('dbname')}, user={config.get('user')}")
        sys.exit(1)
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    run_diagnostic_query()

