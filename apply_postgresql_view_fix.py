#!/usr/bin/env python3
"""
Скрипт для применения исправлений представлений в PostgreSQL
Применяет обновленные представления в правильном порядке
"""
import os
import sys
from pathlib import Path

# Загружаем конфигурацию из config.env
config_file = Path(__file__).parent / 'config.env'
if config_file.exists():
    with open(config_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ[key.strip()] = value.strip()

try:
    import psycopg2
    from psycopg2 import sql
except ImportError:
    print("ERROR: psycopg2 not installed. Install with: pip install psycopg2-binary")
    sys.exit(1)

# Параметры подключения к PostgreSQL
postgresql_config = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'database': os.getenv('POSTGRES_DB', 'billing'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
}

def _remove_comments(sql_text):
    """Удаляет комментарии из SQL текста, сохраняя COMMENT ON команды"""
    lines = []
    for line in sql_text.split('\n'):
        stripped = line.strip()
        # Пропускаем пустые строки
        if not stripped:
            lines.append('')
            continue
        # Пропускаем однострочные комментарии (но не COMMENT ON)
        if stripped.startswith('--'):
            if stripped.upper().startswith('COMMENT'):
                # COMMENT ON - это SQL команда, оставляем
                lines.append(line)
            # Иначе это комментарий, пропускаем
            continue
        # Удаляем комментарии в конце строки (все после --)
        if '--' in line:
            # Проверяем, не является ли это частью строки в кавычках
            parts = line.split('--')
            if len(parts) > 1:
                # Берем только часть до комментария
                # Но нужно проверить, не в кавычках ли это
                before_comment = parts[0]
                # Простая проверка: если четное количество кавычек до --, то это комментарий
                quote_count = before_comment.count("'") - before_comment.count("''")
                if quote_count % 2 == 0:
                    # Это комментарий, удаляем
                    line = before_comment.rstrip()
        lines.append(line)
    return '\n'.join(lines)


def apply_view_fix():
    """Применение исправлений к представлениям PostgreSQL"""
    
    # Подключение к PostgreSQL
    try:
        conn = psycopg2.connect(
            host=postgresql_config['host'],
            port=postgresql_config['port'],
            database=postgresql_config['database'],
            user=postgresql_config['user'],
            password=postgresql_config['password']
        )
        print(f"✅ Подключение к PostgreSQL успешно: {postgresql_config['user']}@{postgresql_config['host']}:{postgresql_config['port']}/{postgresql_config['database']}")
        
        # Читаем SQL файлы в правильном порядке
        sql_files = [
            Path(__file__).parent / 'postgresql' / 'views' / '01_v_spnet_overage_analysis.sql',
            Path(__file__).parent / 'postgresql' / 'views' / '02_v_consolidated_overage_report.sql',
            Path(__file__).parent / 'postgresql' / 'views' / '03_v_iridium_services_info.sql',
            Path(__file__).parent / 'postgresql' / 'views' / '04_v_consolidated_report_with_billing.sql',
            Path(__file__).parent / 'postgresql' / 'views' / '05_v_steccom_access_fees_pivot.sql'
        ]
        
        for sql_file in sql_files:
            if not sql_file.exists():
                print(f"⚠️  Файл не найден: {sql_file} (пропускаем)")
                continue
        
        cursor = conn.cursor()
        
        all_commands = []
        
        # Обрабатываем каждый SQL файл
        for sql_file in sql_files:
            if not sql_file.exists():
                continue
                
            print(f"\n📄 Обработка файла: {sql_file.name}")
            
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # Удаляем команды psql (\echo, \i) и оставляем только SQL
            sql_lines = []
            for line in sql_content.split('\n'):
                stripped = line.strip()
                # Пропускаем команды psql
                if stripped.startswith('\\echo') or stripped.startswith('\\i'):
                    continue
                # Сохраняем все остальные строки
                sql_lines.append(line)
            
            # Объединяем SQL команды
            sql_script = '\n'.join(sql_lines)
            
            # Разделяем на отдельные команды по ';'
            # В PostgreSQL команды разделяются ';'
            commands = []
            current_command = []
            
            for line in sql_script.split('\n'):
                stripped = line.strip()
                # Если строка заканчивается на ';', это конец команды
                if stripped.endswith(';'):
                    current_command.append(line)
                    cmd = '\n'.join(current_command).strip()
                    # Удаляем комментарии из команды
                    cmd_clean = _remove_comments(cmd)
                    if cmd_clean:
                        commands.append(cmd_clean)
                    current_command = []
                else:
                    # Добавляем строку к текущей команде
                    current_command.append(line)
            
            # Добавляем последнюю команду, если она есть (без ';')
            if current_command:
                cmd = '\n'.join(current_command).strip()
                cmd_clean = _remove_comments(cmd)
                if cmd_clean:
                    commands.append(cmd_clean)
            
            # Добавляем команды из этого файла
            for cmd in commands:
                all_commands.append((sql_file.name, cmd))
        
        print("\n" + "="*80)
        print("Применение исправлений к представлениям PostgreSQL")
        print("="*80)
        print(f"Всего команд: {len(all_commands)}\n")
        
        success_count = 0
        error_count = 0
        
        for i, (file_name, cmd) in enumerate(all_commands, 1):
            if not cmd.strip():
                continue
            try:
                print(f"[{i}/{len(all_commands)}] {file_name}...")
                # Команда уже очищена от комментариев выше
                cursor.execute(cmd)
                conn.commit()
                success_count += 1
                print(f"  ✅ Успешно")
            except Exception as e:
                error_count += 1
                print(f"  ❌ Ошибка: {e}")
                # Показываем первые 300 символов команды для отладки
                cmd_preview = cmd[:300].replace('\n', ' ')
                print(f"     Команда: {cmd_preview}...")
                conn.rollback()
                # Продолжаем выполнение остальных команд
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*80)
        print(f"✅ Успешно выполнено команд: {success_count}")
        if error_count > 0:
            print(f"⚠️  Ошибок: {error_count}")
        print("="*80)
        
        return error_count == 0
        
    except Exception as e:
        print(f"❌ Ошибка подключения или выполнения: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = apply_view_fix()
    sys.exit(0 if success else 1)

