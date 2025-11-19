#!/usr/bin/env python3
"""
Скрипт для применения исправлений представления V_CONSOLIDATED_REPORT_WITH_BILLING в Oracle
Исправляет проблему с дублированием fees (абонплат)
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
    import oracledb as cx_Oracle
except ImportError:
    try:
        import cx_Oracle
    except ImportError:
        print("ERROR: oracledb or cx_Oracle not installed. Install with: pip install oracledb")
    sys.exit(1)

# Параметры подключения к Oracle
# ВАЖНО: Используйте переменные окружения или config.env для реальных значений
oracle_config = {
    'username': os.getenv('ORACLE_USER', 'your_user'),
    'password': os.getenv('ORACLE_PASSWORD', 'your_password'),
    'host': os.getenv('ORACLE_HOST', 'localhost'),
    'port': int(os.getenv('ORACLE_PORT', '1521')),
    'sid': os.getenv('ORACLE_SID'),
    'service_name': os.getenv('ORACLE_SERVICE', 'your_service')
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
    """Применение исправлений к представлению"""
    
    # Подключение к Oracle
    try:
        if oracle_config.get('sid'):
            dsn = cx_Oracle.makedsn(
                oracle_config['host'],
                oracle_config['port'],
                sid=oracle_config['sid']
            )
        else:
            dsn = cx_Oracle.makedsn(
                oracle_config['host'],
                oracle_config['port'],
                service_name=oracle_config['service_name']
            )
        
        conn = cx_Oracle.connect(
            user=oracle_config['username'],
            password=oracle_config['password'],
            dsn=dsn
        )
        print(f"✅ Подключение к Oracle успешно: {oracle_config['username']}@{oracle_config['host']}:{oracle_config['port']}")
        
        # Читаем SQL файлы - сначала базовое представление, потом с биллингом
        sql_files = [
            Path(__file__).parent / 'oracle' / 'views' / '01_v_spnet_overage_analysis.sql',
            Path(__file__).parent / 'oracle' / 'views' / '02_v_consolidated_overage_report.sql',
            Path(__file__).parent / 'oracle' / 'views' / '04_v_consolidated_report_with_billing.sql'
        ]
        
        for sql_file in sql_files:
            if not sql_file.exists():
                print(f"ERROR: Файл не найден: {sql_file}")
                return False
        
        cursor = conn.cursor()
        
        all_commands = []
        
        # Обрабатываем каждый SQL файл
        for sql_file in sql_files:
            print(f"\n📄 Обработка файла: {sql_file.name}")
            
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # Удаляем команды psql (\echo) и SET команды, оставляем только SQL
            # НО сохраняем разделитель '/' для правильного разделения команд
            sql_lines = []
            for line in sql_content.split('\n'):
                stripped = line.strip()
                # Пропускаем команды psql
                if stripped.startswith('\\echo'):
                    continue
                # Пропускаем все SET команды (SET DEFINE OFF, SET SQLBLANKLINES ON - это команды SQL*Plus, не SQL)
                if stripped.startswith('SET '):
                    continue
                # Сохраняем все остальные строки, включая разделитель '/'
                sql_lines.append(line)
            
            # Объединяем SQL команды
            sql_script = '\n'.join(sql_lines)
            
            # Разделяем на отдельные команды по '/'
            # В Oracle SQL команды разделяются '/' на отдельной строке
            lines = sql_script.split('\n')
            commands = []
            current_command = []
            
            for line in lines:
                stripped = line.strip()
                # Если строка содержит только '/', это разделитель команды
                if stripped == '/':
                    if current_command:
                        cmd = '\n'.join(current_command).strip()
                        # Удаляем комментарии из команды
                        cmd_clean = _remove_comments(cmd)
                        if cmd_clean:
                            commands.append(cmd_clean)
                    current_command = []
                else:
                    # Добавляем строку к текущей команде (даже пустые строки для сохранения структуры)
                    current_command.append(line)
            
            # Добавляем последнюю команду, если она есть (без разделителя '/')
            if current_command:
                cmd = '\n'.join(current_command).strip()
                cmd_clean = _remove_comments(cmd)
                if cmd_clean:
                    commands.append(cmd_clean)
            
            # Добавляем команды из этого файла
            for cmd in commands:
                all_commands.append((sql_file.name, cmd))
        
        print("\n" + "="*80)
        print("Применение исправлений к представлениям Oracle")
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

