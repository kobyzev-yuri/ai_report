#!/usr/bin/env python3
"""
Скрипт для восстановления истории загрузок в load_logs
на основе данных, уже загруженных в таблицы spnet_traffic и steccom_expenses

Использование:
    python restore_load_history.py [--db-type postgresql|oracle] [--dry-run]
"""

import psycopg2
import sys
import os
import argparse
from pathlib import Path
from datetime import datetime
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config_env():
    """Загрузка config.env если переменные окружения не установлены"""
    config_file = Path(__file__).parent.parent / 'config.env'
    if config_file.exists():
        logger.info(f"Загрузка конфигурации из {config_file}")
        with open(config_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip().strip('"\'')
                    # Загружаем только если переменная еще не установлена
                    if key.startswith(('POSTGRES_', 'ORACLE_', 'PG')) and not os.getenv(key):
                        os.environ[key] = value
                        logger.debug(f"Загружено: {key}")


# Загружаем config.env при импорте модуля
load_config_env()


def get_db_config(db_type='postgresql'):
    """Получение конфигурации БД из переменных окружения"""
    if db_type == 'postgresql':
        return {
            'dbname': os.getenv('POSTGRES_DB', 'billing'),
            'user': os.getenv('POSTGRES_USER', 'cnn'),
            'password': os.getenv('POSTGRES_PASSWORD', ''),
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', '5432'))
        }
    else:
        # Oracle конфигурация
        # Поддержка как ORACLE_SID, так и ORACLE_SERVICE
        oracle_sid = os.getenv('ORACLE_SID')
        oracle_service = os.getenv('ORACLE_SERVICE')
        return {
            'username': os.getenv('ORACLE_USER', 'billing7'),
            'password': os.getenv('ORACLE_PASSWORD', ''),
            'host': os.getenv('ORACLE_HOST', ''),
            'port': int(os.getenv('ORACLE_PORT', '1521')),
            'sid': oracle_sid,
            'service_name': oracle_service or (oracle_sid if oracle_sid else 'bm7')
        }


def restore_postgresql_history(dry_run=False):
    """Восстановление истории загрузок для PostgreSQL"""
    config = get_db_config('postgresql')
    
    try:
        conn = psycopg2.connect(**config)
        conn.autocommit = False
        cursor = conn.cursor()
        
        logger.info("="*80)
        logger.info("ВОССТАНОВЛЕНИЕ ИСТОРИИ ЗАГРУЗОК (PostgreSQL)")
        logger.info("="*80)
        
        # 1. Восстановление истории для SPNet
        logger.info("\n📊 Восстановление истории SPNet Traffic...")
        cursor.execute("""
            SELECT 
                source_file,
                COUNT(*) as records_count,
                MIN(load_date) as first_load_date,
                MAX(load_date) as last_load_date
            FROM spnet_traffic
            WHERE source_file IS NOT NULL
            GROUP BY source_file
            ORDER BY source_file
        """)
        
        spnet_files = cursor.fetchall()
        logger.info(f"Найдено файлов SPNet: {len(spnet_files)}")
        
        spnet_inserted = 0
        for file_name, records_count, first_load, last_load in spnet_files:
            # Проверяем, есть ли уже запись
            cursor.execute("""
                SELECT COUNT(*) FROM load_logs 
                WHERE LOWER(source_file) = LOWER(%s) 
                AND LOWER(table_name) = 'spnet_traffic'
                AND load_status = 'SUCCESS'
            """, (file_name,))
            
            exists = cursor.fetchone()[0] > 0
            
            if exists:
                logger.info(f"  ⏭ Пропускаем (уже есть): {file_name}")
                continue
            
            if not dry_run:
                cursor.execute("""
                    INSERT INTO load_logs (
                        table_name, source_file, records_loaded, load_status,
                        load_start_time, load_end_time, created_by
                    ) VALUES (
                        'spnet_traffic', %s, %s, 'SUCCESS',
                        %s, %s, 'RESTORE_SCRIPT'
                    )
                """, (file_name, records_count, first_load or datetime.now(), last_load or datetime.now()))
                spnet_inserted += 1
                logger.info(f"  ✓ Добавлено: {file_name} ({records_count:,} записей)")
            else:
                logger.info(f"  [DRY-RUN] Добавили бы: {file_name} ({records_count:,} записей)")
                spnet_inserted += 1
        
        # 2. Восстановление истории для STECCOM
        logger.info("\n💰 Восстановление истории STECCOM Expenses...")
        cursor.execute("""
            SELECT 
                source_file,
                COUNT(*) as records_count,
                MIN(load_date) as first_load_date,
                MAX(load_date) as last_load_date
            FROM steccom_expenses
            WHERE source_file IS NOT NULL
            GROUP BY source_file
            ORDER BY source_file
        """)
        
        steccom_files = cursor.fetchall()
        logger.info(f"Найдено файлов STECCOM: {len(steccom_files)}")
        
        steccom_inserted = 0
        for file_name, records_count, first_load, last_load in steccom_files:
            # Проверяем, есть ли уже запись
            cursor.execute("""
                SELECT COUNT(*) FROM load_logs 
                WHERE LOWER(source_file) = LOWER(%s) 
                AND LOWER(table_name) = 'steccom_expenses'
                AND load_status = 'SUCCESS'
            """, (file_name,))
            
            exists = cursor.fetchone()[0] > 0
            
            if exists:
                logger.info(f"  ⏭ Пропускаем (уже есть): {file_name}")
                continue
            
            if not dry_run:
                cursor.execute("""
                    INSERT INTO load_logs (
                        table_name, source_file, records_loaded, load_status,
                        load_start_time, load_end_time, created_by
                    ) VALUES (
                        'steccom_expenses', %s, %s, 'SUCCESS',
                        %s, %s, 'RESTORE_SCRIPT'
                    )
                """, (file_name, records_count, first_load or datetime.now(), last_load or datetime.now()))
                steccom_inserted += 1
                logger.info(f"  ✓ Добавлено: {file_name} ({records_count:,} записей)")
            else:
                logger.info(f"  [DRY-RUN] Добавили бы: {file_name} ({records_count:,} записей)")
                steccom_inserted += 1
        
        if not dry_run:
            conn.commit()
            logger.info(f"\n✅ Успешно восстановлено:")
            logger.info(f"   SPNet: {spnet_inserted} записей")
            logger.info(f"   STECCOM: {steccom_inserted} записей")
        else:
            logger.info(f"\n[DRY-RUN] Было бы восстановлено:")
            logger.info(f"   SPNet: {spnet_inserted} записей")
            logger.info(f"   STECCOM: {steccom_inserted} записей")
        
        # Статистика по load_logs
        cursor.execute("SELECT COUNT(*) FROM load_logs WHERE load_status = 'SUCCESS'")
        total_logs = cursor.fetchone()[0]
        logger.info(f"\n📋 Всего записей в load_logs: {total_logs}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False


def restore_oracle_history(dry_run=False):
    """Восстановление истории загрузок для Oracle"""
    config = get_db_config('oracle')
    
    try:
        import cx_Oracle
        
        # Используем тот же подход, что и в streamlit_report_oracle_backup.py
        # Проверяем переменную окружения напрямую
        oracle_sid = os.getenv('ORACLE_SID')
        oracle_service = os.getenv('ORACLE_SERVICE') or (oracle_sid if oracle_sid else 'bm7')
        
        # Логируем параметры подключения (без пароля)
        logger.info(f"Подключение к Oracle: {config['username']}@{config['host']}:{config['port']}")
        logger.info(f"ORACLE_SID: {oracle_sid}, ORACLE_SERVICE: {oracle_service}")
        
        # Используем SID если задан ORACLE_SID, иначе SERVICE_NAME (как в приложении)
        if oracle_sid:
            dsn = cx_Oracle.makedsn(
                config['host'],
                config['port'],
                sid=oracle_sid
            )
        else:
            dsn = cx_Oracle.makedsn(
                config['host'],
                config['port'],
                service_name=oracle_service
            )
        
        conn = cx_Oracle.connect(
            user=config['username'],
            password=config['password'],
            dsn=dsn
        )
        cursor = conn.cursor()
        
        logger.info("="*80)
        logger.info("ВОССТАНОВЛЕНИЕ ИСТОРИИ ЗАГРУЗОК (Oracle)")
        logger.info("="*80)
        
        # 1. Восстановление истории для SPNet
        logger.info("\n📊 Восстановление истории SPNet Traffic...")
        cursor.execute("""
            SELECT 
                SOURCE_FILE,
                COUNT(*) as records_count,
                MIN(LOAD_DATE) as first_load_date,
                MAX(LOAD_DATE) as last_load_date
            FROM SPNET_TRAFFIC
            WHERE SOURCE_FILE IS NOT NULL
            GROUP BY SOURCE_FILE
            ORDER BY SOURCE_FILE
        """)
        
        spnet_files = cursor.fetchall()
        logger.info(f"Найдено файлов SPNet: {len(spnet_files)}")
        
        # Определяем структуру таблицы LOAD_LOGS
        # Проверяем FILE_NAME vs SOURCE_FILE
        try:
            test_query = "SELECT FILE_NAME FROM LOAD_LOGS WHERE ROWNUM = 1"
            cursor.execute(test_query)
            file_col = "FILE_NAME"
        except:
            try:
                test_query = "SELECT SOURCE_FILE FROM LOAD_LOGS WHERE ROWNUM = 1"
                cursor.execute(test_query)
                file_col = "SOURCE_FILE"
            except:
                logger.warning("Не удалось определить структуру LOAD_LOGS, используем FILE_NAME по умолчанию")
                file_col = "FILE_NAME"
        
        # Проверяем LOADED_BY vs CREATED_BY
        try:
            test_query = "SELECT LOADED_BY FROM LOAD_LOGS WHERE ROWNUM = 1"
            cursor.execute(test_query)
            loaded_by_col = "LOADED_BY"
        except:
            try:
                test_query = "SELECT CREATED_BY FROM LOAD_LOGS WHERE ROWNUM = 1"
                cursor.execute(test_query)
                loaded_by_col = "CREATED_BY"
            except:
                logger.warning("Не удалось определить столбец для created_by, используем LOADED_BY по умолчанию")
                loaded_by_col = "LOADED_BY"
        
        spnet_inserted = 0
        for file_name, records_count, first_load, last_load in spnet_files:
            # Проверяем, есть ли уже запись
            cursor.execute(f"""
                SELECT COUNT(*) FROM LOAD_LOGS 
                WHERE UPPER({file_col}) = UPPER(:1) 
                AND UPPER(TABLE_NAME) = 'SPNET_TRAFFIC'
                AND LOAD_STATUS = 'SUCCESS'
            """, (file_name,))
            
            exists = cursor.fetchone()[0] > 0
            
            if exists:
                logger.info(f"  ⏭ Пропускаем (уже есть): {file_name}")
                continue
            
            if not dry_run:
                # Используем определенные столбцы
                cursor.execute(f"""
                    INSERT INTO LOAD_LOGS (
                        TABLE_NAME, {file_col}, RECORDS_LOADED, LOAD_STATUS,
                        LOAD_START_TIME, LOAD_END_TIME, {loaded_by_col}
                    ) VALUES (
                        'SPNET_TRAFFIC', :1, :2, 'SUCCESS',
                        :3, :4, 'RESTORE_SCRIPT'
                    )
                """, (file_name, records_count, first_load or datetime.now(), last_load or datetime.now()))
                spnet_inserted += 1
                logger.info(f"  ✓ Добавлено: {file_name} ({records_count:,} записей)")
            else:
                logger.info(f"  [DRY-RUN] Добавили бы: {file_name} ({records_count:,} записей)")
                spnet_inserted += 1
        
        # 2. Восстановление истории для STECCOM
        logger.info("\n💰 Восстановление истории STECCOM Expenses...")
        cursor.execute("""
            SELECT 
                SOURCE_FILE,
                COUNT(*) as records_count,
                MIN(LOAD_DATE) as first_load_date,
                MAX(LOAD_DATE) as last_load_date
            FROM STECCOM_EXPENSES
            WHERE SOURCE_FILE IS NOT NULL
            GROUP BY SOURCE_FILE
            ORDER BY SOURCE_FILE
        """)
        
        steccom_files = cursor.fetchall()
        logger.info(f"Найдено файлов STECCOM: {len(steccom_files)}")
        
        steccom_inserted = 0
        for file_name, records_count, first_load, last_load in steccom_files:
            # Проверяем, есть ли уже запись (используем тот же file_col, что определили выше)
            cursor.execute(f"""
                SELECT COUNT(*) FROM LOAD_LOGS 
                WHERE UPPER({file_col}) = UPPER(:1) 
                AND UPPER(TABLE_NAME) = 'STECCOM_EXPENSES'
                AND LOAD_STATUS = 'SUCCESS'
            """, (file_name,))
            
            exists = cursor.fetchone()[0] > 0
            
            if exists:
                logger.info(f"  ⏭ Пропускаем (уже есть): {file_name}")
                continue
            
            if not dry_run:
                cursor.execute(f"""
                    INSERT INTO LOAD_LOGS (
                        TABLE_NAME, {file_col}, RECORDS_LOADED, LOAD_STATUS,
                        LOAD_START_TIME, LOAD_END_TIME, {loaded_by_col}
                    ) VALUES (
                        'STECCOM_EXPENSES', :1, :2, 'SUCCESS',
                        :3, :4, 'RESTORE_SCRIPT'
                    )
                """, (file_name, records_count, first_load or datetime.now(), last_load or datetime.now()))
                steccom_inserted += 1
                logger.info(f"  ✓ Добавлено: {file_name} ({records_count:,} записей)")
            else:
                logger.info(f"  [DRY-RUN] Добавили бы: {file_name} ({records_count:,} записей)")
                steccom_inserted += 1
        
        if not dry_run:
            conn.commit()
            logger.info(f"\n✅ Успешно восстановлено:")
            logger.info(f"   SPNet: {spnet_inserted} записей")
            logger.info(f"   STECCOM: {steccom_inserted} записей")
        else:
            logger.info(f"\n[DRY-RUN] Было бы восстановлено:")
            logger.info(f"   SPNet: {spnet_inserted} записей")
            logger.info(f"   STECCOM: {steccom_inserted} записей")
        
        # Статистика по load_logs
        cursor.execute("SELECT COUNT(*) FROM LOAD_LOGS WHERE LOAD_STATUS = 'SUCCESS'")
        total_logs = cursor.fetchone()[0]
        logger.info(f"\n📋 Всего записей в LOAD_LOGS: {total_logs}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False


def check_missing_logs(db_type='postgresql'):
    """Проверка файлов, загруженных в таблицы, но отсутствующих в load_logs"""
    config = get_db_config(db_type)
    
    try:
        if db_type == 'postgresql':
            conn = psycopg2.connect(**config)
        else:
            import cx_Oracle
            
            # Используем тот же подход, что и в streamlit_report_oracle_backup.py
            # Проверяем переменную окружения напрямую
            oracle_sid = os.getenv('ORACLE_SID')
            oracle_service = os.getenv('ORACLE_SERVICE') or (oracle_sid if oracle_sid else 'bm7')
            
            # Логируем параметры подключения (без пароля)
            logger.info(f"Подключение к Oracle: {config['username']}@{config['host']}:{config['port']}")
            logger.info(f"ORACLE_SID: {oracle_sid}, ORACLE_SERVICE: {oracle_service}")
            
            # Используем SID если задан ORACLE_SID, иначе SERVICE_NAME (как в приложении)
            if oracle_sid:
                dsn = cx_Oracle.makedsn(
                    config['host'], 
                    config['port'], 
                    sid=oracle_sid
                )
            else:
                dsn = cx_Oracle.makedsn(
                    config['host'], 
                    config['port'], 
                    service_name=oracle_service
                )
            
            conn = cx_Oracle.connect(
                config['username'],
                config['password'],
                dsn
            )
        
        cursor = conn.cursor()
        
        logger.info("="*80)
        logger.info("ПРОВЕРКА ОТСУТСТВУЮЩИХ ЗАПИСЕЙ В LOAD_LOGS")
        logger.info("="*80)
        
        # SPNet
        if db_type == 'postgresql':
            cursor.execute("""
                SELECT 
                    t.source_file,
                    COUNT(*) as records_count
                FROM spnet_traffic t
                WHERE t.source_file IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM load_logs l
                    WHERE LOWER(l.source_file) = LOWER(t.source_file)
                    AND LOWER(l.table_name) = 'spnet_traffic'
                    AND l.load_status = 'SUCCESS'
                )
                GROUP BY t.source_file
                ORDER BY t.source_file
            """)
        else:
            # Проверяем, какой столбец используется в LOAD_LOGS
            # Сначала пробуем FILE_NAME, если не работает - пробуем SOURCE_FILE
            try:
                test_query = "SELECT FILE_NAME FROM LOAD_LOGS WHERE ROWNUM = 1"
                cursor.execute(test_query)
                file_col = "FILE_NAME"
            except:
                try:
                    test_query = "SELECT SOURCE_FILE FROM LOAD_LOGS WHERE ROWNUM = 1"
                    cursor.execute(test_query)
                    file_col = "SOURCE_FILE"
                except:
                    logger.error("Не удалось определить структуру таблицы LOAD_LOGS")
                    raise
            
            cursor.execute(f"""
                SELECT 
                    t.SOURCE_FILE,
                    COUNT(*) as records_count
                FROM SPNET_TRAFFIC t
                WHERE t.SOURCE_FILE IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM LOAD_LOGS l
                    WHERE UPPER(l.{file_col}) = UPPER(t.SOURCE_FILE)
                    AND UPPER(l.TABLE_NAME) = 'SPNET_TRAFFIC'
                    AND l.LOAD_STATUS = 'SUCCESS'
                )
                GROUP BY t.SOURCE_FILE
                ORDER BY t.SOURCE_FILE
            """)
        
        spnet_missing = cursor.fetchall()
        logger.info(f"\n📊 SPNet файлы без записей в load_logs: {len(spnet_missing)}")
        for file_name, count in spnet_missing:
            logger.info(f"   - {file_name} ({count:,} записей)")
        
        # STECCOM
        if db_type == 'postgresql':
            cursor.execute("""
                SELECT 
                    t.source_file,
                    COUNT(*) as records_count
                FROM steccom_expenses t
                WHERE t.source_file IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM load_logs l
                    WHERE LOWER(l.source_file) = LOWER(t.source_file)
                    AND LOWER(l.table_name) = 'steccom_expenses'
                    AND l.load_status = 'SUCCESS'
                )
                GROUP BY t.source_file
                ORDER BY t.source_file
            """)
        else:
            # Используем тот же столбец, что определили выше
            cursor.execute(f"""
                SELECT 
                    t.SOURCE_FILE,
                    COUNT(*) as records_count
                FROM STECCOM_EXPENSES t
                WHERE t.SOURCE_FILE IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM LOAD_LOGS l
                    WHERE UPPER(l.{file_col}) = UPPER(t.SOURCE_FILE)
                    AND UPPER(l.TABLE_NAME) = 'STECCOM_EXPENSES'
                    AND l.LOAD_STATUS = 'SUCCESS'
                )
                GROUP BY t.SOURCE_FILE
                ORDER BY t.SOURCE_FILE
            """)
        
        steccom_missing = cursor.fetchall()
        logger.info(f"\n💰 STECCOM файлы без записей в load_logs: {len(steccom_missing)}")
        for file_name, count in steccom_missing:
            logger.info(f"   - {file_name} ({count:,} записей)")
        
        cursor.close()
        conn.close()
        
        return len(spnet_missing) + len(steccom_missing)
        
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return -1


def main():
    parser = argparse.ArgumentParser(
        description='Восстановление истории загрузок в load_logs'
    )
    parser.add_argument(
        '--db-type',
        choices=['postgresql', 'oracle'],
        default='postgresql',
        help='Тип базы данных (по умолчанию: postgresql)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Только показать, что будет сделано, без изменений'
    )
    parser.add_argument(
        '--check-only',
        action='store_true',
        help='Только проверить отсутствующие записи, без восстановления'
    )
    
    args = parser.parse_args()
    
    # config.env уже загружен при импорте модуля через load_config_env()
    # Но можно перезагрузить, если нужно
    load_config_env()
    
    if args.check_only:
        missing = check_missing_logs(args.db_type)
        sys.exit(0 if missing >= 0 else 1)
    else:
        if args.db_type == 'postgresql':
            success = restore_postgresql_history(args.dry_run)
        else:
            success = restore_oracle_history(args.dry_run)
        
        sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()

