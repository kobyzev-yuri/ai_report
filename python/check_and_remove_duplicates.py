#!/usr/bin/env python3
"""
Скрипт для проверки и удаления дубликатов в таблице STECCOM_EXPENSES
Для контракта SUB-54278652475 и IMEI 300234069001680
"""

import cx_Oracle
import os
import sys
from datetime import datetime
from collections import defaultdict

# Конфигурация Oracle из переменных окружения
ORACLE_CONFIG = {
    'host': os.getenv('ORACLE_HOST'),
    'port': int(os.getenv('ORACLE_PORT', '1521')),
    'service_name': os.getenv('ORACLE_SERVICE', os.getenv('ORACLE_SID')),
    'username': os.getenv('ORACLE_USER'),
    'password': os.getenv('ORACLE_PASSWORD')
}

CONTRACT_ID = 'SUB-54278652475'
IMEI = '300234069001680'


def connect_to_oracle():
    """Подключение к Oracle"""
    try:
        if ORACLE_CONFIG.get('sid'):
            dsn = cx_Oracle.makedsn(
                ORACLE_CONFIG['host'],
                ORACLE_CONFIG['port'],
                sid=ORACLE_CONFIG['sid']
            )
        else:
            service_name = ORACLE_CONFIG.get('service_name', 'bm7')
            dsn = cx_Oracle.makedsn(
                ORACLE_CONFIG['host'],
                ORACLE_CONFIG['port'],
                service_name=service_name
            )
        
        conn = cx_Oracle.connect(
            user=ORACLE_CONFIG['username'],
            password=ORACLE_CONFIG['password'],
            dsn=dsn
        )
        print("✅ Успешное подключение к Oracle")
        return conn
    except Exception as e:
        print(f"❌ Ошибка подключения к Oracle: {e}")
        return None


def check_duplicates(conn):
    """Проверка дубликатов"""
    cursor = conn.cursor()
    
    print(f"\n{'='*80}")
    print(f"Проверка дубликатов для контракта {CONTRACT_ID} и IMEI {IMEI}")
    print(f"{'='*80}\n")
    
    # Получаем все записи
    query = """
    SELECT 
        ID,
        INVOICE_DATE,
        CONTRACT_ID,
        ICC_ID_IMEI,
        DESCRIPTION,
        AMOUNT,
        TRANSACTION_DATE,
        SOURCE_FILE,
        LOAD_DATE,
        CREATED_BY
    FROM STECCOM_EXPENSES
    WHERE CONTRACT_ID = :contract_id
      AND ICC_ID_IMEI = :imei
      AND CONTRACT_ID IS NOT NULL
      AND ICC_ID_IMEI IS NOT NULL
      AND INVOICE_DATE IS NOT NULL
      AND (SERVICE IS NULL OR UPPER(TRIM(SERVICE)) != 'BROADBAND')
    ORDER BY INVOICE_DATE DESC, TRANSACTION_DATE DESC, ID DESC
    """
    
    cursor.execute(query, {'contract_id': CONTRACT_ID, 'imei': IMEI})
    records = cursor.fetchall()
    
    if not records:
        print("⚠️  Записи не найдены")
        return None, []
    
    print(f"📊 Всего найдено записей: {len(records)}\n")
    
    # Группируем по ключевым полям для поиска дубликатов
    groups = defaultdict(list)
    for record in records:
        record_id, invoice_date, contract_id, imei, description, amount, transaction_date, source_file, load_date, created_by = record
        
        # Ключ для группировки (те же поля, что используются в дедупликации)
        key = (
            invoice_date.strftime('%Y%m') if invoice_date else None,
            contract_id,
            imei,
            description.strip().upper() if description else None,
            amount,
            transaction_date if transaction_date else None
        )
        groups[key].append(record)
    
    # Находим дубликаты
    duplicates = {}
    for key, group_records in groups.items():
        if len(group_records) > 1:
            duplicates[key] = group_records
    
    if not duplicates:
        print("✅ Дубликаты не найдены")
        return None, []
    
    print(f"⚠️  Найдено групп дубликатов: {len(duplicates)}\n")
    
    # Выводим информацию о дубликатах
    total_duplicate_records = 0
    total_duplicate_amount = 0
    
    for key, group_records in duplicates.items():
        invoice_month, contract_id, imei, description, amount, transaction_date = key
        print(f"Группа дубликатов:")
        print(f"  Период: {invoice_month}")
        print(f"  Контракт: {contract_id}")
        print(f"  IMEI: {imei}")
        print(f"  Описание: {description}")
        print(f"  Сумма: {amount}")
        print(f"  Дата транзакции: {transaction_date}")
        print(f"  Количество записей: {len(group_records)}")
        print(f"  Записи:")
        
        # Сортируем по ID для определения, какую оставить
        group_records.sort(key=lambda x: x[0])  # Сортируем по ID
        
        ids_to_delete = []
        for record in group_records:
            record_id, invoice_date, contract_id, imei, description, amount, transaction_date, source_file, load_date, created_by = record
            status = "✅ ОСТАВИТЬ" if record == group_records[0] else "❌ УДАЛИТЬ"
            print(f"    {status} ID: {record_id}, Файл: {source_file}, Загружено: {load_date}")
            if record != group_records[0]:
                ids_to_delete.append(record_id)
        
        total_duplicate_records += len(group_records) - 1
        total_duplicate_amount += amount * (len(group_records) - 1)
        print()
    
    print(f"{'='*80}")
    print(f"Итого:")
    print(f"  Групп дубликатов: {len(duplicates)}")
    print(f"  Записей для удаления: {total_duplicate_records}")
    print(f"  Сумма дубликатов: {total_duplicate_amount:.2f}")
    print(f"{'='*80}\n")
    
    # Собираем все ID для удаления
    ids_to_delete_all = []
    for key, group_records in duplicates.items():
        group_records.sort(key=lambda x: x[0])
        for record in group_records[1:]:  # Пропускаем первую (оставляем)
            ids_to_delete_all.append(record[0])
    
    return duplicates, ids_to_delete_all


def remove_duplicates(conn, ids_to_delete, dry_run=True):
    """Удаление дубликатов"""
    if not ids_to_delete:
        print("Нет записей для удаления")
        return
    
    if dry_run:
        print(f"\n🔍 РЕЖИМ ПРОВЕРКИ (dry-run):")
        print(f"   Будет удалено записей: {len(ids_to_delete)}")
        print(f"   ID для удаления: {', '.join(map(str, ids_to_delete))}")
        print(f"\n   Для реального удаления запустите скрипт с параметром --execute")
        return
    
    print(f"\n⚠️  ВНИМАНИЕ: Выполняется реальное удаление!")
    print(f"   Будет удалено записей: {len(ids_to_delete)}")
    
    response = input("   Продолжить? (yes/no): ")
    if response.lower() != 'yes':
        print("   Отменено пользователем")
        return
    
    cursor = conn.cursor()
    try:
        # Удаляем дубликаты
        delete_query = """
        DELETE FROM STECCOM_EXPENSES
        WHERE ID IN ({})
        """.format(','.join(map(str, ids_to_delete)))
        
        cursor.execute(delete_query)
        deleted_count = cursor.rowcount
        conn.commit()
        
        print(f"\n✅ Успешно удалено записей: {deleted_count}")
        
    except Exception as e:
        conn.rollback()
        print(f"\n❌ Ошибка при удалении: {e}")
        raise
    finally:
        cursor.close()


def main():
    """Основная функция"""
    # Проверка параметров
    required_params = ['host', 'username', 'password', 'service_name']
    missing = [p for p in required_params if not ORACLE_CONFIG.get(p)]
    if missing:
        print(f"❌ Ошибка: Не установлены переменные окружения: {', '.join(missing)}")
        sys.exit(1)
    
    # Проверка режима выполнения
    dry_run = '--execute' not in sys.argv
    
    # Подключение
    conn = connect_to_oracle()
    if not conn:
        sys.exit(1)
    
    try:
        # Проверка дубликатов
        duplicates, ids_to_delete = check_duplicates(conn)
        
        if duplicates:
            # Удаление дубликатов
            remove_duplicates(conn, ids_to_delete, dry_run=dry_run)
        else:
            print("\n✅ Дубликаты не найдены, удаление не требуется")
            
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()
        print("\n🔌 Подключение закрыто")


if __name__ == "__main__":
    main()


























