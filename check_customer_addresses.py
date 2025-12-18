#!/usr/bin/env python3
"""
Проверка адресов клиентов в Oracle
"""
import sys
import os
from pathlib import Path

# Добавляем путь к модулям
sys.path.insert(0, str(Path(__file__).parent / 'deploy'))

try:
    from db_connection import get_db_connection
except ImportError:
    # Прямое подключение если модуль не найден
    import cx_Oracle
    
    def get_db_connection():
        config = {
            'user': os.getenv('ORACLE_USER', 'billing7'),
            'password': os.getenv('ORACLE_PASSWORD', 'billing'),
            'host': os.getenv('ORACLE_HOST', 'localhost'),
            'port': int(os.getenv('ORACLE_PORT', '1521')),
            'service_name': os.getenv('ORACLE_SERVICE', 'bm7')
        }
        dsn = cx_Oracle.makedsn(config['host'], config['port'], service_name=config['service_name'])
        return cx_Oracle.connect(config['user'], config['password'], dsn)

def check_customer_addresses(customer_id=None, customer_name_pattern=None):
    """Проверка адресов клиентов"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Запрос для проверки адресов конкретного клиента или нескольких
        if customer_id:
            where_clause = "WHERE cc.CUSTOMER_ID = :customer_id"
            params = {'customer_id': customer_id}
        elif customer_name_pattern:
            where_clause = """WHERE EXISTS (
                SELECT 1 FROM BM_CUSTOMER_CONTACT cc2 
                JOIN BM_CONTACT_DICT cd2 ON cc2.CONTACT_DICT_ID = cd2.CONTACT_DICT_ID
                WHERE cc2.CUSTOMER_ID = cc.CUSTOMER_ID 
                AND cd2.MNEMONIC = 'description' 
                AND cc2.CONTACT_DICT_ID = 23
                AND UPPER(cc2.VALUE) LIKE UPPER(:pattern)
            )"""
            params = {'pattern': f'%{customer_name_pattern}%'}
        else:
            # Проверяем первых 10 клиентов с счетами за текущий год
            where_clause = """WHERE EXISTS (
                SELECT 1 FROM BM_INVOICE inv 
                JOIN BM_PERIODS p ON inv.PERIOD_ID = p.PERIOD_ID 
                WHERE inv.CUSTOMER_ID = cc.CUSTOMER_ID 
                AND TO_CHAR(p.DATE_BEG, 'YYYY') = TO_CHAR(SYSDATE, 'YYYY')
                AND ROWNUM = 1
            ) AND ROWNUM <= 10"""
            params = {}
        
        sql = f"""
        SELECT 
            cc.CUSTOMER_ID,
            COALESCE(
                MAX(CASE WHEN cd.MNEMONIC = 'description' AND cc.CONTACT_DICT_ID = 23 THEN cc.VALUE END),
                TRIM(NVL(MAX(CASE WHEN cd.MNEMONIC = 'last_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' || 
                     NVL(MAX(CASE WHEN cd.MNEMONIC = 'first_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' || 
                     NVL(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), ''))
            ) AS CUSTOMER_NAME,
            MAX(CASE WHEN cd.MNEMONIC = 'juridic_address_id' AND cc.CONTACT_DICT_ID = 8 THEN cc.VALUE END) AS JURIDIC_ADDRESS,
            MAX(CASE WHEN cd.MNEMONIC = 'post_address_id' AND cc.CONTACT_DICT_ID = 8 THEN cc.VALUE END) AS POST_ADDRESS,
            MAX(CASE WHEN cd.MNEMONIC = 'home_address_id' AND cc.CONTACT_DICT_ID = 8 THEN cc.VALUE END) AS HOME_ADDRESS,
            COALESCE(
                MAX(CASE WHEN cd.MNEMONIC = 'juridic_address_id' AND cc.CONTACT_DICT_ID = 8 THEN cc.VALUE END),
                MAX(CASE WHEN cd.MNEMONIC = 'post_address_id' AND cc.CONTACT_DICT_ID = 8 THEN cc.VALUE END),
                MAX(CASE WHEN cd.MNEMONIC = 'home_address_id' AND cc.CONTACT_DICT_ID = 8 THEN cc.VALUE END)
            ) AS LEGAL_ADDRESS,
            -- Проверяем какие типы адресов есть у клиента
            LISTAGG(DISTINCT cd.MNEMONIC, ', ') WITHIN GROUP (ORDER BY cd.MNEMONIC) AS ADDRESS_TYPES
        FROM 
            BM_CUSTOMER_CONTACT cc
        JOIN 
            BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID
        {where_clause}
        GROUP BY 
            cc.CUSTOMER_ID
        ORDER BY 
            CUSTOMER_NAME
        """
        
        print("=" * 100)
        print("Проверка адресов клиентов")
        print("=" * 100)
        print()
        
        if customer_id:
            print(f"Проверка клиента с ID: {customer_id}")
        elif customer_name_pattern:
            print(f"Поиск клиентов по паттерну: {customer_name_pattern}")
        else:
            print("Проверка первых 10 клиентов с счетами за текущий год")
        
        print()
        print(f"SQL запрос:")
        print(sql)
        print()
        print("-" * 100)
        
        cursor.execute(sql, params)
        results = cursor.fetchall()
        
        if not results:
            print("Клиенты не найдены")
            return
        
        print(f"Найдено клиентов: {len(results)}")
        print()
        print(f"{'ID':<10} {'Название':<50} {'Юр. адрес':<15} {'Почт. адрес':<15} {'Дом. адрес':<15} {'Итоговый адрес':<15} {'Типы адресов':<30}")
        print("-" * 100)
        
        for row in results:
            customer_id, name, juridic, post, home, legal, address_types = row
            print(f"{customer_id:<10} {str(name or '')[:48]:<50} "
                  f"{'✓' if juridic else '✗':<15} "
                  f"{'✓' if post else '✗':<15} "
                  f"{'✓' if home else '✗':<15} "
                  f"{'✓' if legal else '✗':<15} "
                  f"{str(address_types or 'нет')[:28]:<30}")
        
        print()
        print("-" * 100)
        print("Детальная информация по первому клиенту:")
        print()
        
        if results:
            first_customer_id = results[0][0]
            detail_sql = """
            SELECT 
                cd.CONTACT_DICT_ID,
                cd.MNEMONIC,
                cd.NAME,
                cc.VALUE
            FROM 
                BM_CUSTOMER_CONTACT cc
            JOIN 
                BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID
            WHERE 
                cc.CUSTOMER_ID = :customer_id
                AND cc.CONTACT_DICT_ID IN (8, 11, 23)
            ORDER BY 
                cd.CONTACT_DICT_ID, cd.MNEMONIC
            """
            
            cursor.execute(detail_sql, {'customer_id': first_customer_id})
            detail_results = cursor.fetchall()
            
            print(f"Клиент ID: {first_customer_id}")
            print(f"Название: {results[0][1]}")
            print()
            print("Контактная информация:")
            print(f"{'CONTACT_DICT_ID':<20} {'MNEMONIC':<30} {'NAME':<40} {'VALUE':<50}")
            print("-" * 140)
            
            for row in detail_results:
                dict_id, mnemonic, name, value = row
                value_str = str(value or '')[:48] if value else 'NULL'
                print(f"{dict_id:<20} {str(mnemonic or ''):<30} {str(name or '')[:38]:<40} {value_str:<50}")
        
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Проверка адресов клиентов в Oracle')
    parser.add_argument('--customer-id', type=int, help='ID клиента для проверки')
    parser.add_argument('--name', type=str, help='Паттерн названия клиента для поиска')
    
    args = parser.parse_args()
    
    try:
        check_customer_addresses(
            customer_id=args.customer_id,
            customer_name_pattern=args.name
        )
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)







