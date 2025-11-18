#!/usr/bin/env python3
"""
Проверка всех полей у дубликатов для выявления различий
"""
import sys
import os

try:
    import oracledb as oracle
except ImportError:
    import cx_Oracle as oracle

def check_all_fields():
    imeis = ['300234069508860', '300234069606340']
    
    oracle_host = os.getenv('ORACLE_HOST', 'localhost')
    oracle_port = os.getenv('ORACLE_PORT', '15210')
    oracle_service = os.getenv('ORACLE_SERVICE', 'bm7')
    oracle_user = os.getenv('ORACLE_USER', 'billing7')
    oracle_password = os.getenv('ORACLE_PASSWORD', 'billing')
    
    try:
        if 'oracledb' in sys.modules:
            dsn = f"{oracle_host}:{oracle_port}/{oracle_service}"
            conn = oracle.connect(user=oracle_user, password=oracle_password, dsn=dsn)
        else:
            dsn = oracle.makedsn(oracle_host, int(oracle_port), service_name=oracle_service)
            conn = oracle.connect(user=oracle_user, password=oracle_password, dsn=dsn)
        
        cursor = conn.cursor()
        
        print("=" * 150)
        print("Проверка ВСЕХ полей у дубликатов")
        print("=" * 150)
        print()
        
        query = """
            SELECT 
                se.ID,
                se.ICC_ID_IMEI AS IMEI,
                se.CONTRACT_ID,
                se.INVOICE_DATE,
                se.TRANSACTION_DATE,
                se.DESCRIPTION,
                se.AMOUNT,
                se.SOURCE_FILE,
                se.SERVICE,
                se.RATE_TYPE,
                se.PLAN_DISCOUNT,
                se.FEE_TYPE,
                se.COMPANY_NAME,
                se.COMPANY_NUMBER
            FROM STECCOM_EXPENSES se
            WHERE se.ICC_ID_IMEI IN (:imei1, :imei2)
              AND UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%'
              AND TO_CHAR(se.INVOICE_DATE, 'YYYYMM') = '202509'
            ORDER BY se.ICC_ID_IMEI, se.ID
        """
        
        cursor.execute(query, {'imei1': imeis[0], 'imei2': imeis[1]})
        rows = cursor.fetchall()
        
        if rows:
            print(f"{'ID':<8} {'IMEI':<20} {'CONTRACT':<20} {'INVOICE':<12} {'TRANS':<12} {'AMOUNT':<8} {'SOURCE_FILE':<50}")
            print("-" * 150)
            for row in rows:
                id_val, imei, contract_id, invoice_date, trans_date, description, amount, source_file, service, rate_type, plan_discount, fee_type, company_name, company_number = row
                print(f"{id_val:<8} {imei:<20} {contract_id or '':<20} {str(invoice_date)[:10]:<12} {str(trans_date)[:10] if trans_date else 'NULL':<12} {amount or 0:<8.2f} {source_file or '':<50}")
            
            print()
            print("Проверка различий в полях между дубликатами:")
            if len(rows) >= 2:
                print(f"ID1: {rows[0][0]}, ID2: {rows[1][0]}")
                for i, (val1, val2) in enumerate(zip(rows[0], rows[1])):
                    if val1 != val2:
                        print(f"  Поле {i}: '{val1}' != '{val2}'")
        
        cursor.close()
        conn.close()
        
        return 0
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(check_all_fields())

