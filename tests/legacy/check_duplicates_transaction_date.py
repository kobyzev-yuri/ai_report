#!/usr/bin/env python3
"""
Проверка дубликатов и TRANSACTION_DATE для проблемных IMEI
"""
import sys
import os

try:
    import oracledb as oracle
    USE_ORACLEDB = True
except ImportError:
    try:
        import cx_Oracle as oracle
        USE_ORACLEDB = False
    except ImportError:
        print("❌ Ошибка: Не установлена библиотека Oracle")
        sys.exit(1)

def check_duplicates():
    imeis = ['300234069508860', '300234069606340']
    
    oracle_host = os.getenv('ORACLE_HOST', 'localhost')
    oracle_port = os.getenv('ORACLE_PORT', '1521')
    oracle_service = os.getenv('ORACLE_SERVICE', 'bm7')
    oracle_user = os.getenv('ORACLE_USER', 'billing7')
    oracle_password = os.getenv('ORACLE_PASSWORD', 'billing')
    
    try:
        if USE_ORACLEDB:
            dsn = f"{oracle_host}:{oracle_port}/{oracle_service}"
            conn = oracle.connect(user=oracle_user, password=oracle_password, dsn=dsn)
        else:
            dsn = oracle.makedsn(oracle_host, int(oracle_port), service_name=oracle_service)
            conn = oracle.connect(user=oracle_user, password=oracle_password, dsn=dsn)
        
        cursor = conn.cursor()
        
        print("=" * 120)
        print("Проверка дубликатов и TRANSACTION_DATE")
        print("=" * 120)
        print()
        
        query = """
            SELECT 
                se.ID,
                se.ICC_ID_IMEI AS IMEI,
                se.CONTRACT_ID,
                se.INVOICE_DATE,
                se.TRANSACTION_DATE,
                TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS bill_month,
                UPPER(TRIM(se.DESCRIPTION)) AS description,
                se.AMOUNT
            FROM STECCOM_EXPENSES se
            WHERE se.ICC_ID_IMEI IN (:imei1, :imei2)
              AND UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%'
              AND TO_CHAR(se.INVOICE_DATE, 'YYYYMM') IN ('202509', '202510')
            ORDER BY se.ICC_ID_IMEI, se.INVOICE_DATE, se.TRANSACTION_DATE, se.ID
        """
        
        cursor.execute(query, {'imei1': imeis[0], 'imei2': imeis[1]})
        rows = cursor.fetchall()
        
        if rows:
            print(f"{'ID':<10} {'IMEI':<20} {'CONTRACT_ID':<20} {'INVOICE_DATE':<15} {'TRANS_DATE':<15} {'BILL_MONTH':<12} {'AMOUNT':<10}")
            print("-" * 120)
            for row in rows:
                id_val, imei, contract_id, invoice_date, trans_date, bill_month, description, amount = row
                print(f"{id_val:<10} {imei:<20} {contract_id or '':<20} {str(invoice_date)[:10]:<15} {str(trans_date)[:19] if trans_date else 'NULL':<15} {bill_month or '':<12} {amount or 0:<10.2f}")
        
        print()
        print("=" * 120)
        print("Проверка: как работает unique_steccom_expenses")
        print("=" * 120)
        print()
        
        query2 = """
            SELECT 
                se.ICC_ID_IMEI AS IMEI,
                se.CONTRACT_ID,
                TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS bill_month,
                UPPER(TRIM(se.DESCRIPTION)) AS description,
                se.AMOUNT,
                se.TRANSACTION_DATE,
                ROW_NUMBER() OVER (
                    PARTITION BY 
                        TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
                        se.CONTRACT_ID,
                        se.ICC_ID_IMEI,
                        UPPER(TRIM(se.DESCRIPTION)),
                        se.AMOUNT,
                        se.TRANSACTION_DATE
                    ORDER BY se.ID
                ) AS rn
            FROM STECCOM_EXPENSES se
            WHERE se.CONTRACT_ID IS NOT NULL
              AND se.ICC_ID_IMEI IS NOT NULL
              AND se.INVOICE_DATE IS NOT NULL
              AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
              AND se.ICC_ID_IMEI IN (:imei1, :imei2)
              AND UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%'
              AND TO_CHAR(se.INVOICE_DATE, 'YYYYMM') IN ('202509', '202510')
            ORDER BY se.ICC_ID_IMEI, bill_month, se.TRANSACTION_DATE, se.ID
        """
        
        cursor.execute(query2, {'imei1': imeis[0], 'imei2': imeis[1]})
        rows2 = cursor.fetchall()
        
        if rows2:
            print(f"{'IMEI':<20} {'CONTRACT_ID':<20} {'BILL_MONTH':<12} {'DESCRIPTION':<20} {'AMOUNT':<10} {'TRANS_DATE':<15} {'RN':<5}")
            print("-" * 120)
            for row in rows2:
                imei, contract_id, bill_month, description, amount, trans_date, rn = row
                print(f"{imei:<20} {contract_id or '':<20} {bill_month or '':<12} {description[:20]:<20} {amount or 0:<10.2f} {str(trans_date)[:19] if trans_date else 'NULL':<15} {rn:<5}")
        
        cursor.close()
        conn.close()
        
        return 0
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(check_duplicates())

