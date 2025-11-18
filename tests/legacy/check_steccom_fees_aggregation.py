#!/usr/bin/env python3
"""
Проверка агрегации в steccom_fees CTE
"""
import sys
import os

try:
    import oracledb as oracle
except ImportError:
    import cx_Oracle as oracle

def check_aggregation():
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
        
        print("=" * 120)
        print("Проверка агрегации в steccom_fees (как в представлении)")
        print("=" * 120)
        print()
        
        # Точная копия логики из представления
        query = """
            WITH unique_steccom_expenses AS (
                SELECT 
                    se.*,
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
            ),
            steccom_fees AS (
                SELECT 
                    TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS bill_month,
                    se.CONTRACT_ID,
                    se.ICC_ID_IMEI AS imei,
                    SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' THEN se.AMOUNT ELSE 0 END) AS fee_advance_charge,
                    COUNT(*) AS record_count,
                    COUNT(DISTINCT se.ID) AS distinct_ids
                FROM unique_steccom_expenses se
                WHERE se.rn = 1
                  AND se.ICC_ID_IMEI IN (:imei1, :imei2)
                  AND TO_CHAR(se.INVOICE_DATE, 'YYYYMM') = '202509'
                GROUP BY 
                    TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
                    se.CONTRACT_ID, 
                    se.ICC_ID_IMEI
            )
            SELECT 
                sf.bill_month,
                sf.CONTRACT_ID,
                sf.imei,
                sf.fee_advance_charge,
                sf.record_count,
                sf.distinct_ids
            FROM steccom_fees sf
            ORDER BY sf.imei
        """
        
        cursor.execute(query, {'imei1': imeis[0], 'imei2': imeis[1]})
        rows = cursor.fetchall()
        
        if rows:
            print(f"{'BILL_MONTH':<12} {'CONTRACT_ID':<20} {'IMEI':<20} {'ADVANCE_CHARGE':<15} {'REC_COUNT':<10} {'DIST_IDS':<10}")
            print("-" * 100)
            for row in rows:
                bill_month, contract_id, imei, advance_charge, record_count, distinct_ids = row
                print(f"{bill_month or '':<12} {contract_id or '':<20} {imei:<20} {advance_charge or 0:<15.2f} {record_count:<10} {distinct_ids:<10}")
        else:
            print("⚠️  Записи не найдены")
        
        print()
        print("=" * 120)
        print("Проверка: сколько записей попадает в unique_steccom_expenses с rn = 1")
        print("=" * 120)
        print()
        
        query2 = """
            SELECT 
                se.ICC_ID_IMEI AS IMEI,
                se.CONTRACT_ID,
                TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS bill_month,
                COUNT(*) AS total_records,
                SUM(CASE WHEN rn = 1 THEN 1 ELSE 0 END) AS rn1_count,
                SUM(CASE WHEN rn = 1 THEN se.AMOUNT ELSE 0 END) AS rn1_amount_sum
            FROM (
                SELECT 
                    se.*,
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
                  AND TO_CHAR(se.INVOICE_DATE, 'YYYYMM') = '202509'
                  AND UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%'
            ) se
            GROUP BY se.ICC_ID_IMEI, se.CONTRACT_ID, TO_CHAR(se.INVOICE_DATE, 'YYYYMM')
        """
        
        cursor.execute(query2, {'imei1': imeis[0], 'imei2': imeis[1]})
        rows2 = cursor.fetchall()
        
        if rows2:
            print(f"{'IMEI':<20} {'CONTRACT_ID':<20} {'BILL_MONTH':<12} {'TOTAL_REC':<10} {'RN1_COUNT':<10} {'RN1_AMOUNT':<15}")
            print("-" * 100)
            for row in rows2:
                imei, contract_id, bill_month, total_records, rn1_count, rn1_amount_sum = row
                print(f"{imei:<20} {contract_id or '':<20} {bill_month or '':<12} {total_records:<10} {rn1_count:<10} {rn1_amount_sum or 0:<15.2f}")
        
        cursor.close()
        conn.close()
        
        return 0
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(check_aggregation())

