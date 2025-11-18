#!/usr/bin/env python3
"""
Детальная проверка JOIN для предыдущего месяца
"""
import sys
import os

try:
    import oracledb as oracle
except ImportError:
    import cx_Oracle as oracle

def check_prev_month_join():
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
        print("Детальная проверка JOIN для предыдущего месяца (как в представлении)")
        print("=" * 120)
        print()
        
        # Точная копия логики JOIN из представления
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
                    SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' THEN se.AMOUNT ELSE 0 END) AS fee_advance_charge
                FROM unique_steccom_expenses se
                WHERE se.rn = 1
                GROUP BY 
                    TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
                    se.CONTRACT_ID, 
                    se.ICC_ID_IMEI
            )
            SELECT 
                cor.IMEI,
                cor.CONTRACT_ID,
                cor.BILL_MONTH,
                cor.BILL_MONTH_YYYMM,
                CASE 
                    WHEN cor.BILL_MONTH IS NOT NULL AND LENGTH(TRIM(cor.BILL_MONTH)) >= 6 THEN
                        TO_CHAR(ADD_MONTHS(TO_DATE(SUBSTR(TRIM(cor.BILL_MONTH), 1, 6), 'YYYYMM'), -1), 'YYYYMM')
                    ELSE NULL
                END AS calculated_prev_month,
                sf_prev.bill_month AS sf_prev_bill_month,
                sf_prev.fee_advance_charge AS prev_advance_from_join,
                CASE 
                    WHEN RTRIM(cor.CONTRACT_ID) = RTRIM(sf_prev.CONTRACT_ID) THEN 'MATCH' 
                    ELSE 'MISMATCH' 
                END AS contract_match,
                CASE 
                    WHEN cor.IMEI = sf_prev.imei THEN 'MATCH' 
                    ELSE 'MISMATCH' 
                END AS imei_match
            FROM (
                SELECT DISTINCT
                    IMEI,
                    CONTRACT_ID,
                    BILL_MONTH,
                    CASE 
                        WHEN BILL_MONTH IS NOT NULL AND LENGTH(TRIM(BILL_MONTH)) >= 6 THEN
                            SUBSTR(TRIM(BILL_MONTH), 1, 6)
                        ELSE BILL_MONTH
                    END AS BILL_MONTH_YYYMM
                FROM V_CONSOLIDATED_OVERAGE_REPORT
                WHERE IMEI IN (:imei1, :imei2)
                  AND BILL_MONTH = '202510'
            ) cor
            LEFT JOIN steccom_fees sf_prev ON sf_prev.bill_month = CASE 
                    WHEN cor.BILL_MONTH IS NOT NULL AND LENGTH(TRIM(cor.BILL_MONTH)) >= 6 THEN
                        TO_CHAR(ADD_MONTHS(TO_DATE(SUBSTR(TRIM(cor.BILL_MONTH), 1, 6), 'YYYYMM'), -1), 'YYYYMM')
                    ELSE NULL
                END
                AND RTRIM(cor.CONTRACT_ID) = RTRIM(sf_prev.CONTRACT_ID)
                AND cor.IMEI = sf_prev.imei
            ORDER BY cor.IMEI
        """
        
        cursor.execute(query, {'imei1': imeis[0], 'imei2': imeis[1]})
        rows = cursor.fetchall()
        
        if rows:
            print(f"{'IMEI':<20} {'CONTRACT_ID':<20} {'BILL_MONTH':<15} {'CALC_PREV':<12} {'SF_PREV':<12} {'PREV_ADV':<15} {'CONTRACT':<10} {'IMEI_MATCH':<10}")
            print("-" * 120)
            for row in rows:
                imei, contract_id, bill_month, bill_month_yyymm, calc_prev, sf_prev_month, prev_adv, contract_match, imei_match = row
                print(f"{imei:<20} {contract_id or '':<20} {bill_month or '':<15} {calc_prev or 'NULL':<12} {sf_prev_month or 'NULL':<12} {prev_adv or 0:<15.2f} {contract_match or 'NULL':<10} {imei_match or 'NULL':<10}")
        
        print()
        print("=" * 120)
        print("Проверка: что находится в steccom_fees для предыдущего месяца (202509)")
        print("=" * 120)
        print()
        
        query2 = """
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
                    SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' THEN se.AMOUNT ELSE 0 END) AS fee_advance_charge
                FROM unique_steccom_expenses se
                WHERE se.rn = 1
                GROUP BY 
                    TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
                    se.CONTRACT_ID, 
                    se.ICC_ID_IMEI
            )
            SELECT 
                sf.bill_month,
                sf.CONTRACT_ID,
                sf.imei,
                sf.fee_advance_charge
            FROM steccom_fees sf
            WHERE sf.bill_month = '202509'
              AND sf.imei IN (:imei1, :imei2)
            ORDER BY sf.imei
        """
        
        cursor.execute(query2, {'imei1': imeis[0], 'imei2': imeis[1]})
        rows2 = cursor.fetchall()
        
        if rows2:
            print(f"{'BILL_MONTH':<12} {'CONTRACT_ID':<20} {'IMEI':<20} {'ADVANCE_CHARGE':<15}")
            print("-" * 70)
            for row in rows2:
                bill_month, contract_id, imei, advance_charge = row
                print(f"{bill_month or '':<12} {contract_id or '':<20} {imei:<20} {advance_charge or 0:<15.2f}")
        
        cursor.close()
        conn.close()
        
        return 0
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(check_prev_month_join())

