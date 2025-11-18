#!/usr/bin/env python3
"""
Проверка проблемы с авансом за предыдущий период
IMEI: 300234069508860 (12.5) и 300234069606340 (3.5)
Проблема: не сходится на 16$ (12.5 + 3.5)
"""
import sys
import os

# Попытка использовать oracledb (новая библиотека) или cx_Oracle (старая)
try:
    import oracledb as oracle
    USE_ORACLEDB = True
except ImportError:
    try:
        import cx_Oracle as oracle
        USE_ORACLEDB = False
    except ImportError:
        print("❌ Ошибка: Не установлена библиотека Oracle")
        print("   Установите: pip install oracledb")
        sys.exit(1)

def check_advance_charge_issue():
    """Проверка проблемы с авансом за предыдущий период"""
    
    # Параметры подключения через туннель
    oracle_host = os.getenv('ORACLE_HOST', 'localhost')
    oracle_port = os.getenv('ORACLE_PORT', '15210')
    oracle_service = os.getenv('ORACLE_SERVICE', 'bm7')
    oracle_user = os.getenv('ORACLE_USER', 'billing7')
    oracle_password = os.getenv('ORACLE_PASSWORD', 'billing')
    
    imeis = ['300234069508860', '300234069606340']
    
    print("=" * 100)
    print("Проверка проблемы с авансом за предыдущий период")
    print("=" * 100)
    print(f"IMEI для проверки: {', '.join(imeis)}")
    print(f"Ожидаемые суммы: 12.5 + 3.5 = 16$")
    print()
    
    try:
        # Подключение через туннель
        if USE_ORACLEDB:
            dsn = f"{oracle_host}:{oracle_port}/{oracle_service}"
            conn = oracle.connect(user=oracle_user, password=oracle_password, dsn=dsn)
        else:
            dsn = oracle.makedsn(oracle_host, int(oracle_port), service_name=oracle_service)
            conn = oracle.connect(user=oracle_user, password=oracle_password, dsn=dsn)
        
        cursor = conn.cursor()
        
        # 1. Проверка данных в представлении (что видит финансист)
        print("=" * 100)
        print("1. Данные в представлении V_CONSOLIDATED_REPORT_WITH_BILLING")
        print("=" * 100)
        print()
        
        query1 = """
            SELECT 
                v.IMEI,
                v.CONTRACT_ID,
                v.BILL_MONTH,
                v.FINANCIAL_PERIOD,
                v.FEE_ADVANCE_CHARGE AS current_advance,
                v.FEE_ADVANCE_CHARGE_PREVIOUS_MONTH AS prev_advance
            FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
            WHERE v.IMEI IN (:imei1, :imei2)
            ORDER BY v.IMEI, v.BILL_MONTH DESC
        """
        
        cursor.execute(query1, {'imei1': imeis[0], 'imei2': imeis[1]})
        rows = cursor.fetchall()
        
        if rows:
            print(f"{'IMEI':<20} {'CONTRACT_ID':<20} {'BILL_MONTH':<15} {'FIN_PERIOD':<15} {'CURR_ADV':<12} {'PREV_ADV':<12}")
            print("-" * 100)
            total_prev_advance = 0
            for row in rows:
                imei, contract_id, bill_month, fin_period, curr_adv, prev_adv = row
                prev_adv_val = prev_adv or 0
                total_prev_advance += prev_adv_val
                print(f"{imei:<20} {contract_id or '':<20} {bill_month or '':<15} {fin_period or '':<15} {curr_adv or 0:<12.2f} {prev_adv_val:<12.2f}")
            
            print()
            print(f"Итого аванс за предыдущий период в представлении: {total_prev_advance:.2f}$")
            print(f"Ожидалось: 16.00$")
            print(f"Разница: {16.00 - total_prev_advance:.2f}$")
        else:
            print("⚠️  Записи не найдены в представлении")
        
        print()
        
        # 2. Проверка исходных данных в STECCOM_EXPENSES
        print("=" * 100)
        print("2. Исходные данные в STECCOM_EXPENSES (все периоды)")
        print("=" * 100)
        print()
        
        query2 = """
            SELECT 
                se.ICC_ID_IMEI AS IMEI,
                se.CONTRACT_ID,
                se.INVOICE_DATE,
                TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS bill_month,
                se.DESCRIPTION,
                se.AMOUNT
            FROM STECCOM_EXPENSES se
            WHERE se.ICC_ID_IMEI IN (:imei1, :imei2)
              AND UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%'
            ORDER BY se.ICC_ID_IMEI, se.INVOICE_DATE DESC
        """
        
        cursor.execute(query2, {'imei1': imeis[0], 'imei2': imeis[1]})
        rows2 = cursor.fetchall()
        
        if rows2:
            print(f"{'IMEI':<20} {'CONTRACT_ID':<20} {'INVOICE_DATE':<15} {'BILL_MONTH':<12} {'AMOUNT':<10}")
            print("-" * 100)
            for row in rows2:
                imei, contract_id, invoice_date, bill_month, description, amount = row
                print(f"{imei:<20} {contract_id or '':<20} {str(invoice_date)[:10]:<15} {bill_month or '':<12} {amount or 0:<10.2f}")
        else:
            print("⚠️  Записи не найдены в STECCOM_EXPENSES")
        
        print()
        
        # 3. Проверка JOIN для предыдущего месяца (детальная диагностика)
        print("=" * 100)
        print("3. Детальная диагностика JOIN для предыдущего месяца")
        print("=" * 100)
        print()
        
        query3 = """
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
            ) cor
            LEFT JOIN (
                SELECT 
                    TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS bill_month,
                    se.CONTRACT_ID,
                    se.ICC_ID_IMEI AS imei,
                    SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' THEN se.AMOUNT ELSE 0 END) AS fee_advance_charge
                FROM STECCOM_EXPENSES se
                WHERE se.CONTRACT_ID IS NOT NULL
                  AND se.ICC_ID_IMEI IS NOT NULL
                  AND se.INVOICE_DATE IS NOT NULL
                  AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
                GROUP BY 
                    TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
                    se.CONTRACT_ID, 
                    se.ICC_ID_IMEI
            ) sf_prev ON sf_prev.bill_month = CASE 
                    WHEN cor.BILL_MONTH IS NOT NULL AND LENGTH(TRIM(cor.BILL_MONTH)) >= 6 THEN
                        TO_CHAR(ADD_MONTHS(TO_DATE(SUBSTR(TRIM(cor.BILL_MONTH), 1, 6), 'YYYYMM'), -1), 'YYYYMM')
                    ELSE NULL
                END
                AND RTRIM(cor.CONTRACT_ID) = RTRIM(sf_prev.CONTRACT_ID)
                AND cor.IMEI = sf_prev.imei
            WHERE cor.IMEI IN (:imei1, :imei2)
            ORDER BY cor.IMEI, cor.BILL_MONTH DESC
        """
        
        cursor.execute(query3, {'imei1': imeis[0], 'imei2': imeis[1]})
        rows3 = cursor.fetchall()
        
        if rows3:
            print(f"{'IMEI':<20} {'CONTRACT_ID':<20} {'BILL_MONTH':<15} {'CALC_PREV':<12} {'SF_PREV':<12} {'PREV_ADV':<12} {'CONTRACT':<10} {'IMEI_MATCH':<10}")
            print("-" * 120)
            for row in rows3:
                imei, contract_id, bill_month, bill_month_yyymm, calc_prev, sf_prev_month, prev_adv, contract_match, imei_match = row
                print(f"{imei:<20} {contract_id or '':<20} {bill_month or '':<15} {calc_prev or 'NULL':<12} {sf_prev_month or 'NULL':<12} {prev_adv or 0:<12.2f} {contract_match or 'NULL':<10} {imei_match or 'NULL':<10}")
        else:
            print("⚠️  Записи не найдены")
        
        print()
        
        # 4. Проверка всех доступных периодов для этих IMEI в steccom_fees
        print("=" * 100)
        print("4. Все доступные периоды для этих IMEI в steccom_fees")
        print("=" * 100)
        print()
        
        query4 = """
            SELECT 
                TO_CHAR(se.INVOICE_DATE, 'YYYYMM') AS bill_month,
                se.CONTRACT_ID,
                se.ICC_ID_IMEI AS imei,
                SUM(CASE WHEN UPPER(TRIM(se.DESCRIPTION)) LIKE '%ADVANCE CHARGE%' THEN se.AMOUNT ELSE 0 END) AS fee_advance_charge
            FROM STECCOM_EXPENSES se
            WHERE se.CONTRACT_ID IS NOT NULL
              AND se.ICC_ID_IMEI IS NOT NULL
              AND se.INVOICE_DATE IS NOT NULL
              AND (se.SERVICE IS NULL OR UPPER(TRIM(se.SERVICE)) != 'BROADBAND')
              AND se.ICC_ID_IMEI IN (:imei1, :imei2)
            GROUP BY 
                TO_CHAR(se.INVOICE_DATE, 'YYYYMM'),
                se.CONTRACT_ID, 
                se.ICC_ID_IMEI
            ORDER BY se.ICC_ID_IMEI, bill_month DESC
        """
        
        cursor.execute(query4, {'imei1': imeis[0], 'imei2': imeis[1]})
        rows4 = cursor.fetchall()
        
        if rows4:
            print(f"{'BILL_MONTH':<15} {'CONTRACT_ID':<20} {'IMEI':<20} {'ADVANCE_CHARGE':<15}")
            print("-" * 70)
            for row in rows4:
                bill_month, contract_id, imei, advance_charge = row
                print(f"{bill_month or '':<15} {contract_id or '':<20} {imei:<20} {advance_charge or 0:<15.2f}")
        else:
            print("⚠️  Записи не найдены")
        
        cursor.close()
        conn.close()
        
        print()
        print("=" * 100)
        print("✅ Проверка завершена")
        print("=" * 100)
        
        return 0
        
    except oracle.Error as e:
        error, = e.args
        print(f"❌ Ошибка подключения к Oracle:")
        print(f"   Код: {error.code}")
        print(f"   Сообщение: {error.message}")
        return 1
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(check_advance_charge_issue())

