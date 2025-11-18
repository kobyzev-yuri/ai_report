#!/usr/bin/env python3
"""
Проверка текущего состояния представления на сервере
"""
import sys
import os

try:
    import oracledb as oracle
except ImportError:
    import cx_Oracle as oracle

def check_current_state():
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
        print("Проверка текущего состояния представления на сервере")
        print("=" * 120)
        print()
        
        # Проверка для октября
        print("1. Данные для октября (FINANCIAL_PERIOD = 2025-10)")
        print("-" * 120)
        
        query1 = """
            SELECT 
                FINANCIAL_PERIOD,
                BILL_MONTH,
                IMEI,
                CONTRACT_ID,
                FEE_ADVANCE_CHARGE AS current_advance,
                FEE_ADVANCE_CHARGE_PREVIOUS_MONTH AS prev_advance,
                TRAFFIC_USAGE_BYTES,
                EVENTS_COUNT
            FROM V_CONSOLIDATED_REPORT_WITH_BILLING
            WHERE IMEI IN (:imei1, :imei2)
              AND FINANCIAL_PERIOD = '2025-10'
            ORDER BY IMEI
        """
        
        cursor.execute(query1, {'imei1': imeis[0], 'imei2': imeis[1]})
        rows1 = cursor.fetchall()
        
        if rows1:
            print(f"{'FIN_PERIOD':<15} {'BILL_MONTH':<15} {'IMEI':<20} {'CONTRACT_ID':<20} {'CURR_ADV':<12} {'PREV_ADV':<12} {'TRAFFIC':<15} {'EVENTS':<10}")
            print("-" * 120)
            total_prev = 0
            for row in rows1:
                fin_period, bill_month, imei, contract_id, curr_adv, prev_adv, traffic, events = row
                prev_adv_val = prev_adv or 0
                total_prev += prev_adv_val
                print(f"{fin_period or 'NULL':<15} {bill_month or 'NULL':<15} {imei:<20} {contract_id or '':<20} {curr_adv or 0:<12.2f} {prev_adv_val:<12.2f} {traffic or 0:<15} {events or 0:<10}")
            
            print()
            print(f"Итого аванс за предыдущий период для октября: {total_prev:.2f}$")
            print(f"Ожидалось: 16.00$ (12.5 + 3.5)")
            if abs(total_prev - 16.00) < 0.01:
                print("✅ СХОДИТСЯ!")
            else:
                print(f"⚠️  НЕ СХОДИТСЯ! Разница: {total_prev - 16.00:.2f}$")
        else:
            print("⚠️  Нет данных для FINANCIAL_PERIOD = 2025-10")
            print("   Это означает, что изменения еще не применены или UNION ALL не работает")
        
        print()
        
        # Проверка для ноября (должно быть пусто, если изменения применены)
        print("2. Данные для ноября (FINANCIAL_PERIOD = 2025-11) - должно быть пусто")
        print("-" * 120)
        
        query2 = """
            SELECT 
                FINANCIAL_PERIOD,
                BILL_MONTH,
                IMEI,
                CONTRACT_ID,
                FEE_ADVANCE_CHARGE_PREVIOUS_MONTH AS prev_advance
            FROM V_CONSOLIDATED_REPORT_WITH_BILLING
            WHERE IMEI IN (:imei1, :imei2)
              AND FINANCIAL_PERIOD = '2025-11'
            ORDER BY IMEI
        """
        
        cursor.execute(query2, {'imei1': imeis[0], 'imei2': imeis[1]})
        rows2 = cursor.fetchall()
        
        if rows2:
            print(f"⚠️  Найдено записей: {len(rows2)}")
            print("   Это означает, что изменения еще не применены (нет проверки на будущие периоды)")
            for row in rows2:
                fin_period, bill_month, imei, contract_id, prev_adv = row
                print(f"  FINANCIAL_PERIOD: {fin_period}, BILL_MONTH: {bill_month}, IMEI: {imei}, PREV_ADV: {prev_adv}")
        else:
            print("✅ Нет данных для FINANCIAL_PERIOD = 2025-11")
            print("   Это правильно - будущие периоды не должны создаваться")
        
        print()
        print("=" * 120)
        print("ВЫВОД:")
        print("=" * 120)
        print()
        if rows1 and total_prev == 16.00 and not rows2:
            print("✅ Изменения применены корректно!")
            print("   - Для октября есть данные с авансом за предыдущий период (16.00$)")
            print("   - Для ноября нет данных (будущие периоды не создаются)")
        elif rows1 and total_prev == 16.00:
            print("⚠️  Частично применено:")
            print("   - Для октября есть данные с авансом за предыдущий период (16.00$)")
            print("   - Но для ноября все еще есть данные (нужно применить изменения)")
        elif not rows1:
            print("⚠️  Изменения еще не применены:")
            print("   - Для октября нет данных")
            print("   - Нужно применить изменения через apply_oracle_view_fix.py")
        else:
            print("⚠️  Состояние неопределенное - проверьте вручную")
        
        cursor.close()
        conn.close()
        
        return 0
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(check_current_state())

