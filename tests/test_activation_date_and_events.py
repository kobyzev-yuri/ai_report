#!/usr/bin/env python3
"""
Тест Activation Date и подсчета событий разных типов
Проверяет проблемы:
1. Activation Date не появилось перед планом
2. Events не считаются (все колонки показывают 0)
"""
import sys
import os

try:
    import oracledb as oracle
except ImportError:
    import cx_Oracle as oracle

def test_activation_date_and_events():
    """Тест Activation Date и событий"""
    imei = '300234069209690'
    contract_id = 'SUB-61996030217'
    
    oracle_host = os.getenv('ORACLE_HOST', 'localhost')
    oracle_port = os.getenv('ORACLE_PORT', '1521')
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
        print("Тест Activation Date и подсчета событий")
        print("=" * 120)
        print(f"IMEI: {imei}")
        print(f"Contract ID: {contract_id}")
        print()
        
        # Тест 1: Проверка наличия ACTIVATION_DATE в представлении
        print("1. Проверка наличия колонки ACTIVATION_DATE в V_CONSOLIDATED_REPORT_WITH_BILLING")
        print("-" * 120)
        try:
            cursor.execute("""
                SELECT COLUMN_NAME, DATA_TYPE 
                FROM USER_TAB_COLUMNS 
                WHERE TABLE_NAME = 'V_CONSOLIDATED_REPORT_WITH_BILLING' 
                AND COLUMN_NAME = 'ACTIVATION_DATE'
            """)
            row = cursor.fetchone()
            if row:
                print(f"   ✅ Колонка ACTIVATION_DATE существует (тип: {row[1]})")
            else:
                print("   ❌ Колонка ACTIVATION_DATE отсутствует!")
                print("   ⚠️  Нужно применить изменения: python apply_oracle_view_fix.py")
        except Exception as e:
            print(f"   ❌ Ошибка проверки: {e}")
        
        print()
        
        # Тест 2: Проверка данных в SPNET_TRAFFIC для этого IMEI
        print("2. Проверка данных в SPNET_TRAFFIC")
        print("-" * 120)
        try:
            cursor.execute("""
                SELECT 
                    USAGE_TYPE,
                    USAGE_UNIT,
                    USAGE_BYTES,
                    ACTUAL_USAGE,
                    CALL_SESSION_COUNT,
                    COUNT(*) as record_count
                FROM SPNET_TRAFFIC
                WHERE IMEI = :imei
                GROUP BY USAGE_TYPE, USAGE_UNIT, USAGE_BYTES, ACTUAL_USAGE, CALL_SESSION_COUNT
                ORDER BY USAGE_TYPE
            """, {'imei': imei})
            rows = cursor.fetchall()
            if rows:
                print(f"   ✅ Найдено типов использования: {len(rows)}")
                total_events_expected = 0
                for row in rows:
                    usage_type, usage_unit, usage_bytes, actual_usage, call_session_count, record_count = row
                    print(f"\n   Usage Type: {usage_type}")
                    print(f"      Usage Unit: {usage_unit}")
                    print(f"      Usage Bytes: {usage_bytes}")
                    print(f"      Actual Usage: {actual_usage}")
                    print(f"      Call Session Count: {call_session_count}")
                    print(f"      Record Count: {record_count}")
                    
                    # Подсчитываем ожидаемое количество событий
                    if usage_unit and usage_unit.upper().strip() == 'EVENT':
                        events = usage_bytes or actual_usage or 0
                        total_events_expected += events
                        print(f"      → События (EVENT): {events}")
                    elif call_session_count:
                        total_events_expected += call_session_count
                        print(f"      → События (CALL_SESSION_COUNT): {call_session_count}")
                    else:
                        total_events_expected += record_count
                        print(f"      → События (COUNT): {record_count}")
                
                print(f"\n   📊 Ожидаемое общее количество событий: {total_events_expected}")
            else:
                print("   ⚠️  Данные не найдены в SPNET_TRAFFIC для этого IMEI")
        except Exception as e:
            print(f"   ❌ Ошибка: {e}")
            import traceback
            traceback.print_exc()
        
        print()
        
        # Тест 3: Проверка данных в V_SPNET_OVERAGE_ANALYSIS
        print("3. Проверка данных в V_SPNET_OVERAGE_ANALYSIS")
        print("-" * 120)
        try:
            cursor.execute("""
                SELECT 
                    MAILBOX_EVENTS,
                    REGISTRATION_EVENTS,
                    EVENTS_COUNT,
                    TRAFFIC_USAGE_BYTES
                FROM V_SPNET_OVERAGE_ANALYSIS
                WHERE IMEI = :imei
                AND ROWNUM <= 10
            """, {'imei': imei})
            rows = cursor.fetchall()
            if rows:
                print(f"   ✅ Найдено записей: {len(rows)}")
                for i, row in enumerate(rows, 1):
                    mailbox_events, reg_events, events_count, traffic = row
                    print(f"\n   Запись {i}:")
                    print(f"      Mailbox Events: {mailbox_events or 0}")
                    print(f"      Registration Events: {reg_events or 0}")
                    print(f"      Events Count: {events_count or 0} (должно быть = Mailbox + Registration)")
                    print(f"      Traffic Usage (bytes): {traffic or 0}")
                    
                    if (events_count or 0) == 0:
                        print(f"      ⚠️  ПРОБЛЕМА: Events Count = 0!")
            else:
                print("   ⚠️  Данные не найдены в V_SPNET_OVERAGE_ANALYSIS")
        except Exception as e:
            print(f"   ❌ Ошибка: {e}")
            import traceback
            traceback.print_exc()
        
        print()
        
        # Тест 3.5: Проверка данных в V_CONSOLIDATED_OVERAGE_REPORT
        print("3.5. Проверка данных в V_CONSOLIDATED_OVERAGE_REPORT")
        print("-" * 120)
        try:
            cursor.execute("""
                SELECT 
                    BILL_MONTH,
                    ACTIVATION_DATE,
                    PLAN_NAME,
                    MAILBOX_EVENTS,
                    REGISTRATION_EVENTS,
                    EVENTS_COUNT,
                    TRAFFIC_USAGE_BYTES
                FROM V_CONSOLIDATED_OVERAGE_REPORT
                WHERE IMEI = :imei
                ORDER BY BILL_MONTH DESC
            """, {'imei': imei})
            rows = cursor.fetchall()
            if rows:
                print(f"   ✅ Найдено записей: {len(rows)}")
                for i, row in enumerate(rows, 1):
                    bill_month, activation_date, plan_name, mailbox_events, reg_events, events_count, traffic = row
                    print(f"\n   Запись {i}:")
                    print(f"      Bill Month: {bill_month}")
                    print(f"      Activation Date: {activation_date or 'NULL'}")
                    print(f"      Plan Name: {plan_name}")
                    print(f"      Mailbox Events: {mailbox_events or 0}")
                    print(f"      Registration Events: {reg_events or 0}")
                    print(f"      Events Count: {events_count or 0} (должно быть = Mailbox + Registration)")
                    if (events_count or 0) == 0:
                        print(f"      ⚠️  ПРОБЛЕМА: Events Count = 0!")
                    print(f"      Traffic Usage (bytes): {traffic or 0}")
            else:
                print("   ⚠️  Данные не найдены в V_CONSOLIDATED_OVERAGE_REPORT")
        except Exception as e:
            print(f"   ❌ Ошибка: {e}")
            import traceback
            traceback.print_exc()
        
        print()
        
        # Тест 4: Проверка данных в V_CONSOLIDATED_REPORT_WITH_BILLING
        print("4. Проверка данных в V_CONSOLIDATED_REPORT_WITH_BILLING")
        print("-" * 120)
        try:
            cursor.execute("""
                SELECT 
                    FINANCIAL_PERIOD,
                    BILL_MONTH,
                    ACTIVATION_DATE,
                    PLAN_NAME,
                    MAILBOX_EVENTS,
                    REGISTRATION_EVENTS,
                    EVENTS_COUNT,
                    TRAFFIC_USAGE_BYTES,
                    FEE_ACTIVATION_FEE
                FROM V_CONSOLIDATED_REPORT_WITH_BILLING
                WHERE IMEI = :imei
                ORDER BY BILL_MONTH DESC
            """, {'imei': imei})
            rows = cursor.fetchall()
            if rows:
                print(f"   ✅ Найдено записей: {len(rows)}")
                for i, row in enumerate(rows, 1):
                    fin_period, bill_month, activation_date, plan_name, mailbox_events, reg_events, events_count, traffic, activation_fee = row
                    print(f"\n   Запись {i}:")
                    print(f"      Financial Period: {fin_period}")
                    print(f"      Bill Month: {bill_month}")
                    print(f"      Activation Date: {activation_date or 'NULL'}")
                    if activation_date is None:
                        print(f"      ⚠️  ПРОБЛЕМА: Activation Date = NULL!")
                    print(f"      Plan Name: {plan_name}")
                    print(f"      Mailbox Events: {mailbox_events or 0}")
                    print(f"      Registration Events: {reg_events or 0}")
                    print(f"      Events Count: {events_count or 0} (должно быть = Mailbox + Registration)")
                    if (events_count or 0) == 0:
                        print(f"      ⚠️  ПРОБЛЕМА: Events Count = 0!")
                    print(f"      Traffic Usage (bytes): {traffic or 0}")
                    print(f"      Activation Fee: {activation_fee or 0}")
            else:
                print("   ⚠️  Данные не найдены в V_CONSOLIDATED_REPORT_WITH_BILLING")
        except Exception as e:
            print(f"   ❌ Ошибка: {e}")
            import traceback
            traceback.print_exc()
        
        print()
        
        # Тест 5: Проверка дубликатов в финансовом периоде 2025-11
        print("5. Проверка дубликатов в финансовом периоде 2025-11")
        print("-" * 120)
        try:
            cursor.execute("""
                SELECT 
                    FINANCIAL_PERIOD,
                    BILL_MONTH,
                    IMEI,
                    CONTRACT_ID,
                    COUNT(*) as record_count
                FROM V_CONSOLIDATED_REPORT_WITH_BILLING
                WHERE FINANCIAL_PERIOD = '2025-11'
                GROUP BY FINANCIAL_PERIOD, BILL_MONTH, IMEI, CONTRACT_ID
                HAVING COUNT(*) > 1
                ORDER BY record_count DESC
            """)
            duplicates = cursor.fetchall()
            if duplicates:
                print(f"   ⚠️  Найдено дубликатов: {len(duplicates)}")
                for row in duplicates[:10]:
                    fin_period, bill_month, imei, contract_id, count = row
                    print(f"      IMEI: {imei}, Contract: {contract_id}, Bill Month: {bill_month}, Записей: {count}")
            else:
                print("   ✅ Дубликатов не найдено")
        except Exception as e:
            print(f"   ❌ Ошибка: {e}")
        
        print()
        print("=" * 120)
        print("РЕЗУЛЬТАТЫ ТЕСТА")
        print("=" * 120)
        
        cursor.close()
        conn.close()
        
        return 0
        
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(test_activation_date_and_events())

