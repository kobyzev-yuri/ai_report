"""
Модуль с SQL запросами и функциями получения данных из Oracle
"""
import pandas as pd
import cx_Oracle
import os
import io
from pathlib import Path
import streamlit as st
from datetime import datetime

def count_file_records(file_path):
    """Подсчет количества записей в файле (CSV или XLSX)"""
    try:
        if str(file_path).lower().endswith('.csv'):
            # Для CSV читаем количество строк (минус заголовок)
            import csv
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                return sum(1 for line in f) - 1
        elif str(file_path).lower().endswith('.xlsx'):
            # Для Excel используем pandas
            df = pd.read_excel(file_path)
            return len(df)
    except Exception as e:
        print(f"Ошибка подсчета строк в файле {file_path}: {e}")
        return None
    return None

def get_records_in_db(get_connection, file_name, table_name='SPNET_TRAFFIC'):
    """Получить количество записей в базе для файла"""
    conn = get_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        query = f"SELECT COUNT(*) FROM {table_name} WHERE LOWER(SOURCE_FILE) = :file_name"
        cursor.execute(query, file_name=file_name.lower())
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except Exception as e:
        st.error(f"Ошибка получения количества записей из базы: {e}")
        return None
    finally:
        if conn:
            conn.close()

def get_main_report(get_connection, period_filter=None, plan_filter=None, contract_id_filter=None, imei_filter=None, customer_name_filter=None, code_1c_filter=None):
    """Получение основного отчета"""
    conn = get_connection()
    if not conn:
        return None
    
    # Фильтр по периодам
    period_condition = ""
    if period_filter and period_filter != "All Periods":
        period_condition = f"AND v.FINANCIAL_PERIOD = '{period_filter}'"
    
    # Фильтр по тарифам
    plan_condition = ""
    if plan_filter and plan_filter != "All Plans":
        plan_condition = f"AND v.PLAN_NAME = '{plan_filter}'"
    
    # Фильтр по CONTRACT_ID
    contract_condition = ""
    if contract_id_filter and contract_id_filter.strip():
        contract_value = contract_id_filter.strip().replace("'", "''")
        contract_condition = f"AND v.CONTRACT_ID LIKE '%{contract_value}%'"
    
    # Фильтр по IMEI
    imei_condition = ""
    if imei_filter and imei_filter.strip():
        imei_value = imei_filter.strip().replace("'", "''")
        imei_condition = f"AND v.IMEI = '{imei_value}'"
    
    # Фильтр по названию клиента
    customer_condition = ""
    if customer_name_filter and customer_name_filter.strip():
        customer_value = customer_name_filter.strip().replace("'", "''")
        customer_condition = f"AND UPPER(COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '')) LIKE UPPER('%{customer_value}%')"
    
    # Фильтр по коду 1С
    code_1c_condition = ""
    if code_1c_filter and code_1c_filter.strip():
        code_1c_value = code_1c_filter.strip().replace("'", "''")
        code_1c_condition = f"AND v.CODE_1C LIKE '%{code_1c_value}%'"
    
    base_query = """
    SELECT 
        v.FINANCIAL_PERIOD AS "Отчетный Период",
        v.BILL_MONTH AS "Bill Month",
        v.IMEI AS "IMEI",
        v.CONTRACT_ID AS "Contract ID",
        COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '') AS "Organization/Person",
        v.CODE_1C AS "Code 1C",
        v.SERVICE_ID AS "Service ID",
        v.AGREEMENT_NUMBER AS "Agreement #",
        CASE 
            WHEN v.ACTIVATION_DATE IS NOT NULL THEN TO_CHAR(v.ACTIVATION_DATE, 'YYYY-MM-DD')
            ELSE NULL
        END AS "Activation Date",
        COALESCE(v.PLAN_NAME, '') AS "Plan Name",
        COALESCE(v.STECCOM_PLAN_NAME_MONTHLY, '') AS "Plan Monthly",
        COALESCE(v.STECCOM_PLAN_NAME_SUSPENDED, '') AS "Plan Suspended",
        ROUND(v.TRAFFIC_USAGE_BYTES / 1000, 2) AS "Traffic Usage (KB)",
        v.MAILBOX_EVENTS AS "Mailbox Events",
        v.REGISTRATION_EVENTS AS "Registration Events",
        v.OVERAGE_KB AS "Overage (KB)",
        v.CALCULATED_OVERAGE AS "Calculated Overage ($)",
        NVL(v.SPNET_TOTAL_AMOUNT, 0) AS "Total Amount ($)",
        NVL(v.FEE_ACTIVATION_FEE, 0) AS "Activation Fee",
        NVL(v.FEE_ADVANCE_CHARGE, 0) AS "Advance Charge",
        NVL(v.FEE_ADVANCE_CHARGE_PREVIOUS_MONTH, 0) AS "Advance Charge Previous Month",
        NVL(v.FEE_CREDIT, 0) AS "Credit",
        NVL(v.FEE_CREDITED, 0) AS "Credited",
        NVL(v.FEE_PRORATED, 0) AS "Prorated"
    FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
    WHERE 1=1
        {plan_condition}
        {period_condition}
        {contract_condition}
        {imei_condition}
        {customer_condition}
        {code_1c_condition}
    ORDER BY v.BILL_MONTH DESC, "Calculated Overage ($)" DESC NULLS LAST
    """
    
    query = base_query.format(
        plan_condition=plan_condition,
        period_condition=period_condition,
        contract_condition=contract_condition,
        imei_condition=imei_condition,
        customer_condition=customer_condition,
        code_1c_condition=code_1c_condition
    )
    
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"Ошибка получения отчета: {e}")
        return None
    finally:
        if conn:
            conn.close()

def get_current_period(get_connection):
    """Получение текущего периода из BM_PERIOD"""
    conn = get_connection()
    if not conn:
        return None
    try:
        query = """
        SELECT TO_CHAR(START_DATE, 'YYYY-MM') AS PERIOD_YYYYMM FROM BM_PERIOD
        WHERE SYSDATE BETWEEN START_DATE AND STOP_DATE
        ORDER BY PERIOD_ID DESC FETCH FIRST 1 ROW ONLY
        """
        cursor = conn.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        cursor.close()
        return str(row[0]) if row else None
    except:
        return None
    finally:
        if conn: conn.close()

def get_periods(get_connection):
    """Получение списка периодов из BM_PERIOD"""
    conn = get_connection()
    if not conn:
        return []
    try:
        query = """
        SELECT DISTINCT TO_CHAR(START_DATE, 'YYYY-MM') AS PERIOD_YYYYMM
        FROM BM_PERIOD WHERE START_DATE IS NOT NULL
        ORDER BY TO_CHAR(START_DATE, 'YYYY-MM') DESC FETCH FIRST 100 ROWS ONLY
        """
        cursor = conn.cursor()
        cursor.execute(query)
        periods = [(str(row[0]), str(row[0])) for row in cursor.fetchall() if row[0]]
        cursor.close()
        return periods
    except:
        return []
    finally:
        if conn: conn.close()

@st.cache_data(ttl=300)
def get_plans(_get_connection):
    """Получение списка тарифных планов"""
    conn = _get_connection()
    if not conn:
        return []
    try:
        query = "SELECT DISTINCT PLAN_NAME FROM V_CONSOLIDATED_REPORT_WITH_BILLING WHERE PLAN_NAME IS NOT NULL ORDER BY PLAN_NAME"
        cursor = conn.cursor()
        cursor.execute(query)
        plans = [row[0] for row in cursor.fetchall() if row[0]]
        cursor.close()
        return plans
    except:
        return []
    finally:
        if conn: conn.close()

def get_revenue_periods(get_connection):
    """Получение списка периодов из доходов"""
    conn = get_connection()
    if not conn:
        return []
    try:
        query = "SELECT DISTINCT PERIOD_YYYYMM FROM V_REVENUE_FROM_INVOICES WHERE PERIOD_YYYYMM IS NOT NULL ORDER BY PERIOD_YYYYMM DESC"
        cursor = conn.cursor()
        cursor.execute(query)
        periods = [(str(row[0]), str(row[0])) for row in cursor.fetchall() if row[0]]
        cursor.close()
        return periods
    except:
        return []
    finally:
        if conn: conn.close()

def get_revenue_report(get_connection, period_filter=None, contract_id_filter=None, imei_filter=None, customer_name_filter=None, code_1c_filter=None):
    """Получение отчета по доходам"""
    conn = get_connection()
    if not conn:
        return None
    
    conds = []
    if period_filter and period_filter != "All Periods": 
        conds.append(f"v.PERIOD_YYYYMM = '{period_filter}'")
    if contract_id_filter: 
        val = contract_id_filter.strip().replace("'", "''")
        conds.append(f"v.CONTRACT_ID LIKE '%{val}%'")
    if imei_filter: 
        conds.append(f"v.IMEI = '{imei_filter.strip()}'")
    if customer_name_filter: 
        val = customer_name_filter.strip().replace("'", "''")
        conds.append(f"UPPER(COALESCE(v.CUSTOMER_NAME, '')) LIKE UPPER('%{val}%')")
    if code_1c_filter: 
        conds.append(f"v.CODE_1C LIKE '%{code_1c_filter.strip()}%'")
    
    where = " AND ".join(conds) if conds else "1=1"
    query = f"SELECT * FROM V_REVENUE_FROM_INVOICES v WHERE {where} ORDER BY v.PERIOD_YYYYMM DESC, v.CONTRACT_ID"
    
    try:
        return pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"Ошибка получения отчета по доходам: {e}")
        return None
    finally:
        if conn: conn.close()

def get_analytics_duplicates(get_connection, period_id):
    """Поиск дубликатов в ANALYTICS
    Дубликаты определяются как записи, где ВСЕ поля совпадают, кроме AID
    ВЕРСИЯ 2.1 - включает все поля таблицы ANALYTICS (исправлено 2026-01-03)
    """
    conn = get_connection()
    if not conn: return None
    
    # Явно указываем все колонки для избежания проблем с кэшированием
    query = f"""
    SELECT 
        COUNT(*) AS DUPLICATE_COUNT,
        LISTAGG(AID, ', ') WITHIN GROUP (ORDER BY AID) AS AID_LIST,
        MAX(PERIOD_ID) AS PERIOD_ID,
        MAX(DOMAIN_ID) AS DOMAIN_ID,
        MAX(GROUP_ID) AS GROUP_ID,
        MAX(SERVICE_ID) AS SERVICE_ID,
        MAX(CUSTOMER_ID) AS CUSTOMER_ID,
        MAX(ACCOUNT_ID) AS ACCOUNT_ID,
        MAX(TYPE_ID) AS TYPE_ID,
        MAX(TARIFF_ID) AS TARIFF_ID,
        MAX(TARIFFEL_ID) AS TARIFFEL_ID,
        MAX(VSAT) AS VSAT,
        MAX(MONEY) AS MONEY,
        MAX(PRICE) AS PRICE,
        MAX(TRAF) AS TRAF,
        MAX(TOTAL_TRAF) AS TOTAL_TRAF,
        MAX(CBYTE) AS CBYTE,
        MAX(INVOICE_ITEM_ID) AS INVOICE_ITEM_ID,
        MAX(FLAG) AS FLAG,
        MAX(RESOURCE_TYPE_ID) AS RESOURCE_TYPE_ID,
        MAX(CLASS_ID) AS CLASS_ID,
        MAX(CLASS_NAME) AS CLASS_NAME,
        MAX(BLANK) AS BLANK,
        MAX(COUNTER_ID) AS COUNTER_ID,
        MAX(COUNTER_CF) AS COUNTER_CF,
        MAX(ZONE_ID) AS ZONE_ID,
        MAX(THRESHOLD) AS THRESHOLD,
        MAX(SUB_TYPE_ID) AS SUB_TYPE_ID,
        MAX(SUB_PERIOD_ID) AS SUB_PERIOD_ID,
        MAX(PMONEY) AS PMONEY,
        MAX(PARTNER_PERCENT) AS PARTNER_PERCENT,
        MAX(CARD_ID) AS CARD_ID,
        MAX(SERIAL_ID) AS SERIAL_ID,
        MAX(SUBSCRIPTION_ID) AS SUBSCRIPTION_ID,
        MAX(IRIFILENUM) AS IRIFILENUM
    FROM ANALYTICS
    WHERE PERIOD_ID = {period_id}
    GROUP BY 
        PERIOD_ID,
        NVL(DOMAIN_ID, -999999),
        NVL(GROUP_ID, -999999),
        NVL(SERVICE_ID, -999999),
        NVL(CUSTOMER_ID, -999999),
        NVL(ACCOUNT_ID, -999999),
        NVL(TYPE_ID, -999999),
        NVL(TARIFF_ID, -999999),
        NVL(TARIFFEL_ID, -999999),
        NVL(VSAT, 'NULL'),
        NVL(MONEY, -999999),
        NVL(PRICE, -999999),
        NVL(TRAF, -999999),
        NVL(TOTAL_TRAF, -999999),
        NVL(CBYTE, -999999),
        NVL(INVOICE_ITEM_ID, -999999),
        NVL(FLAG, -999999),
        NVL(RESOURCE_TYPE_ID, -999999),
        NVL(CLASS_ID, -999999),
        NVL(CLASS_NAME, 'NULL'),
        NVL(BLANK, 'NULL'),
        NVL(COUNTER_ID, -999999),
        NVL(COUNTER_CF, -999999),
        NVL(ZONE_ID, -999999),
        NVL(THRESHOLD, -999999),
        NVL(SUB_TYPE_ID, -999999),
        NVL(SUB_PERIOD_ID, -999999),
        NVL(PMONEY, -999999),
        NVL(PARTNER_PERCENT, -999999),
        NVL(CARD_ID, -999999),
        NVL(SERIAL_ID, -999999),
        NVL(SUBSCRIPTION_ID, -999999),
        NVL(IRIFILENUM, -999999)
    HAVING COUNT(*) > 1
    ORDER BY DUPLICATE_COUNT DESC
    """
    try:
        # Выполняем запрос напрямую, без кэширования pandas
        cursor = conn.cursor()
        cursor.execute(query)
        
        # Получаем имена колонок из описания курсора
        column_names = [desc[0] for desc in cursor.description]
        
        # Отладочная информация ДО получения данных
        print(f"DEBUG v2.1: Query executed, cursor.description has {len(cursor.description)} columns")
        print(f"DEBUG v2.1: Column names from cursor: {column_names[:10]}... (showing first 10)")
        if len(column_names) != 35:
            print(f"ERROR v2.1: Expected 35 columns, got {len(column_names)}!")
            print(f"ERROR v2.1: Full column list: {column_names}")
        
        # Получаем данные
        rows = cursor.fetchall()
        cursor.close()
        
        # Создаем DataFrame вручную
        import pandas as pd
        df = pd.DataFrame(rows, columns=column_names)
        
        # Отладочная информация ПОСЛЕ создания DataFrame
        if df is not None and not df.empty:
            print(f"DEBUG v2.1: get_analytics_duplicates returned {len(df)} rows with {len(df.columns)} columns")
            print(f"DEBUG v2.1: Expected 35 columns, got {len(df.columns)}")
            if len(df.columns) != 35:
                print(f"ERROR v2.1: Column count mismatch! Expected 35, got {len(df.columns)}")
                print(f"ERROR v2.1: DataFrame columns: {list(df.columns)}")
                print(f"ERROR v2.1: Missing columns check - ZONE_ID present: {'ZONE_ID' in df.columns}")
                print(f"ERROR v2.1: Missing columns check - TARIFFEL_ID present: {'TARIFFEL_ID' in df.columns}")
        elif df is not None and df.empty:
            print(f"DEBUG v2.1: get_analytics_duplicates returned empty DataFrame (0 rows, {len(df.columns)} columns)")
            print(f"DEBUG v2.1: This is correct if there are no true duplicates (all fields match)")
        
        return df
    except Exception as e:
        import traceback
        print(f"Error in get_analytics_duplicates: {e}")
        print(traceback.format_exc())
        return None
    finally:
        if conn: conn.close()

def remove_analytics_duplicates(get_connection, period_id=None):
    """
    Удаление дубликатов из ANALYTICS
    Оставляет только одну запись с максимальным AID для каждой группы дубликатов
    
    Args:
        get_connection: Функция получения подключения
        period_id: Опционально - период для удаления дубликатов. Если None - удаляет для всех периодов
    
    Returns:
        tuple: (success: bool, deleted_count: int, message: str)
    """
    conn = get_connection()
    if not conn:
        return False, 0, "Ошибка подключения к базе данных"
    
    try:
        cursor = conn.cursor()
        
        # Формируем условие WHERE для периода (если указан)
        period_where = f"WHERE PERIOD_ID = {period_id}" if period_id else ""
        period_and = f"AND PERIOD_ID = {period_id}" if period_id else ""
        
        # SQL для удаления дубликатов используя подзапрос с ROW_NUMBER
        # Удаляем все записи кроме той, у которой максимальный AID в группе дубликатов
        # Группируем по ВСЕМ полям таблицы ANALYTICS кроме AID
        delete_query = f"""
        DELETE FROM ANALYTICS
        WHERE AID IN (
            SELECT AID
            FROM (
                SELECT 
                    AID,
                    ROW_NUMBER() OVER (
                        PARTITION BY 
                            PERIOD_ID,
                            NVL(DOMAIN_ID, -999999),
                            NVL(GROUP_ID, -999999),
                            NVL(SERVICE_ID, -999999),
                            NVL(CUSTOMER_ID, -999999),
                            NVL(ACCOUNT_ID, -999999),
                            NVL(TYPE_ID, -999999),
                            NVL(TARIFF_ID, -999999),
                            NVL(TARIFFEL_ID, -999999),
                            NVL(VSAT, 'NULL'),
                            NVL(MONEY, -999999),
                            NVL(PRICE, -999999),
                            NVL(TRAF, -999999),
                            NVL(TOTAL_TRAF, -999999),
                            NVL(CBYTE, -999999),
                            NVL(INVOICE_ITEM_ID, -999999),
                            NVL(FLAG, -999999),
                            NVL(RESOURCE_TYPE_ID, -999999),
                            NVL(CLASS_ID, -999999),
                            NVL(CLASS_NAME, 'NULL'),
                            NVL(BLANK, 'NULL'),
                            NVL(COUNTER_ID, -999999),
                            NVL(COUNTER_CF, -999999),
                            NVL(ZONE_ID, -999999),
                            NVL(THRESHOLD, -999999),
                            NVL(SUB_TYPE_ID, -999999),
                            NVL(SUB_PERIOD_ID, -999999),
                            NVL(PMONEY, -999999),
                            NVL(PARTNER_PERCENT, -999999),
                            NVL(CARD_ID, -999999),
                            NVL(SERIAL_ID, -999999),
                            NVL(SUBSCRIPTION_ID, -999999),
                            NVL(IRIFILENUM, -999999)
                        ORDER BY AID DESC
                    ) AS rn
                FROM ANALYTICS
                {period_where}
            )
            WHERE rn > 1  -- Удаляем все кроме первой записи (с максимальным AID)
        )
        """
        
        cursor.execute(delete_query)
        deleted_count = cursor.rowcount
        conn.commit()
        cursor.close()
        
        period_msg = f" для периода {period_id}" if period_id else " для всех периодов"
        return True, deleted_count, f"✅ Удалено {deleted_count} дубликатов{period_msg}"
        
    except Exception as e:
        conn.rollback()
        return False, 0, f"❌ Ошибка при удалении дубликатов: {str(e)}"
    finally:
        if conn:
            conn.close()

def get_analytics_invoice_period_report(get_connection, period_filter=None, contract_id_filter=None, imei_filter=None, customer_name_filter=None, code_1c_filter=None, tariff_filter=None, zone_filter=None):
    """Получение отчета по счетам из ANALYTICS"""
    conn = get_connection()
    if not conn: return None
    
    conds = []
    if period_filter and period_filter != "All Periods": 
        conds.append(f"v.PERIOD_YYYYMM = '{period_filter}'")
    if contract_id_filter: 
        val = contract_id_filter.strip().replace("'", "''")
        conds.append(f"v.CONTRACT_ID LIKE '%{val}%'")
    if imei_filter: 
        conds.append(f"v.IMEI = '{imei_filter.strip()}'")
    if customer_name_filter: 
        val = customer_name_filter.strip().replace("'", "''")
        conds.append(f"UPPER(COALESCE(v.CUSTOMER_NAME, '')) LIKE UPPER('%{val}%')")
    if code_1c_filter: 
        conds.append(f"v.CODE_1C LIKE '%{code_1c_filter.strip()}%'")
    if tariff_filter: 
        conds.append(f"v.TARIFF_ID = {tariff_filter}")
    if zone_filter: 
        conds.append(f"v.ZONE_ID = {zone_filter}")
    
    where = " AND ".join(conds) if conds else "1=1"
    query = f"SELECT * FROM V_ANALYTICS_INVOICE_PERIOD v WHERE {where} ORDER BY v.PERIOD_YYYYMM DESC, v.CUSTOMER_NAME"
    
    try:
        return pd.read_sql_query(query, conn)
    except:
        return None
    finally:
        if conn: conn.close()
