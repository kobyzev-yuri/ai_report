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
    """Поиск дубликатов в ANALYTICS"""
    conn = get_connection()
    if not conn: return None
    
    query = f"""
    SELECT 
        COUNT(*) AS DUPLICATE_COUNT,
        LISTAGG(AID, ', ') WITHIN GROUP (ORDER BY AID) AS AID_LIST,
        PERIOD_ID, SERVICE_ID, CUSTOMER_ID, VSAT, MONEY, PRICE, TRAF
    FROM ANALYTICS
    WHERE PERIOD_ID = {period_id}
    GROUP BY PERIOD_ID, SERVICE_ID, CUSTOMER_ID, VSAT, MONEY, PRICE, TRAF
    HAVING COUNT(*) > 1
    ORDER BY DUPLICATE_COUNT DESC
    """
    try:
        return pd.read_sql_query(query, conn)
    except:
        return None
    finally:
        if conn: conn.close()

def remove_analytics_duplicates(get_connection, period_id):
    """
    Удаление дубликатов в таблице ANALYTICS за заданный PERIOD_ID.
    Дубликаты определяются как записи, где совпадают ВСЕ поля, кроме AID.
    Оставляем запись с максимальным AID в каждой группе.
    """
    conn = get_connection()
    if not conn:
        return False, 0, "❌ Не удалось подключиться к базе данных"

    cursor = conn.cursor()
    try:
        delete_sql = """
        DELETE FROM ANALYTICS
        WHERE AID IN (
            SELECT AID FROM (
                SELECT
                    AID,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            PERIOD_ID,
                            DOMAIN_ID,
                            GROUP_ID,
                            CUSTOMER_ID,
                            ACCOUNT_ID,
                            TYPE_ID,
                            SERVICE_ID,
                            ZONE_ID,
                            PRICE,
                            TRAF,
                            MONEY,
                            VSAT,
                            BLANK,
                            COUNTER_CF,
                            TARIFF_ID,
                            TARIFFEL_ID,
                            THRESHOLD,
                            CLASS_ID,
                            CLASS_NAME,
                            CBYTE,
                            FLAG,
                            COUNTER_ID,
                            RESOURCE_TYPE_ID,
                            SUB_TYPE_ID,
                            SUB_PERIOD_ID,
                            INVOICE_ITEM_ID,
                            CARD_ID,
                            SERIAL_ID,
                            SUBSCRIPTION_ID,
                            TOTAL_TRAF,
                            PMONEY,
                            IRIFILENUM,
                            PARTNER_PERCENT
                        ORDER BY AID DESC
                    ) AS RN
                FROM ANALYTICS
                WHERE PERIOD_ID = :period_id
            )
            WHERE RN > 1
        )
        """

        cursor.execute(delete_sql, period_id=period_id)
        deleted_count = cursor.rowcount if cursor.rowcount is not None else 0
        conn.commit()
        return True, deleted_count, f"✅ Удалено дубликатов: {deleted_count}"
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return False, 0, f"❌ Ошибка удаления дубликатов: {e}"
    finally:
        try:
            cursor.close()
        except Exception:
            pass
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

def get_lbs_services_report(get_connection, contract_id_filter=None, imei_filter=None, customer_name_filter=None, code_1c_filter=None):
    """Получение отчета по активным SBD IMEI сервисам без расходов за последний месяц"""
    conn = get_connection()
    if not conn:
        return None
    
    # Фильтр по CONTRACT_ID
    contract_condition = ""
    if contract_id_filter and contract_id_filter.strip():
        contract_value = contract_id_filter.strip().replace("'", "''")
        contract_condition = f"AND vi.CONTRACT_ID LIKE '%{contract_value}%'"
    
    # Фильтр по IMEI
    imei_condition = ""
    if imei_filter and imei_filter.strip():
        imei_value = imei_filter.strip().replace("'", "''")
        imei_condition = f"AND vi.IMEI = '{imei_value}'"
    
    # Фильтр по названию клиента
    customer_condition = ""
    if customer_name_filter and customer_name_filter.strip():
        customer_value = customer_name_filter.strip().replace("'", "''")
        customer_condition = f"AND UPPER(vi.CUSTOMER_NAME) LIKE UPPER('%{customer_value}%')"
    
    # Фильтр по коду 1С
    code_1c_condition = ""
    if code_1c_filter and code_1c_filter.strip():
        code_1c_value = code_1c_filter.strip().replace("'", "''")
        code_1c_condition = f"AND vi.CODE_1C LIKE '%{code_1c_value}%'"
    
    # Определяем последний месяц для проверки расходов
    # Используем период, который соответствует последнему месяцу с данными
    last_month_condition = f"""
        AND NOT EXISTS (
            SELECT 1 
            FROM STECCOM_EXPENSES se 
            WHERE se.CONTRACT_ID = vi.CONTRACT_ID
              AND se.ICC_ID_IMEI = vi.IMEI
              AND TO_CHAR(se.INVOICE_DATE, 'YYYY-MM') = TO_CHAR(ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1), 'YYYY-MM')
        )
    """
    
    query = f"""
    SELECT 
        vi.IMEI,
        vi.SERVICE_ID,
        vi.CUSTOMER_NAME,
        vi.AGREEMENT_NUMBER,
        vi.CONTRACT_ID AS SUB_IRIDIUM,
        vi.CODE_1C,
        vi.START_DATE AS OPEN_DATE
    FROM V_IRIDIUM_SERVICES_INFO vi
    JOIN SERVICES s ON vi.SERVICE_ID = s.SERVICE_ID
    WHERE s.TYPE_ID = 9002
      AND vi.START_DATE IS NOT NULL
      AND (s.CLOSE_DATE IS NULL OR s.CLOSE_DATE >= ADD_MONTHS(SYSDATE, -24))
      AND (vi.STOP_DATE IS NULL OR vi.STOP_DATE >= ADD_MONTHS(SYSDATE, -24))
      {contract_condition}
      {imei_condition}
      {customer_condition}
      {code_1c_condition}
      {last_month_condition}
    ORDER BY vi.CUSTOMER_NAME, vi.CONTRACT_ID, vi.IMEI
    """
    
    try:
        return pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"Ошибка получения отчета LBS: {e}")
        return None
    finally:
        if conn:
            conn.close()
