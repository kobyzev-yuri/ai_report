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
    """Подсчет количества записей в файле (CSV или XLSX). Для CSV — pandas с разными sep/encoding, как в загрузчиках."""
    try:
        path = Path(file_path) if not isinstance(file_path, Path) else file_path
        if str(path).lower().endswith('.csv'):
            for enc in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                for sep in [';', '\t', ',']:
                    try:
                        df = pd.read_csv(path, sep=sep, encoding=enc, dtype=str, na_filter=False, quotechar='"')
                        if len(df.columns) > 1 and len(df) >= 0:
                            return len(df)
                    except Exception:
                        continue
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                return max(0, sum(1 for _ in f) - 1)
        elif str(path).lower().endswith('.xlsx'):
            df = pd.read_excel(path)
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

@st.cache_data(ttl=300)
def get_periods(_get_connection):
    """Получение списка периодов из BM_PERIOD (кэш 5 мин, меньше rerun при входе)."""
    conn = _get_connection()
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

@st.cache_data(ttl=300)
def get_revenue_periods(_get_connection):
    """Получение списка периодов из доходов (кэш 5 мин)."""
    conn = _get_connection()
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
    """Получение отчета по доходам. Без задвоения IMEI: только услуги с CLOSE_DATE > конец периода или NULL."""
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
    # Без задвоения: главная услуга активна в периоде ИЛИ есть другая услуга по IMEI с начислениями в периоде (напр. 9008)
    query = f"""SELECT v.* FROM V_REVENUE_FROM_INVOICES v
LEFT JOIN SERVICES s ON v.SERVICE_ID = s.SERVICE_ID
  AND (s.CLOSE_DATE IS NULL OR s.CLOSE_DATE > LAST_DAY(TO_DATE(v.PERIOD_YYYYMM||'-01','YYYY-MM-DD')))
WHERE {where}
  AND (
    s.SERVICE_ID IS NOT NULL
    OR EXISTS (
      SELECT 1 FROM BM_INVOICE_ITEM ii2
      JOIN SERVICES s2 ON ii2.SERVICE_ID = s2.SERVICE_ID
      JOIN BM_PERIOD p ON ii2.PERIOD_ID = p.PERIOD_ID
      WHERE s2.VSAT = v.IMEI
        AND TO_CHAR(p.START_DATE,'YYYY-MM') = v.PERIOD_YYYYMM
        AND (s2.CLOSE_DATE IS NULL OR s2.CLOSE_DATE > LAST_DAY(TO_DATE(v.PERIOD_YYYYMM||'-01','YYYY-MM-DD')))
    )
  )
ORDER BY v.PERIOD_YYYYMM DESC, v.CONTRACT_ID"""
    
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
    """Удаление дубликатов в ANALYTICS для периода: в каждой группе оставляем одну запись, остальные удаляем.
    Возвращает (success: bool, deleted_count: int, message: str).
    """
    df = get_analytics_duplicates(get_connection, period_id)
    if df is None or df.empty:
        return False, 0, "Не удалось получить список дубликатов или дубликатов нет."

    ids_to_delete = []
    for _, row in df.iterrows():
        aid_list = row.get('AID_LIST')
        if pd.isna(aid_list) or not str(aid_list).strip():
            continue
        aids = [x.strip() for x in str(aid_list).split(',') if x.strip()]
        if len(aids) <= 1:
            continue
        # Оставляем первый AID, остальные удаляем
        ids_to_delete.extend(aids[1:])

    if not ids_to_delete:
        return True, 0, "Дубликатов для удаления не найдено."

    conn = get_connection()
    if not conn:
        return False, 0, "Нет подключения к БД."

    try:
        cursor = conn.cursor()
        # Параметризованный запрос (AID в Oracle — число)
        try:
            aid_values = [int(float(aid)) for aid in ids_to_delete]
        except (ValueError, TypeError):
            aid_values = [aid for aid in ids_to_delete]
        placeholders = ','.join(':' + str(i + 1) for i in range(len(aid_values)))
        cursor.execute("DELETE FROM ANALYTICS WHERE AID IN (" + placeholders + ")", aid_values)
        deleted_count = cursor.rowcount
        conn.commit()
        cursor.close()
        return True, deleted_count, f"Удалено дубликатов: {deleted_count}."
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return False, 0, f"Ошибка при удалении: {e}"
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


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
