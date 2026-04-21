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

# Укороченный набор колонок V_REVENUE_FROM_INVOICES для Streamlit «Доходы» (полное представление в Oracle шире).
_REVENUE_UI_COLUMNS = (
    "SERVICE_ID",
    "CONTRACT_ID",
    "IMEI",
    "ORGANIZATION_NAME",
    "CODE_1C",
    "ACCOUNT_ID",
    "CUSTOMER_ID",
    "AGREEMENT_NUMBER",
    "ORDER_NUMBER",
    "INFO_SERVICE_ID",
    "TARIFF_ID",
    "IS_SUSPENDED",
    "OPEN_DATE",
    "CURRENCY_ID",
    "CURRENCY_NAME",
    "CURRENCY_CODE",
    "CURRENCY_MNEMONIC",
    "ACC_CURRENCY_NAME",
    "ACC_CURRENCY_CODE",
    "ACC_CURRENCY_MNEMONIC",
    "PERIOD_ID",
    "BILL_MONTH",
    "REVENUE_SBD_TRAFFIC",
    "REVENUE_SBD_TRAFFIC_SBD1",
    "REVENUE_SBD_TRAFFIC_SBD10",
    "REVENUE_SBD_ABON",
    "REVENUE_SBD_TOTAL",
    "REVENUE_SUSPEND_ABON",
    "REVENUE_MONITORING_ABON",
    "REVENUE_MONITORING_BLOCK_ABON",
    "REVENUE_MSG_ABON",
    "REVENUE_TOTAL",
    "TARIFF_SINGLE_PAYMENT_MONEY",
    "REVENUE_CONNECTION_RUB",
    "REVENUE_TOTAL_ACC_CURRENCY",
    "INVOICE_ITEMS_COUNT",
    "REVENUE_ANOMALY_NOTE",
)


def _revenue_ui_select_sql(alias: str = "v") -> str:
    return ", ".join(f"{alias}.{c}" for c in _REVENUE_UI_COLUMNS)


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
    
    # Фильтр по IMEI (VSAT в БД может быть NUMBER)
    imei_condition = ""
    if imei_filter and imei_filter.strip():
        imei_value = imei_filter.strip().replace("'", "''")
        imei_condition = f"AND TRIM(TO_CHAR(v.IMEI)) = '{imei_value}'"
    
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
        raw_imei = imei_filter.strip()
        imei_sql = raw_imei.replace("'", "''")
        # Сначала дешёвый поиск SERVICE_ID по VSAT; если есть — только IN (без OR по всему view).
        sid_list = []
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT SERVICE_ID FROM SERVICES WHERE TRIM(TO_CHAR(VSAT)) = :1",
                [raw_imei],
            )
            for row in cur.fetchall():
                if row and row[0] is not None:
                    sid_list.append(int(row[0]))
            cur.close()
        except Exception:
            sid_list = []
        if sid_list:
            conds.append("v.SERVICE_ID IN (" + ",".join(str(x) for x in sid_list) + ")")
        else:
            conds.append(f"TRIM(TO_CHAR(v.IMEI)) = '{imei_sql}'")
    if customer_name_filter: 
        val = customer_name_filter.strip().replace("'", "''")
        conds.append(
            f"UPPER(COALESCE(v.CUSTOMER_NAME, v.ORGANIZATION_NAME, '')) LIKE UPPER('%{val}%')"
        )
    if code_1c_filter: 
        conds.append(f"v.CODE_1C LIKE '%{code_1c_filter.strip()}%'")
    
    where = " AND ".join(conds) if conds else "1=1"
    # Без задвоения: 1) показываем строку если главная услуга активна ИЛИ есть активная услуга с начислениями в периоде;
    # 2) на (IMEI, CONTRACT_ID, PERIOD) оставляем одну строку — приоритет у строки, где главная услуга активна (не клон)
    #
    # Для одного выбранного месяца EXISTS по BM_INVOICE_ITEM заменён на WITH+JOIN (один проход по счетам за период),
    # иначе при «только период» запрос зависал на коррелированном EXISTS по всем строкам view.
    period_esc = None
    if period_filter and period_filter != "All Periods":
        period_esc = period_filter.replace("'", "''")
    exists_sql = f"""
      EXISTS (
        SELECT 1 FROM BM_INVOICE_ITEM ii2
        JOIN SERVICES s2 ON ii2.SERVICE_ID = s2.SERVICE_ID
        JOIN BM_PERIOD p ON ii2.PERIOD_ID = p.PERIOD_ID
        WHERE NVL(NULLIF(TRIM(TO_CHAR(s2.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s2.LOGIN)), '')) = TRIM(TO_CHAR(v.IMEI))
          AND TO_CHAR(p.START_DATE,'YYYY-MM') = v.PERIOD_YYYYMM
          AND (s2.CLOSE_DATE IS NULL OR s2.CLOSE_DATE > LAST_DAY(TO_DATE(v.PERIOD_YYYYMM||'-01','YYYY-MM-DD')))
      )"""
    if period_esc:
        query = f"""WITH inv_cov AS (
  SELECT DISTINCT
    NVL(NULLIF(TRIM(TO_CHAR(s2.VSAT)), ''), NULLIF(TRIM(TO_CHAR(s2.LOGIN)), '')) AS imei_key,
    TO_CHAR(p.START_DATE,'YYYY-MM') AS yyyymm
  FROM BM_INVOICE_ITEM ii2
  JOIN SERVICES s2 ON ii2.SERVICE_ID = s2.SERVICE_ID
  JOIN BM_PERIOD p ON ii2.PERIOD_ID = p.PERIOD_ID
  WHERE TO_CHAR(p.START_DATE,'YYYY-MM') = '{period_esc}'
    AND (s2.CLOSE_DATE IS NULL OR s2.CLOSE_DATE > LAST_DAY(TO_DATE(TO_CHAR(p.START_DATE,'YYYY-MM')||'-01','YYYY-MM-DD')))
)
SELECT * FROM (
  SELECT {_revenue_ui_select_sql('v')},
    ROW_NUMBER() OVER (
      PARTITION BY v.IMEI, v.CONTRACT_ID, v.PERIOD_YYYYMM
      ORDER BY CASE WHEN s.SERVICE_ID IS NOT NULL THEN 0 ELSE 1 END, v.SERVICE_ID
    ) AS rn
  FROM V_REVENUE_FROM_INVOICES v
  LEFT JOIN SERVICES s ON v.SERVICE_ID = s.SERVICE_ID
    AND (s.CLOSE_DATE IS NULL OR s.CLOSE_DATE > LAST_DAY(TO_DATE(v.PERIOD_YYYYMM||'-01','YYYY-MM-DD')))
  LEFT JOIN inv_cov ic ON ic.imei_key = TRIM(TO_CHAR(v.IMEI)) AND ic.yyyymm = v.PERIOD_YYYYMM
  WHERE {where}
    AND (
      s.SERVICE_ID IS NOT NULL
      OR ic.yyyymm IS NOT NULL
    )
) WHERE rn = 1
ORDER BY BILL_MONTH DESC, CONTRACT_ID"""
    else:
        query = f"""SELECT * FROM (
  SELECT {_revenue_ui_select_sql('v')},
    ROW_NUMBER() OVER (
      PARTITION BY v.IMEI, v.CONTRACT_ID, v.PERIOD_YYYYMM
      ORDER BY CASE WHEN s.SERVICE_ID IS NOT NULL THEN 0 ELSE 1 END, v.SERVICE_ID
    ) AS rn
  FROM V_REVENUE_FROM_INVOICES v
  LEFT JOIN SERVICES s ON v.SERVICE_ID = s.SERVICE_ID
    AND (s.CLOSE_DATE IS NULL OR s.CLOSE_DATE > LAST_DAY(TO_DATE(v.PERIOD_YYYYMM||'-01','YYYY-MM-DD')))
  WHERE {where}
    AND (
      s.SERVICE_ID IS NOT NULL
      OR {exists_sql}
    )
) WHERE rn = 1
ORDER BY BILL_MONTH DESC, CONTRACT_ID"""
    
    try:
        df = pd.read_sql_query(query, conn)
        if df is not None and not df.empty and "RN" in df.columns:
            df = df.drop(columns=["RN"])
        return df
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
        imei_val = imei_filter.strip().replace("'", "''")
        conds.append(f"TRIM(TO_CHAR(v.IMEI)) = '{imei_val}'")
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


def get_lbs_services_report(
    get_connection,
    period_filter=None,
    contract_id_filter=None,
    imei_filter=None,
    customer_name_filter=None,
    code_1c_filter=None,
    exclude_steccom=True,
    only_in_invoice=False,
    only_active=False,
):
    """Отчет по LBS услугам (TYPE_ID=9002, тарифы BM_TARIFF.NAME like %LBS%). Без сумм: атрибуты сервиса + признак попадания в СФ."""
    conn = get_connection()
    if not conn:
        return None

    conds = []
    # Пересечение интервала услуги [OPEN_DATE, CLOSE_DATE] с календарным месяцем period_filter (YYYY-MM):
    # OPEN_DATE <= конец месяца AND (CLOSE_DATE IS NULL OR CLOSE_DATE >= начало месяца)
    if period_filter and str(period_filter).strip() and str(period_filter).strip() != "All Periods":
        p = str(period_filter).strip().replace("'", "''")
        conds.append(
            f"v.OPEN_DATE <= LAST_DAY(TO_DATE('{p}-01','YYYY-MM-DD')) "
            f"AND (v.CLOSE_DATE IS NULL OR v.CLOSE_DATE >= TRUNC(TO_DATE('{p}-01','YYYY-MM-DD')))"
        )
    if contract_id_filter and str(contract_id_filter).strip():
        val = str(contract_id_filter).strip().replace("'", "''")
        conds.append(f"v.CONTRACT_ID LIKE '%{val}%'")
    if imei_filter and str(imei_filter).strip():
        val = str(imei_filter).strip().replace("'", "''")
        conds.append(f"TRIM(TO_CHAR(v.IMEI)) = '{val}'")
    if customer_name_filter and str(customer_name_filter).strip():
        val = str(customer_name_filter).strip().replace("'", "''")
        conds.append("UPPER(COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '')) LIKE UPPER('%" + val + "%')")
    if code_1c_filter and str(code_1c_filter).strip():
        val = str(code_1c_filter).strip().replace("'", "''")
        conds.append(f"v.CODE_1C LIKE '%{val}%'")
    if exclude_steccom:
        conds.append("NVL(v.CUSTOMER_ID, -1) <> 521")
    if only_in_invoice:
        conds.append("v.IN_INVOICE = 'Y'")
    if only_active:
        conds.append("v.CLOSE_DATE IS NULL")

    where = " AND ".join(conds) if conds else "1=1"
    query = f"""
    SELECT
        v.SERVICE_ID,
        v.CONTRACT_ID,
        v.IMEI,
        COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '') AS CUSTOMER_NAME,
        v.CODE_1C,
        v.AGREEMENT_NUMBER,
        v.ORDER_NUMBER,
        v.TARIFF_ID,
        v.TARIFF_NAME,
        v.OPEN_DATE,
        v.CLOSE_DATE,
        v.IN_INVOICE,
        v.FIRST_INVOICE_PERIOD_ID,
        v.FIRST_INVOICE_PERIOD_YYYYMM,
        v.ACCOUNT_ID,
        v.CUSTOMER_ID,
        v.STATUS,
        v.ACTUAL_STATUS
    FROM V_LBS_SERVICES v
    WHERE {where}
    ORDER BY v.IN_INVOICE DESC, v.FIRST_INVOICE_PERIOD_ID NULLS LAST, v.OPEN_DATE DESC NULLS LAST, v.CONTRACT_ID, v.IMEI
    """
    try:
        return pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"Ошибка получения отчета по LBS услугам: {e}")
        return None
    finally:
        if conn:
            conn.close()
