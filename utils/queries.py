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

# Укороченный набор колонок для экрана/CSV «Доходы». DDL V_REVENUE_FROM_INVOICES в Oracle не меняем — только список полей в этом SELECT.
# OPEN_DATE не из v.*: на БД без пересборки 05 колонки v.OPEN_DATE нет (ORA-00904). Скаляр по PK SERVICES — без второго JOIN к той же таблице (меньше нагрузка на сессию).
_REVENUE_UI_BEFORE_OPEN_DATE = (
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
)
_REVENUE_UI_AFTER_OPEN_DATE = (
    "ACC_CURRENCY_NAME",
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
    a = ", ".join(f"{alias}.{c}" for c in _REVENUE_UI_BEFORE_OPEN_DATE)
    b = ", ".join(f"{alias}.{c}" for c in _REVENUE_UI_AFTER_OPEN_DATE)
    open_dt = (
        f"(SELECT x.OPEN_DATE FROM SERVICES x WHERE x.SERVICE_ID = {alias}.SERVICE_ID) AS OPEN_DATE"
    )
    return f"{a}, {open_dt}, {b}"


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
) t WHERE t.rn = 1
ORDER BY t.BILL_MONTH DESC, t.CONTRACT_ID"""
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
) t WHERE t.rn = 1
ORDER BY t.BILL_MONTH DESC, t.CONTRACT_ID"""
    
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
    """Поиск дубликатов в ANALYTICS.

    Дубликаты — записи, у которых совпадают все бизнес-поля (кроме AID).
    Ключ совпадает с PARTITION BY в remove_analytics_duplicates.sql.
    """
    conn = get_connection()
    if not conn: return None

    # Полный ключ группировки: записи с разным SUB_PERIOD_ID / COUNTER_ID и т.п. — не дубликаты
    group_cols = """
        PERIOD_ID,
        SERVICE_ID,
        CUSTOMER_ID,
        ACCOUNT_ID,
        TYPE_ID,
        TARIFF_ID,
        TARIFFEL_ID,
        VSAT,
        MONEY,
        PRICE,
        TRAF,
        TOTAL_TRAF,
        CBYTE,
        INVOICE_ITEM_ID,
        FLAG,
        RESOURCE_TYPE_ID,
        CLASS_ID,
        CLASS_NAME,
        BLANK,
        COUNTER_ID,
        COUNTER_CF,
        ZONE_ID,
        THRESHOLD,
        SUB_TYPE_ID,
        SUB_PERIOD_ID,
        PMONEY,
        PARTNER_PERCENT
    """

    query = f"""
    SELECT
        COUNT(*) AS DUPLICATE_COUNT,
        LISTAGG(AID, ', ') WITHIN GROUP (ORDER BY AID) AS AID_LIST,
        {group_cols}
    FROM ANALYTICS
    WHERE PERIOD_ID = {period_id}
    GROUP BY {group_cols}
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


def get_sim_services_report(
    get_connection,
    customer_name_filter=None,
    account_id_filter=None,
    service_id_filter=None,
    iccid_filter=None,
    imsi_filter=None,
    msisdn_filter=None,
    only_active=True,
    exclude_steccom=True,
):
    """
    Услуги SIM / телефония Iridium (SERVICES.TYPE_ID=9001) с атрибутами SERVICES_EXT.

    DICT (TYPE_ID=9001): 123=iccid, 83=imsi, 87=pstn_number_rus,
    84=pstn_number, 86=pstn_number_data. Активные атрибуты: DATE_END IS NULL.
    """
    conn = get_connection()
    if not conn:
        return None

    conds = ["s.TYPE_ID = 9001"]
    if only_active:
        conds.append("s.CLOSE_DATE IS NULL")
    if exclude_steccom:
        conds.append("NVL(s.CUSTOMER_ID, -1) <> 521")
    if account_id_filter and str(account_id_filter).strip():
        val = str(account_id_filter).strip().replace("'", "''")
        conds.append(f"TO_CHAR(s.ACCOUNT_ID) LIKE '%{val}%'")
    if service_id_filter and str(service_id_filter).strip():
        val = str(service_id_filter).strip().replace("'", "''")
        conds.append(f"TO_CHAR(s.SERVICE_ID) LIKE '%{val}%'")
    if customer_name_filter and str(customer_name_filter).strip():
        val = str(customer_name_filter).strip().replace("'", "''")
        conds.append(f"UPPER(NVL(cust.CUSTOMER_NAME, '')) LIKE UPPER('%{val}%')")
    if iccid_filter and str(iccid_filter).strip():
        val = str(iccid_filter).strip().replace("'", "''")
        conds.append(f"NVL(se.ICCID, TO_CHAR(s.VSAT)) LIKE '%{val}%'")
    if imsi_filter and str(imsi_filter).strip():
        val = str(imsi_filter).strip().replace("'", "''")
        conds.append(f"se.IMSI LIKE '%{val}%'")
    if msisdn_filter and str(msisdn_filter).strip():
        val = str(msisdn_filter).strip().replace("'", "''")
        conds.append(
            "("
            f"se.PSTN_NUMBER_RUS LIKE '%{val}%' OR "
            f"se.PSTN_NUMBER LIKE '%{val}%' OR "
            f"se.PSTN_NUMBER_DATA LIKE '%{val}%'"
            ")"
        )

    where = " AND ".join(conds)
    query = f"""
    SELECT
        NVL(cust.CUSTOMER_NAME, '') AS CUSTOMER_NAME,
        s.ACCOUNT_ID,
        s.SERVICE_ID,
        s.DESCRIPTION,
        NVL(NULLIF(TRIM(se.ICCID), ''), TO_CHAR(s.VSAT)) AS ICCID,
        se.IMSI,
        se.PSTN_NUMBER_RUS,
        se.PSTN_NUMBER,
        se.PSTN_NUMBER_DATA
    FROM SERVICES s
    LEFT JOIN (
        SELECT
            SERVICE_ID,
            MAX(CASE WHEN DICT_ID = 123 AND DATE_END IS NULL THEN VALUE END) AS ICCID,
            MAX(CASE WHEN DICT_ID = 83  AND DATE_END IS NULL THEN VALUE END) AS IMSI,
            MAX(CASE WHEN DICT_ID = 87  AND DATE_END IS NULL THEN VALUE END) AS PSTN_NUMBER_RUS,
            MAX(CASE WHEN DICT_ID = 84  AND DATE_END IS NULL THEN VALUE END) AS PSTN_NUMBER,
            MAX(CASE WHEN DICT_ID = 86  AND DATE_END IS NULL THEN VALUE END) AS PSTN_NUMBER_DATA
        FROM SERVICES_EXT
        WHERE DICT_ID IN (123, 83, 87, 84, 86)
        GROUP BY SERVICE_ID
    ) se ON s.SERVICE_ID = se.SERVICE_ID
    LEFT JOIN (
        SELECT
            cc.CUSTOMER_ID,
            NVL(
                MAX(CASE WHEN cd.MNEMONIC = 'description' AND cc.CONTACT_DICT_ID = 23
                    THEN cc.VALUE END),
                TRIM(
                    NVL(MAX(CASE WHEN cd.MNEMONIC = 'last_name' AND cc.CONTACT_DICT_ID = 11
                        THEN cc.VALUE END), '') || ' ' ||
                    NVL(MAX(CASE WHEN cd.MNEMONIC = 'first_name' AND cc.CONTACT_DICT_ID = 11
                        THEN cc.VALUE END), '') || ' ' ||
                    NVL(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' AND cc.CONTACT_DICT_ID = 11
                        THEN cc.VALUE END), '')
                )
            ) AS CUSTOMER_NAME
        FROM BM_CUSTOMER_CONTACT cc
        LEFT JOIN BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID
        GROUP BY cc.CUSTOMER_ID
    ) cust ON s.CUSTOMER_ID = cust.CUSTOMER_ID
    WHERE {where}
    ORDER BY cust.CUSTOMER_NAME NULLS LAST, s.ACCOUNT_ID, s.SERVICE_ID
    """
    try:
        return pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"Ошибка получения отчёта SIM: {e}")
        return None
    finally:
        if conn:
            conn.close()


# Iridium: Open Port 9000, Voice 9001, SBD 9002/9014 + вспомогательные
_PASSPORT_IRIDIUM_TYPES = (9000, 9001, 9002, 9005, 9008, 9013, 9014)
_PASSPORT_SBD_TYPES = (9002, 9014)


def _fetch_iridium_sim_customers_billing(
    get_connection,
    only_active_sim=True,
    exclude_steccom=True,
    service_scope="all",
):
    """
    Клиенты BM7 с услугами Iridium. Одна строка на CUSTOMER_ID.

    service_scope:
      all — любой TYPE_ID из набора (опционально STATUS > 0);
      sbd — 9002/9014 (опционально STATUS > 0);
      open_port — 9000 и STATUS < 0;
      voice — 9001 и STATUS > 0.
    """
    conn = get_connection()
    if not conn:
        return None

    type_list = ",".join(str(t) for t in _PASSPORT_IRIDIUM_TYPES)
    sbd_list = ",".join(str(t) for t in _PASSPORT_SBD_TYPES)
    excl = "AND NVL(base.CUSTOMER_ID, -1) <> 521" if exclude_steccom else ""

    scope = (service_scope or "all").strip().lower()
    if scope == "sbd":
        match_types = f"TYPE_ID IN ({sbd_list})"
        match_status = "AND STATUS > 0" if only_active_sim else ""
    elif scope == "open_port":
        match_types = "TYPE_ID = 9000"
        match_status = "AND STATUS < 0"
    elif scope == "voice":
        match_types = "TYPE_ID = 9001"
        match_status = "AND STATUS > 0"
    else:
        match_types = f"TYPE_ID IN ({type_list})"
        match_status = "AND STATUS > 0" if only_active_sim else ""

    query = f"""
    WITH svc AS (
        SELECT
            s.CUSTOMER_ID,
            s.TYPE_ID,
            s.STATUS,
            s.CLOSE_DATE,
            NVL(bt.NAME, TO_CHAR(s.TYPE_ID)) AS TYPE_NAME
        FROM SERVICES s
        LEFT JOIN BM_TYPE bt ON bt.TYPE_ID = s.TYPE_ID
        WHERE s.TYPE_ID IN ({type_list})
          AND (s.LOGIN IS NULL OR s.LOGIN NOT LIKE '%-clone-%')
    ),
    base AS (
        SELECT DISTINCT svc.CUSTOMER_ID
        FROM svc
        WHERE {match_types}
          {match_status}
    ),
    types AS (
        SELECT
            t.CUSTOMER_ID,
            LISTAGG(t.TYPE_NAME, ', ')
                WITHIN GROUP (ORDER BY t.TYPE_ID, t.TYPE_NAME) AS SERVICE_TYPES
        FROM (
            SELECT DISTINCT svc.CUSTOMER_ID, svc.TYPE_ID, svc.TYPE_NAME
            FROM svc
            INNER JOIN base ON base.CUSTOMER_ID = svc.CUSTOMER_ID
        ) t
        GROUP BY t.CUSTOMER_ID
    ),
    names AS (
        SELECT
            cc.CUSTOMER_ID,
            NVL(
                MAX(CASE WHEN cd.MNEMONIC = 'description' AND cc.CONTACT_DICT_ID = 23
                    THEN cc.VALUE END),
                TRIM(
                    NVL(MAX(CASE WHEN cd.MNEMONIC = 'last_name' THEN cc.VALUE END), '') || ' ' ||
                    NVL(MAX(CASE WHEN cd.MNEMONIC = 'first_name' THEN cc.VALUE END), '') || ' ' ||
                    NVL(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' THEN cc.VALUE END), '')
                )
            ) AS CUSTOMER_NAME,
            TRIM(
                NVL(MAX(CASE WHEN cd.MNEMONIC = 'last_name' THEN cc.VALUE END), '') || ' ' ||
                NVL(MAX(CASE WHEN cd.MNEMONIC = 'first_name' THEN cc.VALUE END), '') || ' ' ||
                NVL(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' THEN cc.VALUE END), '')
            ) AS PERSON_FIO,
            NVL(
                MAX(CASE WHEN cd.MNEMONIC = 'director' THEN cc.VALUE END),
                MAX(CASE WHEN cd.MNEMONIC = 'short_fio' THEN cc.VALUE END)
            ) AS DIRECTOR_FIO
        FROM BM_CUSTOMER_CONTACT cc
        LEFT JOIN BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID
        INNER JOIN base ON base.CUSTOMER_ID = cc.CUSTOMER_ID
        GROUP BY cc.CUSTOMER_ID
    )
    SELECT
        base.CUSTOMER_ID,
        (
            SELECT TRIM(oi.EXT_ID) FROM OUTER_IDS oi
             WHERE oi.ID = base.CUSTOMER_ID
               AND UPPER(TRIM(oi.TBL)) = 'CUSTOMERS'
               AND oi.EXT_ID IS NOT NULL
               AND TRIM(oi.EXT_ID) IS NOT NULL
               AND ROWNUM = 1
        ) AS CODE_1C,
        NVL(names.CUSTOMER_NAME, '') AS CUSTOMER_NAME,
        NULLIF(TRIM(names.PERSON_FIO), '') AS PERSON_FIO,
        NULLIF(TRIM(names.DIRECTOR_FIO), '') AS DIRECTOR_FIO,
        types.SERVICE_TYPES
    FROM base
    LEFT JOIN names ON names.CUSTOMER_ID = base.CUSTOMER_ID
    LEFT JOIN types ON types.CUSTOMER_ID = base.CUSTOMER_ID
    WHERE 1=1
      {excl}
    ORDER BY names.CUSTOMER_NAME NULLS LAST, base.CUSTOMER_ID
    """
    try:
        return pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"Ошибка выборки клиентов Iridium: {e}")
        return None
    finally:
        if conn:
            conn.close()


def get_passport_gaps_report(
    get_connection,
    only_missing_passport=True,
    only_persons=True,
    only_with_email=False,
    only_active_sim=True,
    exclude_steccom=True,
    service_scope="all",
    customer_name_filter=None,
    code_1c_filter=None,
):
    """
    Клиенты с услугами Iridium + паспорт/email из BPM Account.
    service_scope: all | sbd | open_port | voice.
    """
    from utils.crm_connection import fetch_crm_accounts

    def _s(v):
        """Безопасная строка: NaN/None → ''."""
        if v is None:
            return ""
        try:
            if pd.isna(v):
                return ""
        except (TypeError, ValueError):
            pass
        return str(v).strip()

    def _looks_like_fio(v: str) -> bool:
        """Отсечь мусор в поле director (коды, 'дир', …)."""
        if not v or len(v) < 5:
            return False
        letters = sum(ch.isalpha() for ch in v)
        return letters >= 4 and not v.isdigit()

    def _letter_fio(
        *,
        is_person_flag: bool,
        customer_name: str,
        crm_name: str,
        person_fio: str,
        director_contact: str,
        primary_contact: str,
        billing_director: str,
    ):
        """
        ФЛ: название контрагента = ФИО.
        ЮЛ: контакты CRM (ген. директор / директор), иначе PrimaryContact / BM director.
        """
        if is_person_flag:
            for cand in (customer_name, crm_name, person_fio):
                if cand and _looks_like_fio(cand):
                    return cand
            return customer_name or crm_name or person_fio or None

        for cand in (director_contact, primary_contact, billing_director):
            if cand and _looks_like_fio(cand):
                return cand
        return None

    billing = _fetch_iridium_sim_customers_billing(
        get_connection,
        only_active_sim=only_active_sim,
        exclude_steccom=exclude_steccom,
        service_scope=service_scope,
    )
    if billing is None:
        return None
    if billing.empty:
        return billing

    try:
        crm = fetch_crm_accounts()
    except Exception as e:
        st.error(f"Ошибка CRM (ODBC Account): {e}")
        return None

    rows = []
    for _, r in billing.iterrows():
        code = _s(r.get("CODE_1C"))
        acc = crm.get(code) if code else None
        has_passport = bool(acc and acc.get("has_passport"))
        is_person = bool(acc and acc.get("is_person"))
        is_legal = bool(acc and acc.get("is_legal"))
        # если CRM нет — эвристика: PERSON_FIO заполнен → скорее ФЛ
        if not acc and _s(r.get("PERSON_FIO")):
            is_person = True
        email = (acc or {}).get("email")
        crm_name = _s((acc or {}).get("Name"))
        billing_name = _s(r.get("CUSTOMER_NAME"))
        person_fio = _s(r.get("PERSON_FIO"))
        director_fio = _s(r.get("DIRECTOR_FIO"))
        director_contact = _s((acc or {}).get("director_contact_name"))
        director_job = _s((acc or {}).get("director_contact_job"))
        crm_contact = _s((acc or {}).get("primary_contact_name"))
        crm_job = _s((acc or {}).get("primary_contact_job")) or director_job
        letter_fio = _letter_fio(
            is_person_flag=is_person,
            customer_name=billing_name,
            crm_name=crm_name,
            person_fio=person_fio,
            director_contact=director_contact,
            primary_contact=crm_contact,
            billing_director=director_fio if _looks_like_fio(director_fio) else "",
        )
        subject = "ФЛ" if is_person else ("ЮЛ" if is_legal else ("CRM" if acc else "нет в CRM"))

        if only_missing_passport and has_passport:
            continue
        if only_persons and acc and not is_person:
            continue
        if only_persons and not acc:
            pass
        if only_with_email and not email:
            continue
        if customer_name_filter and str(customer_name_filter).strip():
            needle = str(customer_name_filter).strip().upper()
            hay = f"{billing_name} {crm_name} {letter_fio or ''}".upper()
            if needle not in hay:
                continue
        if code_1c_filter and str(code_1c_filter).strip():
            if str(code_1c_filter).strip() not in code:
                continue

        rows.append(
            {
                "CUSTOMER_ID": r.get("CUSTOMER_ID"),
                "CODE_1C": code or None,
                "CUSTOMER_NAME": (billing_name or crm_name) or None,
                "LETTER_FIO": letter_fio,
                "PERSON_FIO": person_fio or None,
                "DIRECTOR_FIO": director_fio if _looks_like_fio(director_fio) else None,
                "CRM_CONTACT": (director_contact or crm_contact) or None,
                "CRM_JOB_TITLE": crm_job or None,
                "SUBJECT_TYPE": subject,
                "SERVICE_TYPES": _s(r.get("SERVICE_TYPES")) or None,
                "EMAIL": email,
                "HAS_PASSPORT": "Y" if has_passport else "N",
                "PASSPORT_ID": (acc or {}).get("passport_id"),
                "ISSUED_BY": _s((acc or {}).get("IssuedBy")) or None,
                "ISSUED_DATE": _s((acc or {}).get("IssuedDate")) or None,
                "CRM_NAME": crm_name or None,
                "PHONE": _s((acc or {}).get("Phone")) or None,
                "MANAGER_FIO": _s((acc or {}).get("manager_fio")) or None,
            }
        )

    return pd.DataFrame(rows)
