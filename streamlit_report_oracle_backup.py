#!/usr/bin/env python3
"""
Streamlit отчет по превышению трафика Iridium M2M
Версия для Oracle Database (backup)
"""

import os
# Исправление проблемы с protobuf - ДОЛЖНО БЫТЬ ПЕРВЫМ, до любых импортов
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

import streamlit as st
import pandas as pd
import cx_Oracle
from datetime import datetime
import io
from pathlib import Path
import warnings
from auth_db import (
    init_db, authenticate_user, create_user, list_users, 
    delete_user, is_superuser
)

# Подавляем предупреждение pandas о cx_Oracle (работает корректно)
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')

# Попытка загрузить config.env если переменные окружения не установлены
def load_config_env():
    """Загрузка config.env если переменные окружения не установлены"""
    if not os.getenv('ORACLE_PASSWORD'):
        config_file = Path(__file__).parent / 'config.env'
        if config_file.exists():
            with open(config_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip().strip('"\'')
                        if key.startswith('ORACLE_') and not os.getenv(key):
                            os.environ[key] = value

# Загружаем config.env если нужно
load_config_env()

# Конфигурация базы данных
# Загружается из config.env через run_streamlit.sh или автоматически из config.env
ORACLE_USER = os.getenv('ORACLE_USER')
ORACLE_PASSWORD = os.getenv('ORACLE_PASSWORD')
ORACLE_HOST = os.getenv('ORACLE_HOST')
ORACLE_PORT = int(os.getenv('ORACLE_PORT', '1521'))

# Проверка обязательных параметров
if not all([ORACLE_USER, ORACLE_PASSWORD, ORACLE_HOST]):
    st.error("❌ Ошибка: Не установлены переменные окружения ORACLE_USER, ORACLE_PASSWORD и ORACLE_HOST")
    st.error("Установите их в config.env или через переменные окружения")
    st.stop()
ORACLE_SID = os.getenv('ORACLE_SID')
# Если задан ORACLE_SERVICE, используем его, иначе ORACLE_SID
ORACLE_SERVICE = os.getenv('ORACLE_SERVICE') or os.getenv('ORACLE_SID')


def get_connection():
    """Создание подключения к Oracle"""
    try:
        if not ORACLE_PASSWORD:
            st.error("⚠️ Пароль не установлен! Убедитесь, что config.env загружен через run_streamlit.sh")
            return None
        # Используем SID если задан ORACLE_SID, иначе SERVICE_NAME
        if os.getenv('ORACLE_SID'):
            dsn = cx_Oracle.makedsn(
                ORACLE_HOST,
                ORACLE_PORT,
                sid=ORACLE_SID
            )
        else:
            dsn = cx_Oracle.makedsn(
                ORACLE_HOST,
                ORACLE_PORT,
                service_name=ORACLE_SERVICE
            )
        conn = cx_Oracle.connect(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=dsn
        )
        return conn
    except Exception as e:
        st.error(f"Ошибка подключения к Oracle: {e}")
        st.info("Проверьте конфигурацию подключения к базе данных")
        return None


def count_file_records(file_path):
    """Подсчет количества записей в файле (CSV или XLSX)"""
    try:
        file_ext = Path(file_path).suffix.lower()
        
        if not Path(file_path).exists():
            return None
        
        if file_ext == '.xlsx':
            try:
                df = pd.read_excel(file_path, dtype=str, na_filter=False, engine='openpyxl')
                df = df.dropna(how='all')
                df = df[~df.apply(lambda x: x.astype(str).str.strip().eq('').all(), axis=1)]
                return len(df)
            except Exception as e:
                try:
                    df = pd.read_excel(file_path, dtype=str, na_filter=False)
                    df = df.dropna(how='all')
                    return len(df)
                except:
                    return None
        else:
            # CSV файл
            try:
                df = pd.read_csv(file_path, dtype=str, na_filter=False)
                return len(df)
            except Exception as e:
                # Пробуем разные кодировки
                for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                    try:
                        df = pd.read_csv(file_path, dtype=str, na_filter=False, encoding=encoding)
                        return len(df)
                    except:
                        continue
                return None
    except Exception as e:
        return None


def get_records_in_db(file_name, table_name='SPNET_TRAFFIC'):
    """Получить количество записей в базе для файла"""
    conn = get_connection()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM {} 
            WHERE UPPER(SOURCE_FILE) = UPPER(:1)
        """.format(table_name), (file_name,))
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except Exception as e:
        return None
    finally:
        conn.close()


def get_main_report(period_filter=None, plan_filter=None, contract_id_filter=None, imei_filter=None, customer_name_filter=None, code_1c_filter=None):
    """Получение основного отчета"""
    conn = get_connection()
    if not conn:
        return None
    
    # Фильтр по периодам
    period_condition = ""
    if period_filter and period_filter != "All Periods":
        # Фильтруем по FINANCIAL_PERIOD (Отчетный период), который на месяц меньше BILL_MONTH
        period_condition = f"AND v.FINANCIAL_PERIOD = '{period_filter}'"
    
    # Фильтр по тарифам
    plan_condition = ""
    if plan_filter and plan_filter != "All Plans":
        plan_condition = f"AND v.PLAN_NAME = '{plan_filter}'"
    
    # Фильтр по CONTRACT_ID (SUB-*)
    contract_condition = ""
    if contract_id_filter and contract_id_filter.strip():
        # Экранируем одинарные кавычки для безопасности
        contract_value = contract_id_filter.strip().replace("'", "''")
        contract_condition = f"AND v.CONTRACT_ID LIKE '%{contract_value}%'"
    
    # Фильтр по IMEI
    imei_condition = ""
    if imei_filter and imei_filter.strip():
        # Экранируем одинарные кавычки для безопасности
        imei_value = imei_filter.strip().replace("'", "''")
        imei_condition = f"AND v.IMEI = '{imei_value}'"
    
    # Фильтр по названию клиента
    customer_condition = ""
    if customer_name_filter and customer_name_filter.strip():
        # Экранируем одинарные кавычки для безопасности
        customer_value = customer_name_filter.strip().replace("'", "''")
        customer_condition = f"AND UPPER(COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '')) LIKE UPPER('%{customer_value}%')"
    
    # Фильтр по коду 1С
    code_1c_condition = ""
    if code_1c_filter and code_1c_filter.strip():
        # Экранируем одинарные кавычки для безопасности
        code_1c_value = code_1c_filter.strip().replace("'", "''")
        code_1c_condition = f"AND v.CODE_1C LIKE '%{code_1c_value}%'"
    
    # Формируем базовый запрос (без параметров в WHERE)
    # В Oracle нужно экранировать % в строковых литералах как %%
    # Добавляем дополнительный JOIN для получения названия клиента по CODE_1C, если оно отсутствует
    base_query = """
    SELECT 
        v.FINANCIAL_PERIOD AS "Отчетный Период",
        v.BILL_MONTH AS "Bill Month",
        v.IMEI AS "IMEI",
        v.CONTRACT_ID AS "Contract ID",
        -- Доп. поля из биллинга (после Contract ID)
        -- Если название клиента отсутствует в представлении, получаем его по SERVICE_ID, IMEI (через SERVICES_EXT) или CODE_1C
        COALESCE(
            v.ORGANIZATION_NAME, 
            v.CUSTOMER_NAME,
            service_cust_info.CUSTOMER_NAME,
            imei_service_ext_info.CUSTOMER_NAME,
            imei_service_info.CUSTOMER_NAME,
            cust_info.CUSTOMER_NAME,
            ''
        ) AS "Organization/Person",
        -- CODE_1C: сначала из представления, потом по SERVICE_ID, потом по IMEI через SERVICES_EXT, потом по CODE_1C
        COALESCE(
            v.CODE_1C,
            service_cust_info.CODE_1C,
            imei_service_ext_info.CODE_1C,
            imei_service_info.CODE_1C,
            cust_info.CODE_1C
        ) AS "Code 1C",
        -- SERVICE_ID: сначала из представления, потом по IMEI через SERVICES_EXT (для swap случаев)
        COALESCE(
            v.SERVICE_ID,
            imei_service_ext_info.SERVICE_ID,
            imei_service_info.SERVICE_ID
        ) AS "Service ID",
        -- AGREEMENT_NUMBER: если SERVICE_ID есть, сначала из service_cust_info (надежнее), потом из представления и других источников
        COALESCE(
            service_cust_info.AGREEMENT_NUMBER,  -- Приоритет: если SERVICE_ID найден, берем из прямого JOIN
            v.AGREEMENT_NUMBER,                  -- Затем из представления
            imei_service_ext_info.AGREEMENT_NUMBER,  -- Затем по IMEI через SERVICES_EXT
            imei_service_info.AGREEMENT_NUMBER      -- Затем по IMEI через SERVICES.VSAT
        ) AS "Agreement #",
        CASE 
            WHEN v.ACTIVATION_DATE IS NOT NULL THEN TO_CHAR(v.ACTIVATION_DATE, 'YYYY-MM-DD')
            ELSE NULL
        END AS "Activation Date",
        COALESCE(v.PLAN_NAME, '') AS "Plan Name",
        COALESCE(v.STECCOM_PLAN_NAME_MONTHLY, '') AS "Plan Monthly",
        COALESCE(v.STECCOM_PLAN_NAME_SUSPENDED, '') AS "Plan Suspended",
        -- Разделение трафика и событий
        ROUND(v.TRAFFIC_USAGE_BYTES / 1000, 2) AS "Traffic Usage (KB)",
        v.MAILBOX_EVENTS AS "Mailbox Events",
        v.REGISTRATION_EVENTS AS "Registration Events",
        -- Превышения
        v.OVERAGE_KB AS "Overage (KB)",
        v.CALCULATED_OVERAGE AS "Calculated Overage ($)",
        -- Сумма из отчета SPNet (стоимость трафика из детализации)
        NVL(v.SPNET_TOTAL_AMOUNT, 0) AS "Total Amount ($)",
        -- Fees из STECCOM_EXPENSES (убрали префикс "Fee:")
        NVL(v.FEE_ACTIVATION_FEE, 0) AS "Activation Fee",
        NVL(v.FEE_ADVANCE_CHARGE, 0) AS "Advance Charge",
        NVL(v.FEE_ADVANCE_CHARGE_PREVIOUS_MONTH, 0) AS "Advance Charge Previous Month",
        NVL(v.FEE_CREDIT, 0) AS "Credit",
        NVL(v.FEE_CREDITED, 0) AS "Credited",
        NVL(v.FEE_PRORATED, 0) AS "Prorated"
    FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
    -- Дополнительный JOIN для получения клиента по SERVICE_ID напрямую из SERVICES
    -- Это нужно для случаев, когда нет CONTRACT_ID в V_IRIDIUM_SERVICES_INFO или IMEI был перенесен на другой контракт
    -- SERVICE_ID - самый надежный способ найти клиента, так как он уникален
    LEFT JOIN (
        SELECT 
            s.SERVICE_ID,
            MAX(oi.EXT_ID) AS CODE_1C,
            MAX(a.DESCRIPTION) AS AGREEMENT_NUMBER,
            COALESCE(
                MAX(CASE WHEN cd.MNEMONIC = 'description' AND cc.CONTACT_DICT_ID = 23 THEN cc.VALUE END),
                TRIM(
                    NVL(MAX(CASE WHEN cd.MNEMONIC = 'last_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' ||
                    NVL(MAX(CASE WHEN cd.MNEMONIC = 'first_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' ||
                    NVL(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '')
                )
            ) AS CUSTOMER_NAME
        FROM SERVICES s
        JOIN CUSTOMERS c ON s.CUSTOMER_ID = c.CUSTOMER_ID
        JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
        LEFT JOIN OUTER_IDS oi 
            ON oi.ID = c.CUSTOMER_ID
           AND UPPER(TRIM(oi.TBL)) = 'CUSTOMERS'
        LEFT JOIN BM_CUSTOMER_CONTACT cc ON cc.CUSTOMER_ID = c.CUSTOMER_ID
        LEFT JOIN BM_CONTACT_DICT cd ON cd.CONTACT_DICT_ID = cc.CONTACT_DICT_ID
        -- Убираем фильтр TYPE_ID, так как для любого найденного SERVICE_ID должен быть ACCOUNT_ID
        GROUP BY s.SERVICE_ID
    ) service_cust_info ON service_cust_info.SERVICE_ID = v.SERVICE_ID
    -- Дополнительный JOIN для получения SERVICE_ID по IMEI через SERVICES_EXT (когда SERVICE_ID в представлении NULL)
    -- Для swap IMEI: IMEI может храниться в SERVICES_EXT.VALUE, а не в SERVICES.VSAT
    LEFT JOIN (
        SELECT 
            se.VALUE AS IMEI,
            se.SERVICE_ID,
            MAX(oi.EXT_ID) AS CODE_1C,
            MAX(a.DESCRIPTION) AS AGREEMENT_NUMBER,
            COALESCE(
                MAX(CASE WHEN cd.MNEMONIC = 'description' AND cc.CONTACT_DICT_ID = 23 THEN cc.VALUE END),
                TRIM(
                    NVL(MAX(CASE WHEN cd.MNEMONIC = 'last_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' ||
                    NVL(MAX(CASE WHEN cd.MNEMONIC = 'first_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' ||
                    NVL(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '')
                )
            ) AS CUSTOMER_NAME
        FROM SERVICES_EXT se
        JOIN SERVICES s ON se.SERVICE_ID = s.SERVICE_ID
        JOIN CUSTOMERS c ON s.CUSTOMER_ID = c.CUSTOMER_ID
        JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
        LEFT JOIN OUTER_IDS oi 
            ON oi.ID = c.CUSTOMER_ID
           AND UPPER(TRIM(oi.TBL)) = 'CUSTOMERS'
        LEFT JOIN BM_CUSTOMER_CONTACT cc ON cc.CUSTOMER_ID = c.CUSTOMER_ID
        LEFT JOIN BM_CONTACT_DICT cd ON cd.CONTACT_DICT_ID = cc.CONTACT_DICT_ID
        WHERE se.VALUE IS NOT NULL
          AND se.DATE_END IS NULL  -- Только активные записи
          -- Убираем фильтр TYPE_ID, так как для любого найденного SERVICE_ID должен быть ACCOUNT_ID
        GROUP BY se.VALUE, se.SERVICE_ID
    ) imei_service_ext_info ON TRIM(imei_service_ext_info.IMEI) = TRIM(v.IMEI)
    -- Дополнительный JOIN для получения SERVICE_ID и клиента по IMEI (VSAT) для случаев swap IMEI
    -- Когда IMEI был перенесен на другой контракт, SERVICE_ID в представлении может быть NULL
    -- Ищем активный сервис по IMEI (VSAT) с приоритетом активным (STATUS=10) и более новым
    LEFT JOIN (
        SELECT 
            s_ranked.VSAT AS IMEI,
            s_ranked.SERVICE_ID,
            s_ranked.CODE_1C,
            s_ranked.AGREEMENT_NUMBER,
            s_ranked.CUSTOMER_NAME
        FROM (
            SELECT 
                s.VSAT,
                s.SERVICE_ID,
                MAX(oi.EXT_ID) AS CODE_1C,
                MAX(a.DESCRIPTION) AS AGREEMENT_NUMBER,
                COALESCE(
                    MAX(CASE WHEN cd.MNEMONIC = 'description' AND cc.CONTACT_DICT_ID = 23 THEN cc.VALUE END),
                    TRIM(
                        NVL(MAX(CASE WHEN cd.MNEMONIC = 'last_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' ||
                        NVL(MAX(CASE WHEN cd.MNEMONIC = 'first_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' ||
                        NVL(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '')
                    )
                ) AS CUSTOMER_NAME,
                MAX(s.STATUS) AS STATUS,
                MAX(s.CREATE_DATE) AS CREATE_DATE,
                ROW_NUMBER() OVER (
                    PARTITION BY s.VSAT 
                    ORDER BY 
                        CASE WHEN MAX(s.STATUS) = 10 THEN 0 ELSE 1 END,  -- Приоритет активным
                        MAX(s.CREATE_DATE) DESC NULLS LAST,  -- Затем по дате создания
                        MAX(s.SERVICE_ID) DESC  -- Затем по SERVICE_ID (больший = новее)
                ) AS rn
            FROM SERVICES s
            JOIN CUSTOMERS c ON s.CUSTOMER_ID = c.CUSTOMER_ID
            JOIN ACCOUNTS a ON s.ACCOUNT_ID = a.ACCOUNT_ID
            LEFT JOIN OUTER_IDS oi 
                ON oi.ID = c.CUSTOMER_ID
               AND UPPER(TRIM(oi.TBL)) = 'CUSTOMERS'
            LEFT JOIN BM_CUSTOMER_CONTACT cc ON cc.CUSTOMER_ID = c.CUSTOMER_ID
            LEFT JOIN BM_CONTACT_DICT cd ON cd.CONTACT_DICT_ID = cc.CONTACT_DICT_ID
            WHERE s.VSAT IS NOT NULL
              -- Убираем фильтр TYPE_ID, так как для любого найденного SERVICE_ID должен быть ACCOUNT_ID
            GROUP BY s.VSAT, s.SERVICE_ID
        ) s_ranked
        WHERE s_ranked.rn = 1  -- Берем только первый (самый приоритетный) сервис
    ) imei_service_info ON TRIM(imei_service_info.IMEI) = TRIM(v.IMEI)
        AND (v.SERVICE_ID IS NULL OR v.SERVICE_ID = imei_service_info.SERVICE_ID)
    -- Дополнительный JOIN для получения названия клиента по CODE_1C, если оно отсутствует
    LEFT JOIN (
        SELECT 
            oi.EXT_ID AS CODE_1C,
            COALESCE(
                MAX(CASE WHEN cd.MNEMONIC = 'description' AND cc.CONTACT_DICT_ID = 23 THEN cc.VALUE END),
                TRIM(
                    NVL(MAX(CASE WHEN cd.MNEMONIC = 'last_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' ||
                    NVL(MAX(CASE WHEN cd.MNEMONIC = 'first_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' ||
                    NVL(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '')
                )
            ) AS CUSTOMER_NAME
        FROM CUSTOMERS c
        LEFT JOIN OUTER_IDS oi 
            ON oi.ID = c.CUSTOMER_ID
           AND UPPER(TRIM(oi.TBL)) = 'CUSTOMERS'
        LEFT JOIN BM_CUSTOMER_CONTACT cc 
            ON cc.CUSTOMER_ID = c.CUSTOMER_ID
        LEFT JOIN BM_CONTACT_DICT cd 
            ON cd.CONTACT_DICT_ID = cc.CONTACT_DICT_ID
        WHERE oi.EXT_ID IS NOT NULL
        GROUP BY oi.EXT_ID
    ) cust_info ON cust_info.CODE_1C = v.CODE_1C
    WHERE 1=1
        {plan_condition}
        {period_condition}
        {contract_condition}
        {imei_condition}
        {customer_condition}
        {code_1c_condition}
    ORDER BY v.BILL_MONTH DESC, "Calculated Overage ($)" DESC NULLS LAST
    """
    
    # Формируем финальный запрос с подстановкой условий
    query = base_query.format(
        plan_condition=plan_condition,
        period_condition=period_condition,
        contract_condition=contract_condition,
        imei_condition=imei_condition,
        customer_condition=customer_condition,
        code_1c_condition=code_1c_condition
    )
    
    try:
        # Выполняем запрос напрямую (все параметры уже подставлены в запрос)
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            return df
        
        # Bill Month уже в формате "YYYY-MM" из VIEW, дополнительные действия не требуются
        
        return df
    except Exception as e:
        st.error(f"Ошибка получения отчета: {e}")
        return None
    finally:
        if conn:
            conn.close()


# Временно отключено кэширование для диагностики зацикливания
# @st.cache_data(ttl=300)  # Кэшируем на 5 минут
def get_current_period():
    """Получение текущего периода из BM_PERIOD (где SYSDATE между START_DATE и STOP_DATE, или самый последний открытый)"""
    try:
        conn = get_connection()
        if not conn:
            return None
        
        # Ищем период, где текущая дата попадает в диапазон START_DATE - STOP_DATE
        # Если такого нет, берем последний открытый период (IS_CLOSED = 0 или NULL)
        query = """
        SELECT 
            TO_CHAR(START_DATE, 'YYYY-MM') AS PERIOD_YYYYMM
        FROM (
            SELECT 
                START_DATE,
                STOP_DATE,
                IS_CLOSED
            FROM BM_PERIOD
            WHERE SYSDATE BETWEEN START_DATE AND STOP_DATE
            ORDER BY PERIOD_ID DESC
            FETCH FIRST 1 ROW ONLY
        )
        UNION ALL
        SELECT 
            TO_CHAR(START_DATE, 'YYYY-MM') AS PERIOD_YYYYMM
        FROM (
            SELECT 
                START_DATE,
                STOP_DATE,
                IS_CLOSED
            FROM BM_PERIOD
            WHERE SYSDATE NOT BETWEEN START_DATE AND STOP_DATE
              AND (IS_CLOSED = 0 OR IS_CLOSED IS NULL)
            ORDER BY PERIOD_ID DESC
            FETCH FIRST 1 ROW ONLY
        )
        FETCH FIRST 1 ROW ONLY
        """
        
        cursor = conn.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        cursor.close()
        
        if row and row[0]:
            return str(row[0])
        # Если период не найден в BM_PERIOD, возвращаем текущий месяц
        return datetime.now().strftime('%Y-%m')
    except Exception as e:
        import traceback
        print(f"Ошибка получения текущего периода: {e}")
        print(traceback.format_exc())
        # В случае ошибки возвращаем текущий месяц
        return datetime.now().strftime('%Y-%m')
    finally:
        try:
            if conn:
                conn.close()
        except:
            pass


def get_periods():
    """Получение списка периодов из BM_PERIOD (возвращаем период в формате YYYY-MM для отображения и фильтрации)"""
    try:
        conn = get_connection()
        if not conn:
            return []
        
        # Загружаем периоды из BM_PERIOD, используем TO_CHAR(START_DATE, 'YYYY-MM') для отображения
        query = """
        SELECT DISTINCT 
            TO_CHAR(START_DATE, 'YYYY-MM') AS display_period
        FROM BM_PERIOD
        WHERE START_DATE IS NOT NULL
        ORDER BY START_DATE DESC
        FETCH FIRST 100 ROWS ONLY
        """
        
        cursor = conn.cursor()
        cursor.execute(query)
        periods = []
        for row in cursor.fetchall():
            if row[0]:
                # Возвращаем кортеж (display_period, display_period) для совместимости с существующим кодом
                periods.append((str(row[0]), str(row[0])))
        cursor.close()
        return periods
    except Exception as e:
        # Не используем st.error здесь, так как это может вызвать зацикливание
        # Вместо этого возвращаем пустой список и логируем ошибку
        import traceback
        print(f"Ошибка получения периодов: {e}")
        print(traceback.format_exc())
        return []
    finally:
        try:
            if conn:
                conn.close()
        except:
            pass


@st.cache_data(ttl=300)  # Кэшируем на 5 минут
def get_plans():
    """Получение списка тарифных планов"""
    conn = get_connection()
    if not conn:
        return []
    
    query = """
    SELECT DISTINCT PLAN_NAME
    FROM V_CONSOLIDATED_REPORT_WITH_BILLING
    WHERE PLAN_NAME IS NOT NULL
    ORDER BY PLAN_NAME
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        plans = [row[0] for row in cursor.fetchall() if row[0]]
        cursor.close()
        return plans
    except Exception as e:
        st.error(f"Ошибка получения планов: {e}")
        return []
    finally:
        if conn:
            conn.close()


def get_revenue_periods():
    """Получение списка периодов из доходов (V_REVENUE_FROM_INVOICES)"""
    try:
        conn = get_connection()
        if not conn:
            return []
        
        # Используем PERIOD_YYYYMM для отображения и фильтрации (формат 2025-01)
        query = """
        SELECT DISTINCT 
            PERIOD_YYYYMM AS display_period
        FROM V_REVENUE_FROM_INVOICES
        WHERE PERIOD_YYYYMM IS NOT NULL
        ORDER BY PERIOD_YYYYMM DESC
        FETCH FIRST 100 ROWS ONLY
        """
        
        cursor = conn.cursor()
        cursor.execute(query)
        periods = []
        for row in cursor.fetchall():
            if row[0]:
                # Возвращаем кортеж (display_period, display_period) для совместимости
                periods.append((str(row[0]), str(row[0])))
        cursor.close()
        return periods
    except Exception as e:
        import traceback
        print(f"Ошибка получения периодов доходов: {e}")
        print(traceback.format_exc())
        return []
    finally:
        try:
            if conn:
                conn.close()
        except:
            pass


def get_revenue_report(period_filter=None, contract_id_filter=None, imei_filter=None, customer_name_filter=None, code_1c_filter=None):
    """Получение отчета по доходам из счетов-фактур"""
    conn = get_connection()
    if not conn:
        return None
    
    # Фильтр по периодам
    period_condition = ""
    if period_filter and period_filter != "All Periods":
        period_condition = f"AND v.PERIOD_YYYYMM = '{period_filter}'"
    
    # Фильтр по CONTRACT_ID (SUB-*)
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
        customer_condition = f"AND UPPER(COALESCE(v.CUSTOMER_NAME, '')) LIKE UPPER('%{customer_value}%')"
    
    # Фильтр по коду 1С
    code_1c_condition = ""
    if code_1c_filter and code_1c_filter.strip():
        code_1c_value = code_1c_filter.strip().replace("'", "''")
        code_1c_condition = f"AND v.CODE_1C LIKE '%{code_1c_value}%'"
    
    query = """
    SELECT 
        v.PERIOD_YYYYMM AS "Период",
        v.CONTRACT_ID AS "Contract ID",
        v.IMEI AS "IMEI",
        v.CUSTOMER_NAME AS "Organization/Person",
        v.CODE_1C AS "Code 1C",
        v.SERVICE_ID AS "Service ID",
        v.AGREEMENT_NUMBER AS "Agreement #",
        v.ORDER_NUMBER AS "Order #",
        v.ACC_CURRENCY_NAME AS "Валюта учета",
        v.REVENUE_SBD_TRAFFIC AS "SBD Трафик превышения",
        NVL(v.REVENUE_SBD_TRAFFIC_SBD1, 0) AS "SBD Трафик SBD-1",
        NVL(v.REVENUE_SBD_TRAFFIC_SBD10, 0) AS "SBD Трафик SBD-10",
        v.REVENUE_SBD_ABON AS "SBD Абонплата",
        v.REVENUE_SBD_TOTAL AS "SBD Всего",
        v.REVENUE_SUSPEND_ABON AS "SUSPEND Абонплата",
        v.REVENUE_MONITORING_ABON AS "Мониторинг Абонплата",
        v.REVENUE_MONITORING_BLOCK_ABON AS "Блокировка мониторинга",
        v.REVENUE_MSG_ABON AS "Сообщения Абонплата",
        v.REVENUE_TOTAL AS "Итого доходов (руб)",
        v.INVOICE_ITEMS_COUNT AS "Позиций в СФ"
    FROM V_REVENUE_FROM_INVOICES v
    WHERE 1=1
        {period_condition}
        {contract_condition}
        {imei_condition}
        {customer_condition}
        {code_1c_condition}
    ORDER BY v.PERIOD_YYYYMM DESC, v.CONTRACT_ID
    """
    
    query = query.format(
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
        st.error(f"Ошибка получения отчета по доходам: {e}")
        return None
    finally:
        if conn:
            conn.close()


def get_analytics_duplicates(period_id):
    """Поиск дубликатов в ANALYTICS для конкретного PERIOD_ID.
    
    Дубликаты - это записи, где все поля совпадают, кроме AID (первичного ключа).
    """
    conn = get_connection()
    if not conn:
        return None
    
    if not period_id:
        return None
    
    # Запрос для поиска дубликатов: группируем по всем полям кроме AID
    query = """
    WITH duplicate_groups AS (
        SELECT 
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
            PARTNER_PERCENT,
            COUNT(*) AS DUPLICATE_COUNT,
            LISTAGG(AID, ', ') WITHIN GROUP (ORDER BY AID) AS AID_LIST
        FROM ANALYTICS
        WHERE PERIOD_ID = :period_id
        GROUP BY 
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
        HAVING COUNT(*) > 1
    )
    SELECT 
        dg.PERIOD_ID,
        dg.SERVICE_ID AS SERVICE_ID_ANALYTICS,
        dg.CUSTOMER_ID AS CUSTOMER_ID_ANALYTICS,
        dg.ACCOUNT_ID,
        dg.TYPE_ID,
        dg.TARIFF_ID,
        dg.TARIFFEL_ID,
        dg.VSAT,
        dg.MONEY,
        dg.PRICE,
        dg.TRAF,
        dg.TOTAL_TRAF,
        dg.CBYTE,
        dg.INVOICE_ITEM_ID,
        dg.FLAG,
        dg.RESOURCE_TYPE_ID,
        dg.CLASS_ID,
        dg.CLASS_NAME,
        dg.BLANK,
        dg.COUNTER_ID,
        dg.COUNTER_CF,
        dg.ZONE_ID,
        dg.THRESHOLD,
        dg.SUB_TYPE_ID,
        dg.SUB_PERIOD_ID,
        dg.PMONEY,
        dg.PARTNER_PERCENT,
        dg.DUPLICATE_COUNT,
        dg.AID_LIST,
        c.CUSTOMER_ID,
        oi.EXT_ID AS CODE_1C,
        MAX(CASE WHEN cd.MNEMONIC = 'description' AND cc.CONTACT_DICT_ID = 23 THEN cc.VALUE END) AS CUSTOMER_NAME,
        s.LOGIN AS CONTRACT_ID,
        s.SERVICE_ID,
        rt.MNEMONIC AS RESOURCE_MNEMONIC,
        rt.NAME AS RESOURCE_NAME,
        t.NAME AS TARIFF_NAME,
        z.DESCRIPTION AS ZONE_NAME
    FROM duplicate_groups dg
    LEFT JOIN SERVICES s ON dg.SERVICE_ID = s.SERVICE_ID
    LEFT JOIN CUSTOMERS c ON dg.CUSTOMER_ID = c.CUSTOMER_ID
    LEFT JOIN OUTER_IDS oi ON oi.ID = c.CUSTOMER_ID AND UPPER(TRIM(oi.TBL)) = 'CUSTOMERS'
    LEFT JOIN BM_CUSTOMER_CONTACT cc ON cc.CUSTOMER_ID = c.CUSTOMER_ID
    LEFT JOIN BM_CONTACT_DICT cd ON cd.CONTACT_DICT_ID = cc.CONTACT_DICT_ID
    LEFT JOIN BM_RESOURCE_TYPE rt ON dg.RESOURCE_TYPE_ID = rt.RESOURCE_TYPE_ID
    LEFT JOIN BM_TARIFF t ON dg.TARIFF_ID = t.TARIFF_ID
    LEFT JOIN BM_ZONE z ON dg.ZONE_ID = z.ZONE_ID
    GROUP BY 
        dg.PERIOD_ID,
        dg.SERVICE_ID,
        dg.CUSTOMER_ID,
        dg.ACCOUNT_ID,
        dg.TYPE_ID,
        dg.TARIFF_ID,
        dg.TARIFFEL_ID,
        dg.VSAT,
        dg.MONEY,
        dg.PRICE,
        dg.TRAF,
        dg.TOTAL_TRAF,
        dg.CBYTE,
        dg.INVOICE_ITEM_ID,
        dg.FLAG,
        dg.RESOURCE_TYPE_ID,
        dg.CLASS_ID,
        dg.CLASS_NAME,
        dg.BLANK,
        dg.COUNTER_ID,
        dg.COUNTER_CF,
        dg.ZONE_ID,
        dg.THRESHOLD,
        dg.SUB_TYPE_ID,
        dg.SUB_PERIOD_ID,
        dg.PMONEY,
        dg.PARTNER_PERCENT,
        dg.DUPLICATE_COUNT,
        dg.AID_LIST,
        c.CUSTOMER_ID,
        oi.EXT_ID,
        s.LOGIN,
        s.SERVICE_ID,
        rt.MNEMONIC,
        rt.NAME,
        t.NAME,
        z.DESCRIPTION
    ORDER BY dg.DUPLICATE_COUNT DESC, dg.MONEY DESC
    """
    
    try:
        df = pd.read_sql_query(query, conn, params={'period_id': period_id})
        return df
    except Exception as e:
        st.error(f"Ошибка поиска дубликатов: {e}")
        import traceback
        st.code(traceback.format_exc())
        return None
    finally:
        if conn:
            conn.close()


def get_analytics_invoice_period_report(period_filter=None, contract_id_filter=None, imei_filter=None, 
                                        customer_name_filter=None, code_1c_filter=None, tariff_filter=None, zone_filter=None):
    """Получение отчета по счетам за период из ANALYTICS"""
    conn = get_connection()
    if not conn:
        return None
    
    # Фильтр по периодам
    period_condition = ""
    if period_filter and period_filter != "All Periods":
        period_condition = f"AND v.PERIOD_YYYYMM = '{period_filter}'"
    
    # Фильтр по CONTRACT_ID (SUB-*)
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
        customer_condition = f"AND UPPER(COALESCE(v.CUSTOMER_NAME, '')) LIKE UPPER('%{customer_value}%')"
    
    # Фильтр по коду 1С
    code_1c_condition = ""
    if code_1c_filter and code_1c_filter.strip():
        code_1c_value = code_1c_filter.strip().replace("'", "''")
        code_1c_condition = f"AND v.CODE_1C LIKE '%{code_1c_value}%'"
    
    # Фильтр по тарифу
    tariff_condition = ""
    if tariff_filter and tariff_filter.strip():
        tariff_value = tariff_filter.strip().replace("'", "''")
        tariff_condition = f"AND v.TARIFF_ID = {tariff_value}"
    
    # Фильтр по зоне
    zone_condition = ""
    if zone_filter and zone_filter.strip():
        zone_value = zone_filter.strip().replace("'", "''")
        zone_condition = f"AND v.ZONE_ID = {zone_value}"
    
    query = """
    SELECT 
        v.PERIOD_YYYYMM AS "Период",
        v.CUSTOMER_NAME AS "Клиент",
        v.CODE_1C AS "Код 1С",
        v.ACCOUNT_NAME AS "Договор",
        v.CONTRACT_ID AS "Contract ID",
        v.SERVICE_ID AS "Service ID",
        v.IMEI AS "IMEI",
        v.TARIFF_NAME AS "Тариф",
        v.ZONE_NAME AS "Зона",
        v.RESOURCE_MNEMONIC AS "Тип ресурса",
        v.RESOURCE_TYPE_NAME AS "Название ресурса",
        v.MONEY AS "Сумма (руб)",
        v.MONEY_ABON AS "Абонплата (руб)",
        v.MONEY_TRAFFIC AS "Трафик (руб)",
        v.TRAF AS "Трафик (объем)",
        v.TOTAL_TRAF AS "Общий трафик",
        v.IN_INVOICE AS "В счете",
        v.SERVICE_STATUS AS "Статус услуги"
    FROM V_ANALYTICS_INVOICE_PERIOD v
    WHERE 1=1
        {period_condition}
        {contract_condition}
        {imei_condition}
        {customer_condition}
        {code_1c_condition}
        {tariff_condition}
        {zone_condition}
    ORDER BY v.PERIOD_YYYYMM DESC, v.CUSTOMER_NAME, v.CONTRACT_ID, v.TARIFF_ID, v.ZONE_ID
    """
    
    query = query.format(
        period_condition=period_condition,
        contract_condition=contract_condition,
        imei_condition=imei_condition,
        customer_condition=customer_condition,
        code_1c_condition=code_1c_condition,
        tariff_condition=tariff_condition,
        zone_condition=zone_condition
    )
    
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"Ошибка получения отчета по счетам за период: {e}")
        return None
    finally:
        if conn:
            conn.close()


def export_to_csv(df):
    """Экспорт в CSV"""
    output = io.StringIO()
    df.to_csv(output, index=False, encoding='utf-8')
    return output.getvalue()


def export_to_excel(df):
    """Экспорт в Excel"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Overage Report')
    return output.getvalue()


# Инициализация базы данных пользователей
init_db()

def show_login_page():
    """Отображение страницы входа"""
    st.title("🔐 Система отчетов по Iridium M2M")
    st.markdown("---")
    
    st.info("💡 Для создания учетной записи обратитесь к администратору или используйте скрипт `create_user.py`")
    
    st.subheader("Вход")
    with st.form("login_form"):
        login_username = st.text_input("Имя пользователя", key="login_username")
        login_password = st.text_input("Пароль", type="password", key="login_password")
        login_submitted = st.form_submit_button("Войти", use_container_width=True)
        
        if login_submitted:
            if not login_username or not login_password:
                st.error("Заполните все поля")
            else:
                success, username, is_super = authenticate_user(login_username, login_password)
                if success:
                    st.session_state.authenticated = True
                    st.session_state.username = username
                    st.session_state.is_superuser = is_super
                    st.success(f"Добро пожаловать, {username}! 👋")
                    st.rerun()
                else:
                    st.error("Неверное имя пользователя или пароль")

def show_user_management():
    """Отображение управления пользователями (только для суперпользователей)"""
    if not st.session_state.get('is_superuser', False):
        return
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("👥 Управление пользователями")
    
    # Создание нового пользователя
    with st.sidebar.expander("➕ Создать пользователя"):
        with st.form("create_user_form"):
            new_username = st.text_input("Имя пользователя")
            new_password = st.text_input("Пароль", type="password")
            new_is_super = st.checkbox("Суперпользователь")
            create_submitted = st.form_submit_button("Создать")
            
            if create_submitted:
                success, message = create_user(
                    new_username, 
                    new_password, 
                    is_superuser=new_is_super,
                    created_by=st.session_state.username
                )
                if success:
                    st.sidebar.success(message)
                    st.rerun()
                else:
                    st.sidebar.error(message)
    
    # Список пользователей
    with st.sidebar.expander("📋 Список пользователей"):
        users = list_users()
        if users:
            for user in users:
                superuser_mark = " 👑" if user['is_superuser'] else ""
                st.write(f"**{user['username']}**{superuser_mark}")
                if user['last_login']:
                    st.caption(f"Последний вход: {user['last_login'][:10]}")
                
                # Кнопка удаления (кроме текущего пользователя и суперпользователей)
                if user['username'] != st.session_state.username and not user['is_superuser']:
                    if st.button("🗑️ Удалить", key=f"delete_{user['username']}"):
                        success, message = delete_user(user['username'])
                        if success:
                            st.sidebar.success(message)
                            st.rerun()
                        else:
                            st.sidebar.error(message)
        else:
            st.write("Пользователи не найдены")

def main():
    """Основная функция приложения"""
    
    # Инициализация session state для авторизации
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    if 'username' not in st.session_state:
        st.session_state.username = None
    if 'is_superuser' not in st.session_state:
        st.session_state.is_superuser = False
    
    # Если не авторизован, показываем страницу входа
    if not st.session_state.authenticated:
        st.set_page_config(
            page_title="Iridium M2M - Авторизация",
            page_icon="🔐",
            layout="centered"
        )
        show_login_page()
        return
    
    # Основное приложение для авторизованных пользователей
    st.set_page_config(
        page_title="Iridium M2M KB Assistant",
        page_icon="📊",
        layout="wide"
    )
    
    # Боковая панель с информацией о пользователе
    st.sidebar.header("👤 Пользователь")
    st.sidebar.write(f"**{st.session_state.username}**")
    if st.session_state.is_superuser:
        st.sidebar.write("👑 Суперпользователь")
    
    # Управление пользователями для суперпользователей
    show_user_management()
    
    # Кнопка выхода
    if st.sidebar.button("🚪 Выйти"):
        st.session_state.authenticated = False
        st.session_state.username = None
        st.session_state.is_superuser = False
        st.rerun()
    
    st.sidebar.markdown("---")
    
    # Проверка загрузки конфигурации
    if not ORACLE_PASSWORD:
        st.error("⚠️ **Конфигурация не загружена!**")
        st.warning("""
        Запустите приложение через скрипт:
        ```bash
        ./run_streamlit.sh
        ```
        
        Скрипт автоматически загрузит `config.env` с настройками базы данных.
        """)
        st.stop()
    
    # Заголовок
    st.title("📊 Iridium M2M KB Assistant")
    
    st.markdown("---")
    
    # Фильтры в боковой панели (вне вкладок, чтобы были доступны всегда)
    with st.sidebar:
        st.header("⚙️ Filters")
        
        # Кэшируем периоды и планы, чтобы не делать запросы при каждом rerun
        # Загружаем периоды только если они еще не загружены или нужно обновить
        if 'cached_periods_data' not in st.session_state:
            with st.spinner("Загрузка периодов..."):
                periods_data = get_periods()
                st.session_state.cached_periods_data = periods_data
        else:
            periods_data = st.session_state.cached_periods_data
        
        # Проверяем, что periods_data не пустой
        if not periods_data:
            st.error("⚠️ Не удалось загрузить периоды. Проверьте подключение к базе данных.")
            st.stop()
        
        # periods_data теперь список кортежей (display_period, filter_period)
        # Используем display_period (FINANCIAL_PERIOD) для отображения и фильтрации
        period_display_list = []
        
        for display_period, filter_period in periods_data:
            if display_period:
                # Используем только уникальные значения FINANCIAL_PERIOD
                if display_period not in period_display_list:
                    period_display_list.append(display_period)
        
        # Проверяем, что period_display_list не пустой
        if not period_display_list:
            st.error("⚠️ Нет доступных периодов для отображения.")
            st.stop()
        
        # Получаем текущий период из BM_PERIOD (в формате YYYY-MM)
        current_period = get_current_period()
        
        # Если текущий период не найден, используем текущий месяц
        if not current_period:
            current_period = datetime.now().strftime('%Y-%m')
        
        # По умолчанию выбираем текущий период, если он есть в списке, иначе последний период
        if 'selected_period_index' not in st.session_state:
            if current_period and current_period in period_display_list:
                # Находим индекс текущего периода
                st.session_state.selected_period_index = period_display_list.index(current_period)
            else:
                # Иначе выбираем последний период (первый в отсортированном списке)
                st.session_state.selected_period_index = 0
        
        # Проверяем, что индекс не выходит за границы
        if st.session_state.selected_period_index >= len(period_display_list):
            st.session_state.selected_period_index = 0
        
        period_options = period_display_list + ["All Periods"]  # Последний период первым, потом "All Periods"
        selected_period_display = st.selectbox(
            "Отчетный Период", 
            period_options,
            index=st.session_state.selected_period_index,
            key='period_selectbox',
            help="Выберите отчетный период. Фильтрация выполняется по BILL_MONTH."
        )
        
        # Используем selected_period_display напрямую для фильтрации по BILL_MONTH
        if selected_period_display == "All Periods":
            selected_period = None
        else:
            selected_period = selected_period_display
        
        # Обновляем индекс при изменении
        if selected_period_display in period_options:
            try:
                st.session_state.selected_period_index = period_options.index(selected_period_display)
            except ValueError:
                st.session_state.selected_period_index = 0
        
        # Фильтр по планам (ленивая загрузка - только при необходимости)
        # Используем session_state для хранения выбранного плана
        if 'selected_plan' not in st.session_state:
            st.session_state.selected_plan = "All Plans"
        if 'use_plan_filter' not in st.session_state:
            st.session_state.use_plan_filter = False
        
        # Чекбокс для включения фильтра по планам
        use_plan_filter = st.checkbox(
            "📋 Использовать фильтр по тарифному плану",
            value=st.session_state.use_plan_filter,
            key='use_plan_filter_checkbox',
            help="Включите для фильтрации по тарифному плану"
        )
        st.session_state.use_plan_filter = use_plan_filter
        
        if use_plan_filter:
            # Загружаем планы только если фильтр включен
            plans = get_plans()
            plan_options = ["All Plans"] + plans
            selected_plan = st.selectbox(
                "Plan", 
                plan_options, 
                key='plan_selectbox',
                index=0,
                help="Выберите тарифный план для фильтрации"
            )
            st.session_state.selected_plan = selected_plan
        else:
            # Если фильтр выключен, используем "All Plans"
            selected_plan = "All Plans"
            st.session_state.selected_plan = "All Plans"
        
        st.markdown("---")
        st.subheader("🔍 Additional Filters")
        
        # Фильтр по CONTRACT_ID (SUB-*)
        contract_id_filter = st.text_input(
            "Contract ID (SUB-*)",
            value="",
            key='contract_id_filter',
            help="Поиск по CONTRACT_ID (например: SUB-45439909011)"
        )
        
        # Фильтр по IMEI
        imei_filter = st.text_input(
            "IMEI",
            value="",
            key='imei_filter',
            help="Поиск по IMEI (например: 300215060074700)"
        )
        
        # Фильтр по названию клиента
        customer_name_filter = st.text_input(
            "Organization/Person",
            value="",
            key='customer_name_filter',
            help="Поиск по названию организации или ФИО клиента"
        )
        
        # Фильтр по коду 1С
        code_1c_filter = st.text_input(
            "Code 1C",
            value="",
            key='code_1c_filter',
            help="Поиск по коду 1С (например: 00007660)"
        )
        
        st.markdown("---")
        st.header("🔐 Database Connection")
        # Информация о подключении скрыта для безопасности
        
        # Кнопка тестирования подключения
        if st.button("🔌 Test Connection", key='test_connection_btn'):
            test_conn = get_connection()
            if test_conn:
                st.success("✅ Подключение успешно!")
                test_conn.close()
            else:
                st.error("❌ Ошибка подключения. Проверьте config.env")
        
        st.info("💡 Конфигурация загружается из config.env при запуске через run_streamlit.sh")
    
    # Создаем вкладки для отчета, доходов, загрузки данных, ассистента и расширения KB
    tab_assistant, tab_kb_expansion, tab_report, tab_revenue, tab_analytics, tab_loader = st.tabs([
        "🤖 Ассистент",
        "📚 Расширение KB",
        "💰 Расходы Иридиум", 
        "💰 Доходы",
        "📋 Счета за период",
        "📥 Data Loader"
    ])
    
    # ========== ASSISTANT TAB ==========
    with tab_assistant:
        try:
            # Убеждаемся, что переменная окружения установлена перед импортом
            os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'
            from kb_billing.rag.streamlit_assistant import show_assistant_tab
            show_assistant_tab()
        except ImportError as e:
            st.error(f"❌ Ошибка импорта модуля ассистента: {e}")
            st.info("""
            Убедитесь, что:
            1. Установлены зависимости: `pip install qdrant-client sentence-transformers`
            2. Qdrant запущен: `docker run -d -p 6333:6333 qdrant/qdrant`
            3. KB инициализирована: `python kb_billing/rag/init_kb.py`
            """)
        except Exception as e:
            st.error(f"❌ Ошибка при загрузке ассистента: {e}")
            import traceback
            with st.expander("Детали ошибки"):
                st.code(traceback.format_exc())
    
    # ========== KB EXPANSION TAB ==========
    with tab_kb_expansion:
        try:
            # Убеждаемся, что переменная окружения установлена перед импортом
            os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'
            from kb_billing.rag.streamlit_kb_expansion import show_kb_expansion_tab
            show_kb_expansion_tab()
        except ImportError as e:
            st.error(f"❌ Ошибка импорта модуля расширения KB: {e}")
            st.info("""
            Убедитесь, что:
            1. Установлены зависимости: `pip install qdrant-client sentence-transformers`
            2. Qdrant запущен: `docker run -d -p 6333:6333 qdrant/qdrant`
            3. KB инициализирована: `python kb_billing/rag/init_kb.py`
            """)
        except Exception as e:
            st.error(f"❌ Ошибка при загрузке расширения KB: {e}")
            import traceback
            with st.expander("Детали ошибки"):
                st.code(traceback.format_exc())
    
    # ========== REPORT TAB ==========
    with tab_report:
        
        period_filter = selected_period  # selected_period уже преобразован в filter_period (BILL_MONTH) или None
        plan_filter = None if selected_plan == "All Plans" else selected_plan
        contract_id_filter = contract_id_filter if contract_id_filter else None
        imei_filter = imei_filter if imei_filter else None
        customer_name_filter = customer_name_filter if customer_name_filter else None
        code_1c_filter = code_1c_filter if code_1c_filter else None
        
        # Загружаем отчет ТОЛЬКО если выбран период (не "All Periods")
        filter_key = f"{period_filter}_{plan_filter}_{contract_id_filter}_{imei_filter}_{customer_name_filter}_{code_1c_filter}"
        
        # Проверяем, нужно ли загружать отчет
        if period_filter is not None:
            if 'last_report_key' not in st.session_state or st.session_state.last_report_key != filter_key:
                with st.spinner("Loading report data..."):
                    df = get_main_report(
                        period_filter, 
                        plan_filter,
                        contract_id_filter,
                        imei_filter,
                        customer_name_filter,
                        code_1c_filter
                    )
                    st.session_state.last_report_key = filter_key
                    st.session_state.last_report_df = df
            else:
                df = st.session_state.get('last_report_df', None)
        else:
            # Если период не выбран, не загружаем отчет
            df = None
            st.info("ℹ️ Выберите период для загрузки отчета")
        
        if df is not None and not df.empty:
            st.success(f"✅ Загружено записей: {len(df):,}")
            
            # Метрики
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Всего записей", f"{len(df):,}")
            with col2:
                total_overage = df["Calculated Overage ($)"].sum()
                st.metric("Total Overage", f"${total_overage:,.2f}")
            with col3:
                total_advance = df["Advance Charge"].sum()
                st.metric("Advance Charge", f"${total_advance:,.2f}")
            with col4:
                total_advance_prev = df["Advance Charge Previous Month"].sum()
                st.metric("Advance Charge Previous Month", f"${total_advance_prev:,.2f}")

            # Дополнительные метрики по тарифам SBD-1 / SBD-10
            if "Plan Name" in df.columns:
                sbd1_mask = df["Plan Name"] == "SBD Tiered 1250 1K"
                sbd10_mask = df["Plan Name"] == "SBD Tiered 1250 10K"

                sbd1_overage = df.loc[sbd1_mask, "Calculated Overage ($)"].sum()
                sbd10_overage = df.loc[sbd10_mask, "Calculated Overage ($)"].sum()

                col_s1, col_s2, col_s3 = st.columns(3)
                with col_s1:
                    st.metric("SBD-1 Overage ($)", f"${sbd1_overage:,.2f}")
                with col_s2:
                    st.metric("SBD-10 Overage ($)", f"${sbd10_overage:,.2f}")
                with col_s3:
                    st.metric("SBD Overage SBD-1+10 ($)", f"${(sbd1_overage + sbd10_overage):,.2f}")
            
            st.markdown("---")
            
            # Убеждаемся, что все колонки видны, даже если они NULL
            display_df = df.copy()
            
            # Заполняем NULL пустыми строками для строковых колонок (включая Activation Date)
            for col in display_df.columns:
                if display_df[col].dtype == 'object':  # строковые колонки
                    display_df[col] = display_df[col].fillna('')
            
            # Убеждаемся, что Activation Date всегда присутствует и отображается
            if 'Activation Date' in display_df.columns:
                # Заполняем NULL пустыми строками для Activation Date
                display_df['Activation Date'] = display_df['Activation Date'].fillna('')
            else:
                # Колонка отсутствует - добавляем пустую колонку (не должно случиться)
                display_df['Activation Date'] = ''
            
            # Упорядочиваем колонки: Activation Date должна быть перед Plan Name
            # Определяем правильный порядок колонок
            expected_order = [
                "Отчетный Период", "Bill Month", "IMEI", "Contract ID",
                "Organization/Person", "Code 1C", "Service ID", "Agreement #",
                "Activation Date",  # Должна быть перед Plan Name
                "Plan Name", "Plan Monthly", "Plan Suspended",
                "Traffic Usage (KB)", 
                "Mailbox Events", "Registration Events",
                "Overage (KB)", "Calculated Overage ($)", "Total Amount ($)",
                "Activation Fee", "Advance Charge", "Advance Charge Previous Month",
                "Credit", "Credited", "Prorated"
            ]
            
            # Берем только те колонки, которые есть в dataframe, в нужном порядке
            ordered_columns = [col for col in expected_order if col in display_df.columns]
            # Добавляем остальные колонки, которых нет в списке
            other_columns = [col for col in display_df.columns if col not in expected_order]
            display_df = display_df[ordered_columns + other_columns]
            
            # Таблица данных
            st.dataframe(display_df, use_container_width=True, height=400)
            
            # Экспорт
            st.markdown("---")
            col1, col2 = st.columns(2)
            with col1:
                csv_data = export_to_csv(df)
                st.download_button(
                    label="📥 Download CSV",
                    data=csv_data,
                    file_name=f"iridium_overage_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            with col2:
                excel_data = export_to_excel(df)
                st.download_button(
                    label="📥 Download Excel",
                    data=excel_data,
                    file_name=f"iridium_overage_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            
            # Детализация плат по категориям
            st.markdown("---")
            st.subheader("🔎 Fees breakdown (by category)")
            contract_filter = st.text_input("Filter by Contract ID (optional)", value="", key="contract_filter")
            
            # Фильтруем только контракты из основного отчета
            contract_ids = df['Contract ID'].dropna().unique().tolist() if not df.empty else []
            
            # Если указан фильтр, добавляем его
            if contract_filter.strip():
                contract_ids.append(contract_filter.strip())
            
            # Подключаемся для получения fees breakdown
            conn2 = get_connection()
            if conn2:
                contract_condition = ""
                
                if contract_ids:
                    # Ограничиваем количество контрактов в запросе (макс 200)
                    limited_contract_ids = contract_ids[:200]
                    
                    # Экранируем кавычки и формируем условие
                    contract_list = "', '".join([str(c).replace("'", "''") for c in limited_contract_ids[:100]])
                    contract_condition = f"AND f.CONTRACT_ID IN ('{contract_list}')"
                
                # Фильтр по периоду (если выбран период в основном отчете)
                period_condition = ""
                if period_filter and period_filter != "All Periods":
                    # BILL_MONTH в формате "YYYY-MM", извлекаем YYYYMM
                    year_month = period_filter.replace('-', '')
                    # Ищем fees с периодами начинающимися с YYYYMM (формат YYYYMMDD в SOURCE_FILE)
                    period_condition = f"AND f.SOURCE_FILE LIKE '%.{year_month}%.csv'"
                
                # Запрос для Oracle - используем STECCOM_EXPENSES напрямую
                fees_q = f"""
                SELECT 
                    CASE 
                        WHEN REGEXP_LIKE(f.SOURCE_FILE, '\\.([0-9]{{8}})\\.csv$') THEN
                            TO_NUMBER(REGEXP_SUBSTR(f.SOURCE_FILE, '\\.([0-9]{{8}})\\.csv$', 1, 1, NULL, 1))
                        ELSE NULL
                    END AS "Period",
                    f.CONTRACT_ID AS "Contract ID",
                    f.ICC_ID_IMEI AS "IMEI",
                    f.DESCRIPTION AS "Category",
                    SUM(f.AMOUNT) AS "Amount"
                FROM STECCOM_EXPENSES f
                WHERE f.ICC_ID_IMEI IS NOT NULL
                    AND f.AMOUNT IS NOT NULL
                    AND REGEXP_LIKE(f.SOURCE_FILE, '\\.([0-9]{{8}})\\.csv$')
                    {contract_condition}
                    {period_condition}
                GROUP BY 
                    CASE 
                        WHEN REGEXP_LIKE(f.SOURCE_FILE, '\\.([0-9]{{8}})\\.csv$') THEN
                            TO_NUMBER(REGEXP_SUBSTR(f.SOURCE_FILE, '\\.([0-9]{{8}})\\.csv$', 1, 1, NULL, 1))
                        ELSE NULL
                    END,
                    f.CONTRACT_ID,
                    f.ICC_ID_IMEI,
                    f.DESCRIPTION
                ORDER BY "Period" DESC NULLS LAST, f.CONTRACT_ID, f.ICC_ID_IMEI, f.DESCRIPTION
                """
                try:
                    fees_detail_df = pd.read_sql_query(fees_q, conn2)
                    
                    # Форматируем период: 20250302 -> "2025-03-02"
                    if 'Period' in fees_detail_df.columns and not fees_detail_df.empty:
                        fees_detail_df['Period'] = fees_detail_df['Period'].apply(
                            lambda x: f"{str(int(x))[:4]}-{str(int(x))[4:6]}-{str(int(x))[6:8]}" 
                            if pd.notna(x) and len(str(int(x))) >= 8 else str(x)
                        )
                    
                    if not fees_detail_df.empty:
                        st.dataframe(fees_detail_df, use_container_width=True, hide_index=True, height=300)
                    else:
                        st.info("No fees data found for selected filters")
                except Exception as e:
                    st.warning(f"Failed to load fees breakdown: {e}")
                finally:
                    conn2.close()
        elif df is not None and df.empty:
            st.warning("⚠️ Данные не найдены для выбранных фильтров")
        else:
            st.error("❌ Ошибка загрузки данных")
    
    # ========== REVENUE TAB ==========
    with tab_revenue:
        st.header("💰 Доходы из счетов-фактур")
        st.markdown("Отчет по доходам из счетов-фактур (BM_INVOICE_ITEM). Все суммы в рублях.")
        
        # Используем те же фильтры из sidebar
        period_filter = selected_period  # Фильтр по PERIOD_YYYYMM
        contract_id_filter = contract_id_filter if contract_id_filter else None
        imei_filter = imei_filter if imei_filter else None
        customer_name_filter = customer_name_filter if customer_name_filter else None
        code_1c_filter = code_1c_filter if code_1c_filter else None
        
        # Загружаем отчет ТОЛЬКО если выбран период (не "All Periods")
        filter_key = f"revenue_{period_filter}_{contract_id_filter}_{imei_filter}_{customer_name_filter}_{code_1c_filter}"
        
        if period_filter is not None:
            if 'last_revenue_key' not in st.session_state or st.session_state.last_revenue_key != filter_key:
                with st.spinner("Загрузка данных по доходам..."):
                    df_revenue = get_revenue_report(
                        period_filter,
                        contract_id_filter,
                        imei_filter,
                        customer_name_filter,
                        code_1c_filter
                    )
                    st.session_state.last_revenue_key = filter_key
                    st.session_state.last_revenue_df = df_revenue
            else:
                df_revenue = st.session_state.get('last_revenue_df', None)
        else:
            df_revenue = None
            st.info("ℹ️ Выберите период для загрузки отчета по доходам")
        
        if df_revenue is not None and not df_revenue.empty:
            st.success(f"✅ Загружено записей: {len(df_revenue):,}")
            
            # Статистика вверху (метрики для сверки, разделенные по валюте учета)
            st.markdown("---")
            st.subheader("📊 Статистика по доходам (в рублях)")
            
            # Группируем по валюте учета (ACC_CURRENCY_ID)
            if 'Валюта учета' in df_revenue.columns:
                # Статистика по валютам
                for currency in df_revenue['Валюта учета'].dropna().unique():
                    df_curr = df_revenue[df_revenue['Валюта учета'] == currency]
                    st.markdown(f"**{currency}:**")
                    
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("SBD Трафик превышения", f"{df_curr['SBD Трафик превышения'].sum():,.2f}")
                        # Разделение по тарифам
                        if 'SBD Трафик SBD-1' in df_curr.columns:
                            st.metric("SBD Трафик SBD-1", f"{df_curr['SBD Трафик SBD-1'].sum():,.2f}")
                        if 'SBD Трафик SBD-10' in df_curr.columns:
                            st.metric("SBD Трафик SBD-10", f"{df_curr['SBD Трафик SBD-10'].sum():,.2f}")
                        st.metric("SBD Абонплата", f"{df_curr['SBD Абонплата'].sum():,.2f}")
                        st.metric("SBD Всего", f"{df_curr['SBD Всего'].sum():,.2f}")
                    with col2:
                        st.metric("SUSPEND Абонплата", f"{df_curr['SUSPEND Абонплата'].sum():,.2f}")
                        st.metric("Мониторинг Абонплата", f"{df_curr['Мониторинг Абонплата'].sum():,.2f}")
                        st.metric("Блокировка мониторинга", f"{df_curr['Блокировка мониторинга'].sum():,.2f}")
                    with col3:
                        st.metric("Сообщения Абонплата", f"{df_curr['Сообщения Абонплата'].sum():,.2f}")
                    with col4:
                        st.metric("**Итого доходов**", f"**{df_curr['Итого доходов (руб)'].sum():,.2f}**")
                        st.metric("Записей", f"{len(df_curr):,}")
                    
                    st.markdown("---")
            else:
                # Если нет колонки валюты, показываем общую статистику
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("SBD Трафик превышения", f"{df_revenue['SBD Трафик превышения'].sum():,.2f}")
                    # Разделение по тарифам
                    if 'SBD Трафик SBD-1' in df_revenue.columns:
                        st.metric("SBD Трафик SBD-1", f"{df_revenue['SBD Трафик SBD-1'].sum():,.2f}")
                    if 'SBD Трафик SBD-10' in df_revenue.columns:
                        st.metric("SBD Трафик SBD-10", f"{df_revenue['SBD Трафик SBD-10'].sum():,.2f}")
                    st.metric("SBD Абонплата", f"{df_revenue['SBD Абонплата'].sum():,.2f}")
                    st.metric("SBD Всего", f"{df_revenue['SBD Всего'].sum():,.2f}")
                with col2:
                    st.metric("SUSPEND Абонплата", f"{df_revenue['SUSPEND Абонплата'].sum():,.2f}")
                    st.metric("Мониторинг Абонплата", f"{df_revenue['Мониторинг Абонплата'].sum():,.2f}")
                    st.metric("Блокировка мониторинга", f"{df_revenue['Блокировка мониторинга'].sum():,.2f}")
                with col3:
                    st.metric("Сообщения Абонплата", f"{df_revenue['Сообщения Абонплата'].sum():,.2f}")
                with col4:
                    st.metric("**Итого доходов**", f"**{df_revenue['Итого доходов (руб)'].sum():,.2f}**")
                    st.metric("Записей", f"{len(df_revenue):,}")
            
            st.markdown("---")
            
            # Таблица данных
            display_df_revenue = df_revenue.copy()
            
            # Заполняем NULL пустыми строками для строковых колонок
            for col in display_df_revenue.columns:
                if display_df_revenue[col].dtype == 'object':
                    display_df_revenue[col] = display_df_revenue[col].fillna('')
            
            st.dataframe(display_df_revenue, use_container_width=True, height=400)
            
            # Экспорт
            st.markdown("---")
            col1, col2 = st.columns(2)
            with col1:
                csv_data = export_to_csv(df_revenue)
                st.download_button(
                    label="📥 Download CSV",
                    data=csv_data,
                    file_name=f"iridium_revenue_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            with col2:
                excel_data = export_to_excel(df_revenue)
                st.download_button(
                    label="📥 Download Excel",
                    data=excel_data,
                    file_name=f"iridium_revenue_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        elif df_revenue is not None and df_revenue.empty:
            st.warning("⚠️ Данные не найдены для выбранных фильтров")
        else:
            st.error("❌ Ошибка загрузки данных по доходам")
    
    # ========== ANALYTICS INVOICE PERIOD TAB ==========
    with tab_analytics:
        st.header("📋 Счета за период")
        st.markdown("Отчет по счетам за период на основе таблицы ANALYTICS. Иерархия: клиент → договор → сервис. Группировка по тарифам и зонам.")
        
        # Создаем подвкладки для аналитики
        sub_tab_report, sub_tab_duplicates = st.tabs([
            "📊 Отчет по счетам",
            "🔍 Проверка дубликатов"
        ])
        
        # Используем те же фильтры из sidebar
        period_filter = selected_period  # Фильтр по PERIOD_YYYYMM
        contract_id_filter = contract_id_filter if contract_id_filter else None
        imei_filter = imei_filter if imei_filter else None
        customer_name_filter = customer_name_filter if customer_name_filter else None
        code_1c_filter = code_1c_filter if code_1c_filter else None
        
        # ========== SUB TAB: ОТЧЕТ ПО СЧЕТАМ ==========
        with sub_tab_report:
        
            # Дополнительные фильтры для аналитики
            col1, col2 = st.columns(2)
            with col1:
                tariff_filter = st.text_input(
                    "Tariff ID",
                    value="",
                    key='tariff_filter',
                    help="Фильтр по ID тарифа (BM_TARIFF.TARIFF_ID)"
                )
            with col2:
                zone_filter = st.text_input(
                    "Zone ID",
                    value="",
                    key='zone_filter',
                    help="Фильтр по ID зоны (BM_ZONE.ZONE_ID)"
                )
            
            # Загружаем отчет ТОЛЬКО если выбран период (не "All Periods")
            filter_key = f"analytics_{period_filter}_{contract_id_filter}_{imei_filter}_{customer_name_filter}_{code_1c_filter}_{tariff_filter}_{zone_filter}"
            
            if period_filter is not None:
                if 'last_analytics_key' not in st.session_state or st.session_state.last_analytics_key != filter_key:
                    with st.spinner("Загрузка данных по счетам за период..."):
                        df_analytics = get_analytics_invoice_period_report(
                            period_filter,
                            contract_id_filter,
                            imei_filter,
                            customer_name_filter,
                            code_1c_filter,
                            tariff_filter if tariff_filter else None,
                            zone_filter if zone_filter else None
                        )
                        st.session_state.last_analytics_key = filter_key
                        st.session_state.last_analytics_df = df_analytics
                else:
                    df_analytics = st.session_state.get('last_analytics_df', None)
            else:
                df_analytics = None
                st.info("ℹ️ Выберите период для загрузки отчета по счетам за период")
            
            if df_analytics is not None and not df_analytics.empty:
                st.success(f"✅ Загружено записей: {len(df_analytics):,}")
                
                # Статистика вверху
                st.markdown("---")
                st.subheader("📊 Статистика по счетам за период")
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Всего сумм (руб)", f"{df_analytics['Сумма (руб)'].sum():,.2f}")
                    st.metric("Абонплата (руб)", f"{df_analytics['Абонплата (руб)'].sum():,.2f}")
                with col2:
                    st.metric("Трафик (руб)", f"{df_analytics['Трафик (руб)'].sum():,.2f}")
                    st.metric("В счетах", f"{len(df_analytics[df_analytics['В счете'] == 'Y']):,}")
                with col3:
                    st.metric("Уникальных клиентов", f"{df_analytics['Клиент'].nunique():,}")
                    st.metric("Уникальных договоров", f"{df_analytics['Договор'].nunique():,}")
                with col4:
                    st.metric("Уникальных сервисов", f"{df_analytics['Service ID'].nunique():,}")
                    st.metric("Записей", f"{len(df_analytics):,}")
                
                st.markdown("---")
                
                # Группировка по тарифам и зонам
                st.subheader("📈 Группировка по тарифам и зонам")
                grouping_option = st.selectbox(
                    "Группировка",
                    ["По тарифам", "По зонам", "По тарифам и зонам", "Детализация"],
                    key='analytics_grouping'
                )
                
                if grouping_option == "По тарифам":
                    grouped_df = df_analytics.groupby('Тариф').agg({
                        'Сумма (руб)': 'sum',
                        'Абонплата (руб)': 'sum',
                        'Трафик (руб)': 'sum',
                        'Service ID': 'nunique'
                    }).reset_index()
                    grouped_df.columns = ['Тариф', 'Сумма (руб)', 'Абонплата (руб)', 'Трафик (руб)', 'Кол-во сервисов']
                    st.dataframe(grouped_df, use_container_width=True, height=300)
                elif grouping_option == "По зонам":
                    grouped_df = df_analytics.groupby('Зона').agg({
                        'Сумма (руб)': 'sum',
                        'Абонплата (руб)': 'sum',
                        'Трафик (руб)': 'sum',
                        'Service ID': 'nunique'
                    }).reset_index()
                    grouped_df.columns = ['Зона', 'Сумма (руб)', 'Абонплата (руб)', 'Трафик (руб)', 'Кол-во сервисов']
                    st.dataframe(grouped_df, use_container_width=True, height=300)
                elif grouping_option == "По тарифам и зонам":
                    grouped_df = df_analytics.groupby(['Тариф', 'Зона']).agg({
                        'Сумма (руб)': 'sum',
                        'Абонплата (руб)': 'sum',
                        'Трафик (руб)': 'sum',
                        'Service ID': 'nunique'
                    }).reset_index()
                    grouped_df.columns = ['Тариф', 'Зона', 'Сумма (руб)', 'Абонплата (руб)', 'Трафик (руб)', 'Кол-во сервисов']
                    st.dataframe(grouped_df, use_container_width=True, height=400)
                else:
                    # Детализация - показываем все записи
                    display_df_analytics = df_analytics.copy()
                    
                    # Заполняем NULL пустыми строками для строковых колонок
                    for col in display_df_analytics.columns:
                        if display_df_analytics[col].dtype == 'object':
                            display_df_analytics[col] = display_df_analytics[col].fillna('')
                    
                    st.dataframe(display_df_analytics, use_container_width=True, height=400)
                
                # Экспорт
                st.markdown("---")
                col1, col2 = st.columns(2)
                with col1:
                    csv_data = export_to_csv(df_analytics)
                    st.download_button(
                        label="📥 Download CSV",
                        data=csv_data,
                        file_name=f"analytics_invoice_period_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
                with col2:
                    excel_data = export_to_excel(df_analytics)
                    st.download_button(
                        label="📥 Download Excel",
                        data=excel_data,
                        file_name=f"analytics_invoice_period_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            elif df_analytics is not None and df_analytics.empty:
                st.warning("⚠️ Данные не найдены для выбранных фильтров")
            else:
                st.error("❌ Ошибка загрузки данных по счетам за период")
        
        # ========== SUB TAB: ПРОВЕРКА ДУБЛИКАТОВ ==========
        with sub_tab_duplicates:
            st.header("🔍 Проверка дубликатов в ANALYTICS")
            st.markdown("Поиск записей, где все поля совпадают, кроме AID (первичного ключа).")
            st.info("💡 Дубликаты могут возникать при повторной загрузке данных или ошибках в процессе формирования ANALYTICS.")
            
            # Получаем список периодов для выбора PERIOD_ID
            conn = get_connection()
            if conn:
                try:
                    periods_query = """
                    SELECT 
                        p.PERIOD_ID,
                        TO_CHAR(p.START_DATE, 'YYYY-MM') AS PERIOD_YYYYMM,
                        p.MONTH AS PERIOD_NAME,
                        p.START_DATE,
                        p.STOP_DATE
                    FROM BM_PERIOD p
                    ORDER BY p.PERIOD_ID DESC
                    """
                    periods_df = pd.read_sql_query(periods_query, conn)
                    conn.close()
                    
                    if not periods_df.empty:
                        # Создаем список опций для selectbox
                        period_options = [
                            f"{row['PERIOD_ID']} - {row['PERIOD_YYYYMM']} ({row['PERIOD_NAME']})"
                            for _, row in periods_df.iterrows()
                        ]
                        period_options.insert(0, "Выберите период...")
                        
                        selected_period_option = st.selectbox(
                            "Выберите период (PERIOD_ID) для проверки дубликатов:",
                            period_options,
                            key='duplicates_period_select'
                        )
                        
                        if selected_period_option and selected_period_option != "Выберите период...":
                            # Извлекаем PERIOD_ID из выбранной опции
                            period_id = int(selected_period_option.split(' - ')[0])
                            
                            st.markdown("---")
                            
                            if st.button("🔍 Найти дубликаты", key='find_duplicates_btn'):
                                with st.spinner("Поиск дубликатов..."):
                                    df_duplicates = get_analytics_duplicates(period_id)
                                    
                                    if df_duplicates is not None and not df_duplicates.empty:
                                        st.success(f"✅ Найдено групп дубликатов: {len(df_duplicates)}")
                                        
                                        # Статистика
                                        total_duplicate_records = df_duplicates['DUPLICATE_COUNT'].sum()
                                        total_unique_groups = len(df_duplicates)
                                        
                                        col1, col2, col3 = st.columns(3)
                                        with col1:
                                            st.metric("Групп дубликатов", total_unique_groups)
                                        with col2:
                                            st.metric("Всего дублирующихся записей", total_duplicate_records)
                                        with col3:
                                            st.metric("Максимум дубликатов в группе", df_duplicates['DUPLICATE_COUNT'].max())
                                        
                                        st.markdown("---")
                                        
                                        # Отображаем таблицу дубликатов
                                        display_columns = [
                                            'DUPLICATE_COUNT', 'AID_LIST', 'CUSTOMER_ID', 'CUSTOMER_NAME', 'CODE_1C',
                                            'CONTRACT_ID', 'SERVICE_ID', 'VSAT', 'RESOURCE_MNEMONIC',
                                            'RESOURCE_NAME', 'TARIFF_NAME', 'ZONE_NAME', 'MONEY',
                                            'PRICE', 'TRAF', 'INVOICE_ITEM_ID'
                                        ]
                                        
                                        # Фильтруем только существующие колонки
                                        available_columns = [col for col in display_columns if col in df_duplicates.columns]
                                        
                                        # Переименовываем для отображения
                                        rename_dict = {
                                            'DUPLICATE_COUNT': 'Кол-во дубликатов',
                                            'AID_LIST': 'AID (список)',
                                            'CUSTOMER_ID': 'Customer ID',
                                            'CUSTOMER_NAME': 'Клиент',
                                            'CODE_1C': 'Код 1С',
                                            'CONTRACT_ID': 'Contract ID',
                                            'SERVICE_ID': 'Service ID',
                                            'VSAT': 'IMEI',
                                            'RESOURCE_MNEMONIC': 'Тип ресурса (мнемоника)',
                                            'RESOURCE_NAME': 'Тип ресурса',
                                            'TARIFF_NAME': 'Тариф',
                                            'ZONE_NAME': 'Зона',
                                            'MONEY': 'Сумма',
                                            'PRICE': 'Цена',
                                            'TRAF': 'Трафик',
                                            'INVOICE_ITEM_ID': 'Invoice Item ID'
                                        }
                                        
                                        display_df = df_duplicates[available_columns].copy()
                                        display_df = display_df.rename(columns=rename_dict)
                                        
                                        st.dataframe(display_df, use_container_width=True, height=400)
                                        
                                        # Экспорт
                                        st.markdown("---")
                                        col1, col2 = st.columns(2)
                                        with col1:
                                            csv_data = export_to_csv(df_duplicates)
                                            st.download_button(
                                                label="📥 Download CSV",
                                                data=csv_data,
                                                file_name=f"analytics_duplicates_period_{period_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                                mime="text/csv"
                                            )
                                        with col2:
                                            excel_data = export_to_excel(df_duplicates)
                                            st.download_button(
                                                label="📥 Download Excel",
                                                data=excel_data,
                                                file_name=f"analytics_duplicates_period_{period_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                            )
                                    elif df_duplicates is not None and df_duplicates.empty:
                                        st.success("✅ Дубликаты не найдены для выбранного периода!")
                                    else:
                                        st.error("❌ Ошибка при поиске дубликатов")
                    else:
                        st.warning("⚠️ Периоды не найдены в базе данных")
                except Exception as e:
                    st.error(f"❌ Ошибка получения списка периодов: {e}")
                    import traceback
                    with st.expander("Детали ошибки"):
                        st.code(traceback.format_exc())
                finally:
                    if 'conn' in locals() and conn:
                        try:
                            conn.close()
                        except:
                            pass
            else:
                st.error("❌ Ошибка подключения к базе данных")
    
    # ========== DATA LOADER TAB ==========
    with tab_loader:
        st.header("📥 Data Loader")
        st.markdown("Загрузка и импорт данных Иридиум (трафик и финансовые файлы)")
        
        # Expander с комментарием к процедуре загрузки
        with st.expander("ℹ️ О процедуре загрузки документов (CSV) в базу", expanded=False):
            st.markdown("""
            **Процедура загрузки CSV файлов:**
            
            1. **Автоматическое определение типа файла:**
               - Файлы с именами, содержащими "spnet" или "traffic" → загружаются как SPNet
               - Файлы с именами, содержащими "steccom", "access" или "fee" → загружаются как STECCOM
            
            2. **Автоматическое сохранение:**
               - SPNet файлы сохраняются в `data/SPNet reports/`
               - STECCOM файлы сохраняются в `data/STECCOMLLCRussiaSBD.AccessFees_reports/`
            
            3. **Проверка дубликатов:**
               - Система автоматически проверяет, был ли файл уже загружен
               - Уже загруженные файлы пропускаются
               - Неполные загрузки перезагружаются автоматически
            
            4. **Типы данных:**
               - **SPNet**: данные об использовании трафика (CSV/Excel)
               - **STECCOM**: финансовые данные из инвойсов (CSV/Excel)
            
            5. **После загрузки:**
               - Обновите вкладку "Report" для просмотра новых данных
               - Данные автоматически попадают в базу данных Oracle
            
            **Формат файлов:**
            - Поддерживаются форматы: CSV, XLSX
            - Файлы должны соответствовать структуре таблиц SPNET_TRAFFIC или STECCOM_EXPENSES
            """)
        
        st.markdown("---")
        
        # Директории для данных
        from pathlib import Path
        DATA_DIR = Path(__file__).parent / 'data'
        SPNET_DIR = DATA_DIR / 'SPNet reports'
        ACCESS_FEES_DIR = DATA_DIR / 'STECCOMLLCRussiaSBD.AccessFees_reports'
        
        # Функция для определения типа файла по имени
        def detect_file_type(filename):
            """Определяет тип файла (SPNet или STECCOM) по имени"""
            filename_lower = filename.lower()
            if 'spnet' in filename_lower or 'traffic' in filename_lower:
                return 'SPNet'
            elif 'steccom' in filename_lower or 'access' in filename_lower or 'fee' in filename_lower:
                return 'STECCOM'
            return None
        
        st.markdown("---")
        
        # Универсальный загрузчик файлов - автоматически определяет тип по имени
        st.subheader("📤 Upload File")
        uploaded_file = st.file_uploader(
            "📤 Upload file (drag & drop) - автоматически определит тип по имени",
            type=['csv', 'xlsx'],
            key='file_uploader',
            help="Файлы автоматически сохраняются в нужную директорию на основе имени файла"
        )
        
        if uploaded_file:
            # Автоматически определяем тип файла
            file_type = detect_file_type(uploaded_file.name)
            
            if file_type == 'SPNet':
                target_dir = SPNET_DIR
                file_type_msg = "✅ **Определен как SPNet файл** - будет сохранен в SPNet reports"
            elif file_type == 'STECCOM':
                target_dir = ACCESS_FEES_DIR
                file_type_msg = "✅ **Определен как Access Fees файл** - будет сохранен в Access Fees directory"
            else:
                # Если не удалось определить, спрашиваем пользователя
                file_type = st.radio(
                    "Не удалось определить тип файла. Выберите тип:",
                    ["SPNet Traffic", "Access Fees (Financial)"],
                    horizontal=True,
                    key='file_type_selector'
                )
                if file_type == "SPNet Traffic":
                    target_dir = SPNET_DIR
                    file_type_msg = "⚠️ **Выбран SPNet** - будет сохранен в SPNet reports"
                else:
                    target_dir = ACCESS_FEES_DIR
                    file_type_msg = "⚠️ **Выбран Access Fees** - будет сохранен в Access Fees directory"
            
            if file_type:
                st.info(file_type_msg)
                save_path = target_dir / uploaded_file.name
                
                if save_path.exists():
                    st.warning(f"⚠️ File `{uploaded_file.name}` already exists")
                else:
                    # Используем form для изоляции процесса сохранения
                    with st.form(key='save_file_form', clear_on_submit=True):
                        if st.form_submit_button("💾 Save File", use_container_width=True):
                            try:
                                with st.spinner("Saving file..."):
                                    target_dir.mkdir(parents=True, exist_ok=True)
                                    with open(save_path, 'wb') as f:
                                        f.write(uploaded_file.getbuffer())
                                st.success(f"✅ File saved to {target_dir.name}/: {uploaded_file.name}")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error saving: {e}")
        
        st.markdown("---")
        
        # Показываем оба типа файлов в двух колонках
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 SPNet Traffic Reports")
            st.markdown(f"**Directory:** `{SPNET_DIR}`")
            
            # Список существующих файлов с статусом загрузки
            if SPNET_DIR.exists():
                spnet_files = list(SPNET_DIR.glob("*.csv")) + list(SPNET_DIR.glob("*.xlsx"))
                if spnet_files:
                    # Кэшируем список загруженных файлов, чтобы не делать запрос при каждом rerun
                    cache_key = 'spnet_loaded_files'
                    if cache_key not in st.session_state:
                        # Получаем список загруженных файлов из load_logs только один раз
                        conn_status = get_connection()
                        loaded_files = set()
                        if conn_status:
                            try:
                                cursor = conn_status.cursor()
                                # Определяем структуру таблицы LOAD_LOGS
                                try:
                                    test_query = "SELECT FILE_NAME FROM LOAD_LOGS WHERE ROWNUM = 1"
                                    cursor.execute(test_query)
                                    file_col = "FILE_NAME"
                                except:
                                    try:
                                        test_query = "SELECT SOURCE_FILE FROM LOAD_LOGS WHERE ROWNUM = 1"
                                        cursor.execute(test_query)
                                        file_col = "SOURCE_FILE"
                                    except:
                                        file_col = "FILE_NAME"  # по умолчанию
                                
                                cursor.execute(f"""
                                    SELECT LOWER({file_col}) FROM LOAD_LOGS 
                                    WHERE UPPER(TABLE_NAME) = 'SPNET_TRAFFIC' 
                                    AND LOAD_STATUS = 'SUCCESS'
                                """)
                                loaded_files = {row[0] for row in cursor.fetchall()}
                                cursor.close()
                                st.session_state[cache_key] = loaded_files
                            except:
                                st.session_state[cache_key] = set()
                            finally:
                                conn_status.close()
                        else:
                            st.session_state[cache_key] = set()
                    else:
                        loaded_files = st.session_state[cache_key]
                    
                    st.markdown(f"**Found files: {len(spnet_files)}**")
                    files_info = []
                    for f in sorted(spnet_files, key=lambda x: x.stat().st_mtime, reverse=True)[:10]:
                        is_loaded = f.name.lower() in loaded_files
                        
                        # Подсчитываем записи в файле
                        records_in_file = count_file_records(f)
                        records_in_file_str = f"{records_in_file:,}" if records_in_file is not None else "N/A"
                        
                        # Получаем количество записей в базе
                        records_in_db = None
                        if is_loaded:
                            records_in_db = get_records_in_db(f.name, 'SPNET_TRAFFIC')
                        records_in_db_str = f"{records_in_db:,}" if records_in_db is not None and records_in_db > 0 else "-"
                        
                        files_info.append({
                            'File Name': f.name,
                            'Size (MB)': round(f.stat().st_size / (1024 * 1024), 2),
                            'Records in File': records_in_file_str,
                            'Records in DB': records_in_db_str,
                            'Modified': datetime.fromtimestamp(f.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                            'Status': '✅ Loaded' if is_loaded else '⏳ Not loaded'
                        })
                    df_files = pd.DataFrame(files_info)
                    st.dataframe(df_files, use_container_width=True, hide_index=True, height=200)
                    
                    # Кнопка для обновления списка загруженных файлов
                    if st.button("🔄 Refresh Load Status", key='refresh_spnet_status'):
                        if cache_key in st.session_state:
                            del st.session_state[cache_key]
                        st.rerun()
                else:
                    st.info("📁 Directory is empty")
            else:
                st.info(f"📁 Directory does not exist: {SPNET_DIR}")
        
        with col2:
            st.subheader("💰 Access Fees (Financial)")
            st.markdown(f"**Directory:** `{ACCESS_FEES_DIR}`")
            
            # Список существующих файлов с статусом загрузки
            if ACCESS_FEES_DIR.exists():
                access_fees_files = list(ACCESS_FEES_DIR.glob("*.csv"))
                if access_fees_files:
                    # Кэшируем список загруженных файлов
                    cache_key = 'access_fees_loaded_files'
                    if cache_key not in st.session_state:
                        conn_status = get_connection()
                        loaded_files = set()
                        if conn_status:
                            try:
                                cursor = conn_status.cursor()
                                # Определяем структуру таблицы LOAD_LOGS
                                try:
                                    test_query = "SELECT FILE_NAME FROM LOAD_LOGS WHERE ROWNUM = 1"
                                    cursor.execute(test_query)
                                    file_col = "FILE_NAME"
                                except:
                                    try:
                                        test_query = "SELECT SOURCE_FILE FROM LOAD_LOGS WHERE ROWNUM = 1"
                                        cursor.execute(test_query)
                                        file_col = "SOURCE_FILE"
                                    except:
                                        file_col = "FILE_NAME"  # по умолчанию
                                
                                cursor.execute(f"""
                                    SELECT LOWER({file_col}) FROM LOAD_LOGS 
                                    WHERE UPPER(TABLE_NAME) = 'STECCOM_EXPENSES' 
                                    AND LOAD_STATUS = 'SUCCESS'
                                """)
                                loaded_files = {row[0] for row in cursor.fetchall()}
                                cursor.close()
                                st.session_state[cache_key] = loaded_files
                            except:
                                st.session_state[cache_key] = set()
                            finally:
                                conn_status.close()
                        else:
                            st.session_state[cache_key] = set()
                    else:
                        loaded_files = st.session_state[cache_key]
                    
                    st.markdown(f"**Found files: {len(access_fees_files)}**")
                    files_info = []
                    for f in sorted(access_fees_files, key=lambda x: x.stat().st_mtime, reverse=True)[:10]:
                        is_loaded = f.name.lower() in loaded_files
                        
                        # Подсчитываем записи в файле
                        records_in_file = count_file_records(f)
                        records_in_file_str = f"{records_in_file:,}" if records_in_file is not None else "N/A"
                        
                        # Получаем количество записей в базе
                        records_in_db = None
                        if is_loaded:
                            records_in_db = get_records_in_db(f.name, 'STECCOM_EXPENSES')
                        records_in_db_str = f"{records_in_db:,}" if records_in_db is not None and records_in_db > 0 else "-"
                        
                        files_info.append({
                            'File Name': f.name,
                            'Size (MB)': round(f.stat().st_size / (1024 * 1024), 2),
                            'Records in File': records_in_file_str,
                            'Records in DB': records_in_db_str,
                            'Modified': datetime.fromtimestamp(f.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                            'Status': '✅ Loaded' if is_loaded else '⏳ Not loaded'
                        })
                    df_files = pd.DataFrame(files_info)
                    st.dataframe(df_files, use_container_width=True, hide_index=True, height=200)
                    
                    # Кнопка для обновления списка загруженных файлов
                    if st.button("🔄 Refresh Load Status", key='refresh_access_fees_status'):
                        if cache_key in st.session_state:
                            del st.session_state[cache_key]
                        st.rerun()
                else:
                    st.info("📁 Directory is empty")
            else:
                st.info(f"📁 Directory does not exist: {ACCESS_FEES_DIR}")
        
        st.markdown("---")
        st.subheader("🔄 Import to Database")
        
        # Импорт данных в базу - одна кнопка для обоих типов файлов
        if st.button("📥 Import All Files", use_container_width=True, type="primary"):
            oracle_config = {
                'username': ORACLE_USER,
                'password': ORACLE_PASSWORD,
                'host': ORACLE_HOST,
                'port': ORACLE_PORT,
                'service_name': ORACLE_SERVICE if not os.getenv('ORACLE_SID') else None,
                'sid': ORACLE_SID if os.getenv('ORACLE_SID') else None
            }
            
            import io
            import sys
            all_logs = []
            
            # Импорт SPNet
            with st.spinner("Импорт данных SPNet..."):
                try:
                    from python.load_spnet_traffic import SPNetDataLoader
                    
                    loader = SPNetDataLoader(oracle_config)
                    if loader.connect_to_oracle():
                        loader.gdrive_path = str(SPNET_DIR)
                        
                        log_capture = io.StringIO()
                        old_stdout = sys.stdout
                        old_stderr = sys.stderr
                        
                        try:
                            sys.stdout = log_capture
                            sys.stderr = log_capture
                            
                            result = loader.load_spnet_files()
                            log_output = log_capture.getvalue()
                            all_logs.append(("SPNet", result, log_output))
                        finally:
                            sys.stdout = old_stdout
                            sys.stderr = old_stderr
                            if loader.connection:
                                loader.close_connection()
                    else:
                        all_logs.append(("SPNet", False, "❌ Не удалось подключиться к базе данных"))
                except Exception as e:
                    import traceback
                    all_logs.append(("SPNet", False, f"❌ Ошибка: {e}\n{traceback.format_exc()}"))
            
            # Импорт Access Fees
            with st.spinner("Импорт данных Access Fees..."):
                try:
                    from python.load_steccom_expenses import STECCOMDataLoader
                    
                    loader = STECCOMDataLoader(oracle_config)
                    if loader.connect_to_oracle():
                        loader.gdrive_path = str(ACCESS_FEES_DIR)
                        
                        log_capture = io.StringIO()
                        old_stdout = sys.stdout
                        old_stderr = sys.stderr
                        
                        try:
                            sys.stdout = log_capture
                            sys.stderr = log_capture
                            
                            result = loader.load_steccom_files()
                            log_output = log_capture.getvalue()
                            all_logs.append(("Access Fees", result, log_output))
                        finally:
                            sys.stdout = old_stdout
                            sys.stderr = old_stderr
                            if loader.connection:
                                loader.close_connection()
                    else:
                        all_logs.append(("Access Fees", False, "❌ Не удалось подключиться к базе данных"))
                except Exception as e:
                    import traceback
                    all_logs.append(("Access Fees", False, f"❌ Ошибка: {e}\n{traceback.format_exc()}"))
            
            # Показываем результаты
            for file_type, success, log_output in all_logs:
                if success:
                    st.success(f"✅ Импорт {file_type} завершен успешно!")
                else:
                    st.error(f"❌ Ошибка импорта {file_type}")
                if log_output:
                    st.text_area(f"{file_type} Log", log_output, height=150, key=f'log_{file_type.lower().replace(" ", "_")}')
        
        st.markdown("---")
        st.caption("💡 **Tip:** After importing, refresh the Report tab to see updated data")


if __name__ == "__main__":
    main()

