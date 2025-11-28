#!/usr/bin/env python3
"""
Streamlit отчет по превышению трафика Iridium M2M
Версия для Oracle Database (backup)
"""

import streamlit as st
import pandas as pd
import cx_Oracle
from datetime import datetime
import io
import os
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
ORACLE_USER = os.getenv('ORACLE_USER', 'billing7')
ORACLE_PASSWORD = os.getenv('ORACLE_PASSWORD')
ORACLE_HOST = os.getenv('ORACLE_HOST')
ORACLE_PORT = int(os.getenv('ORACLE_PORT', '1521'))

# Проверка обязательных параметров
if not all([ORACLE_USER, ORACLE_PASSWORD, ORACLE_HOST]):
    st.error("❌ Ошибка: Не установлены переменные окружения ORACLE_USER, ORACLE_PASSWORD и ORACLE_HOST")
    st.error("Установите их в config.env или через переменные окружения")
    st.stop()
ORACLE_SID = os.getenv('ORACLE_SID', 'bm7')
# Если задан ORACLE_SERVICE, используем его, иначе ORACLE_SID
ORACLE_SERVICE = os.getenv('ORACLE_SERVICE') or os.getenv('ORACLE_SID', 'bm7')


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
    base_query = """
    SELECT 
        v.FINANCIAL_PERIOD AS "Отчетный Период",
        v.BILL_MONTH AS "Bill Month",
        v.IMEI AS "IMEI",
        v.CONTRACT_ID AS "Contract ID",
        -- Доп. поля из биллинга (после Contract ID)
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
def get_periods():
    """Получение списка периодов (возвращаем FINANCIAL_PERIOD для отображения и фильтрации)"""
    try:
        conn = get_connection()
        if not conn:
            return []
        
        # Используем FINANCIAL_PERIOD для отображения и фильтрации (Отчетный период на месяц меньше BILL_MONTH)
        query = """
        SELECT DISTINCT 
            FINANCIAL_PERIOD AS display_period
        FROM V_CONSOLIDATED_REPORT_WITH_BILLING
        WHERE FINANCIAL_PERIOD IS NOT NULL
        ORDER BY FINANCIAL_PERIOD DESC
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
        page_title="Iridium M2M Overage Report (Oracle)",
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
    st.title("📊 Iridium M2M Overage Report")
    st.markdown("**Oracle Database | All Plans (Calculated Overage for SBD-1 and SBD-10 only)**")
    
    # Expander с комментарием к отчету
    with st.expander("ℹ️ О комментарии к отчету", expanded=False):
        st.markdown("""
        **Описание отчета:**
        
        Этот отчет объединяет данные из двух источников:
        
        1. **SPNet** - данные об использовании трафика:
           - `Traffic Usage (KB)` - объем использованного трафика
           - `Overage (KB)` - превышение включенного трафика
           - `Calculated Overage ($)` - рассчитанная стоимость превышения (только для SBD-1 и SBD-10)
        
        2. **STECCOM** - данные из биллинга:
           - `Plan Monthly` - название активного тарифного плана
           - `Plan Suspended` - название приостановленного тарифного плана
        
        **Важно:**
        - Каждая строка отчета = отдельный период (BILL_MONTH)
        - Периоды НЕ суммируются
        - Расчет превышения выполняется только для активных тарифов SBD-1 и SBD-10
        - Данные группируются по IMEI + CONTRACT_ID + BILL_MONTH
        
        **Логика периодов STECCOM:**
        - Файл `STECCOMLLCRussiaSBD.AccessFees.20250702.csv` содержит счета за период с 2 июня по 1 июля включительно
        - Дата в имени файла (20250702) - это дата окончания периода
        - Для отчета за июнь (202506) используется файл с датой 20250702
        - Система автоматически вычитает один месяц из даты файла для правильного сопоставления периодов
        """)
    
    st.markdown("---")
    
    # Фильтры в боковой панели (вне вкладок, чтобы были доступны всегда)
    with st.sidebar:
        st.header("⚙️ Filters")
        
        # Кэшируем периоды и планы, чтобы не делать запросы при каждом rerun
        periods_data = get_periods()
        
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
        
        # По умолчанию выбираем последний период (первый в отсортированном списке)
        if 'selected_period_index' not in st.session_state:
            st.session_state.selected_period_index = 0  # 0 = последний период (не "All Periods")
        
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
        
        plans = get_plans()
        plan_options = ["All Plans"] + plans
        selected_plan = st.selectbox("Plan", plan_options, key='plan_selectbox')
        
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
    
    # Создаем вкладки для отчета, доходов и загрузки данных
    tab_report, tab_revenue, tab_loader = st.tabs(["💰 Расходы Иридиум", "💰 Доходы", "📥 Data Loader"])
    
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
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Всего записей", f"{len(df):,}")
            with col2:
                total_overage = df["Calculated Overage ($)"].sum()
                st.metric("Total Overage", f"${total_overage:,.2f}")
            with col3:
                total_advance_prev = df["Advance Charge Previous Month"].sum()
                st.metric("Advance Charge Previous Month", f"${total_advance_prev:,.2f}")
            
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

