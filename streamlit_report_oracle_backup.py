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
ORACLE_PASSWORD = os.getenv('ORACLE_PASSWORD', 'billing')
ORACLE_HOST = os.getenv('ORACLE_HOST', '192.168.3.35')
ORACLE_PORT = int(os.getenv('ORACLE_PORT', '1521'))
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


def get_main_report(period_filter=None, plan_filter=None):
    """Получение основного отчета"""
    conn = get_connection()
    if not conn:
        return None
    
    # Фильтр по периодам
    period_condition = ""
    if period_filter and period_filter != "All Periods":
        # BILL_MONTH в Oracle уже в формате "YYYY-MM" из VIEW
        period_condition = f"AND v.BILL_MONTH = '{period_filter}'"
    
    # Фильтр по тарифам
    plan_condition = ""
    if plan_filter and plan_filter != "All Plans":
        plan_condition = f"AND v.PLAN_NAME = '{plan_filter}'"
    
    query = f"""
    SELECT 
        v.BILL_MONTH AS "Bill Month",
        v.IMEI AS "IMEI",
        v.CONTRACT_ID AS "Contract ID",
        -- Доп. поля из биллинга (после Contract ID)
        COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '') AS "Organization/Person",
        v.CODE_1C AS "Code 1C",
        v.SERVICE_ID AS "Service ID",
        v.AGREEMENT_NUMBER AS "Agreement #",
        COALESCE(v.STECCOM_PLAN_NAME_MONTHLY, '') AS "Plan Monthly",
        COALESCE(v.STECCOM_PLAN_NAME_SUSPENDED, '') AS "Plan Suspended",
        -- Разделение трафика и событий
        ROUND(v.TRAFFIC_USAGE_BYTES / 1000, 2) AS "Traffic Usage (KB)",
        v.EVENTS_COUNT AS "Events (Count)",
        v.DATA_USAGE_EVENTS AS "Data Events",
        v.MAILBOX_EVENTS AS "Mailbox Events",
        v.REGISTRATION_EVENTS AS "Registration Events",
        -- Превышения: зануляем суммы Iridium для СТЭК.КОМ
        CASE 
            WHEN UPPER(COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '')) LIKE '%СТЭК.КОМ%' 
                 OR UPPER(COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '')) LIKE '%СТЭККОМ%'
                 OR UPPER(COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '')) LIKE '%STECCOM%'
            THEN 0
            ELSE v.OVERAGE_KB
        END AS "Overage (KB)",
        CASE 
            WHEN UPPER(COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '')) LIKE '%СТЭК.КОМ%' 
                 OR UPPER(COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '')) LIKE '%СТЭККОМ%'
                 OR UPPER(COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '')) LIKE '%STECCOM%'
            THEN 0
            ELSE v.CALCULATED_OVERAGE
        END AS "Calculated Overage ($)",
        CASE 
            WHEN UPPER(COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '')) LIKE '%СТЭК.КОМ%' 
                 OR UPPER(COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '')) LIKE '%СТЭККОМ%'
                 OR UPPER(COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '')) LIKE '%STECCOM%'
            THEN 0
            ELSE v.SPNET_TOTAL_AMOUNT
        END AS "SPNet Total Amount ($)",
        -- Fees из STECCOM_EXPENSES (убрали префикс "Fee:")
        NVL(v.FEE_ACTIVATION_FEE, 0) AS "Activation Fee",
        NVL(v.FEE_ADVANCE_CHARGE, 0) AS "Advance Charge",
        NVL(v.FEE_CREDIT, 0) AS "Credit",
        NVL(v.FEE_CREDITED, 0) AS "Credited",
        NVL(v.FEE_PRORATED, 0) AS "Prorated"
    FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
    WHERE 1=1
        {plan_condition}
        {period_condition}
    ORDER BY v.BILL_MONTH DESC, "Calculated Overage ($)" DESC NULLS LAST
    """
    
    try:
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


@st.cache_data(ttl=300)  # Кэшируем на 5 минут
def get_periods():
    """Получение списка периодов"""
    conn = get_connection()
    if not conn:
        return []
    
    # Используем V_CONSOLIDATED_REPORT_WITH_BILLING, где BILL_MONTH уже в формате "YYYY-MM"
    query = """
    SELECT DISTINCT BILL_MONTH
    FROM V_CONSOLIDATED_REPORT_WITH_BILLING
    WHERE BILL_MONTH IS NOT NULL
    ORDER BY BILL_MONTH DESC
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        periods = []
        for row in cursor.fetchall():
            if row[0]:
                # BILL_MONTH уже в формате "YYYY-MM"
                periods.append(str(row[0]))
        cursor.close()
        return periods
    except Exception as e:
        st.error(f"Ошибка получения периодов: {e}")
        return []
    finally:
        if conn:
            conn.close()


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


def main():
    """Основная функция приложения"""
    
    # Настройка страницы
    st.set_page_config(
        page_title="Iridium M2M Overage Report (Oracle)",
        page_icon="📊",
        layout="wide"
    )
    
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
    st.markdown("---")
    
    # Фильтры в боковой панели (вне вкладок, чтобы были доступны всегда)
    with st.sidebar:
        st.header("⚙️ Filters")
        
        # Кэшируем периоды и планы, чтобы не делать запросы при каждом rerun
        periods = get_periods()
        
        # По умолчанию выбираем последний период (первый в отсортированном списке)
        if 'selected_period_index' not in st.session_state:
            st.session_state.selected_period_index = 0  # 0 = последний период (не "All Periods")
        
        period_options = periods + ["All Periods"]  # Последний период первым, потом "All Periods"
        selected_period = st.selectbox(
            "Period", 
            period_options,
            index=st.session_state.selected_period_index,
            key='period_selectbox'
        )
        
        # Обновляем индекс при изменении
        if selected_period in period_options:
            st.session_state.selected_period_index = period_options.index(selected_period)
        
        plans = get_plans()
        plan_options = ["All Plans"] + plans
        selected_plan = st.selectbox("Plan", plan_options, key='plan_selectbox')
        
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
    
    # Создаем вкладки для отчета и загрузки данных
    tab_report, tab_loader = st.tabs(["📊 Report", "📥 Data Loader"])
    
    # ========== REPORT TAB ==========
    with tab_report:
        
        period_filter = None if selected_period == "All Periods" else selected_period
        plan_filter = None if selected_plan == "All Plans" else selected_plan
        
        # Загружаем отчет ТОЛЬКО если выбран период (не "All Periods")
        filter_key = f"{period_filter}_{plan_filter}"
        
        # Проверяем, нужно ли загружать отчет
        if period_filter is not None:
            if 'last_report_key' not in st.session_state or st.session_state.last_report_key != filter_key:
                with st.spinner("Loading report data..."):
                    df = get_main_report(period_filter, plan_filter)
                    st.session_state.last_report_key = filter_key
                    st.session_state.last_report_df = df
            else:
                df = st.session_state.last_report_df
        else:
            # Если период не выбран, не загружаем отчет
            df = None
            st.info("ℹ️ Выберите период для загрузки отчета")
        
        if df is not None and not df.empty:
            st.success(f"✅ Загружено записей: {len(df):,}")
            
            # Метрики
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Всего записей", f"{len(df):,}")
            with col2:
                total_overage = df["Calculated Overage ($)"].sum()
                st.metric("Total Overage", f"${total_overage:,.2f}")
            
            st.markdown("---")
            
            # Таблица данных
            st.dataframe(df, use_container_width=True, height=400)
            
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
    
    # ========== DATA LOADER TAB ==========
    with tab_loader:
        st.header("📥 Data Loader")
        st.markdown("Загрузка и импорт данных Иридиум (трафик и финансовые файлы)")
        st.markdown("---")
        
        # Директории для данных
        from pathlib import Path
        DATA_DIR = Path(__file__).parent / 'data'
        SPNET_DIR = DATA_DIR / 'SPNet reports'
        ACCESS_FEES_DIR = DATA_DIR / 'STECCOMLLCRussiaSBD.AccessFees_reports'
        
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

