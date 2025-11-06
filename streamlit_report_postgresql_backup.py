#!/usr/bin/env python3
"""
Streamlit отчет по превышению трафика Iridium M2M
Расчет только для SBD-1 и SBD-10
"""

import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime
import io
import os
from pathlib import Path

# Попытка загрузить config.env если переменные окружения не установлены
def load_config_env():
    """Загрузка config.env если переменные окружения не установлены"""
    if not os.getenv('POSTGRES_PASSWORD'):
        config_file = Path(__file__).parent / 'config.env'
        if config_file.exists():
            with open(config_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip().strip('"\'')
                        if key.startswith('POSTGRES_') and not os.getenv(key):
                            os.environ[key] = value

# Загружаем config.env если нужно
load_config_env()

# Конфигурация базы данных
# Загружается из config.env через run_streamlit.sh или автоматически из config.env
DB_CONFIG = {
    'dbname': os.getenv('POSTGRES_DB', 'billing'),
    'user': os.getenv('POSTGRES_USER', 'cnn'),
    'password': os.getenv('POSTGRES_PASSWORD', ''),
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432'))
}


def get_connection():
    """Создание подключения к базе данных"""
    try:
        if not DB_CONFIG['password']:
            st.error("⚠️ Пароль не установлен! Убедитесь, что config.env загружен через run_streamlit.sh")
            return None
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        st.error(f"Ошибка подключения к базе данных: {e}")
        st.info(f"Проверьте конфигурацию: {DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
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


def get_records_in_db(file_name, table_name='spnet_traffic'):
    """Получить количество записей в базе для файла"""
    conn = get_connection()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM {} 
            WHERE LOWER(source_file) = LOWER(%s)
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
        # bill_month в PostgreSQL уже в формате "YYYY-MM" (текст)
        period_condition = f"AND v.bill_month = '{period_filter}'"
    
    # Фильтр по тарифам (все тарифы)
    plan_condition = ""
    if plan_filter and plan_filter != "All Plans":
        plan_condition = f"AND v.plan_name = '{plan_filter}'"
    
    query = f"""
    SELECT 
        v.bill_month AS "Bill Month",
        v.imei AS "IMEI",
        v.contract_id AS "Contract ID",
        -- Доп. поля из биллинга (после Contract ID)
        v.display_name         AS "Organization/Person",
        v.code_1c              AS "Code 1C",
        v.service_id           AS "Service ID",
        v.agreement_number     AS "Agreement #",
        COALESCE(v.steccom_plan_name_monthly, '') AS "Plan Monthly",
        COALESCE(v.steccom_plan_name_suspended, '') AS "Plan Suspended",
        -- Разделение трафика и событий
        ROUND(CAST(v.traffic_usage_bytes AS NUMERIC) / 1000, 2) AS "Traffic Usage (KB)",
        v.events_count AS "Events (Count)",
        v.data_usage_events AS "Data Events",
        v.mailbox_events AS "Mailbox Events",
        v.registration_events AS "Registration Events",
        -- Превышения: зануляем суммы Iridium для СТЭК.КОМ
        CASE 
            WHEN UPPER(COALESCE(v.display_name, '')) LIKE '%СТЭК.КОМ%' 
                 OR UPPER(COALESCE(v.display_name, '')) LIKE '%СТЭККОМ%'
                 OR UPPER(COALESCE(v.display_name, '')) LIKE '%STECCOM%'
            THEN 0
            ELSE v.overage_kb
        END AS "Overage (KB)",
        CASE 
            WHEN UPPER(COALESCE(v.display_name, '')) LIKE '%СТЭК.КОМ%' 
                 OR UPPER(COALESCE(v.display_name, '')) LIKE '%СТЭККОМ%'
                 OR UPPER(COALESCE(v.display_name, '')) LIKE '%STECCOM%'
            THEN 0
            ELSE v.calculated_overage
        END AS "Calculated Overage ($)",
        CASE 
            WHEN UPPER(COALESCE(v.display_name, '')) LIKE '%СТЭК.КОМ%' 
                 OR UPPER(COALESCE(v.display_name, '')) LIKE '%СТЭККОМ%'
                 OR UPPER(COALESCE(v.display_name, '')) LIKE '%STECCOM%'
            THEN 0
            ELSE v.spnet_total_amount
        END AS "SPNet Total Amount ($)",
        -- Fees из STECCOM_EXPENSES (убрали префикс "Fee:")
        COALESCE(v.fee_activation_fee, 0) AS "Activation Fee",
        COALESCE(v.fee_advance_charge, 0) AS "Advance Charge",
        COALESCE(v.fee_credit, 0) AS "Credit",
        COALESCE(v.fee_credited, 0) AS "Credited",
        COALESCE(v.fee_prorated, 0) AS "Prorated"
    FROM v_consolidated_report_with_billing v
    WHERE 1=1
        {plan_condition}
        {period_condition}
    ORDER BY v.bill_month DESC, "Calculated Overage ($)" DESC NULLS LAST
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            return df
        
        # Fees уже в VIEW, дополнительные действия не требуются
        # Bill Month уже в формате "YYYY-MM" (текст), не нужно форматировать
        if 'Bill Month' in df.columns:
            df['Bill Month'] = df['Bill Month'].astype(str).replace('nan', '')
        
        return df
    except Exception as e:
        st.error(f"Ошибка получения отчета: {e}")
        return None
    finally:
        conn.close()


@st.cache_data(ttl=300)  # Кэшируем на 5 минут
def get_periods():
    """Получение списка периодов"""
    conn = get_connection()
    if not conn:
        return []
    
    # Используем v_consolidated_report_with_billing, где bill_month уже в формате "YYYY-MM"
    query = """
    SELECT DISTINCT bill_month
    FROM v_consolidated_report_with_billing
    WHERE bill_month IS NOT NULL
    ORDER BY bill_month DESC
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        periods = []
        for row in cursor.fetchall():
            if row[0]:
                # bill_month уже в формате "YYYY-MM"
                periods.append(str(row[0]))
        return periods
    except Exception as e:
        st.error(f"Ошибка получения периодов: {e}")
        return []
    finally:
        conn.close()


@st.cache_data(ttl=300)  # Кэшируем на 5 минут
def get_plans():
    """Получение списка тарифных планов"""
    conn = get_connection()
    if not conn:
        return []
    
    query = """
    SELECT DISTINCT plan_name
    FROM v_consolidated_report_with_billing
    WHERE plan_name IS NOT NULL
    ORDER BY plan_name
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        plans = [row[0] for row in cursor.fetchall() if row[0]]
        return plans
    except Exception as e:
        st.error(f"Ошибка получения планов: {e}")
        return []
    finally:
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
        page_title="Iridium M2M Overage Report (SBD-1, SBD-10)",
        page_icon="📊",
        layout="wide"
    )
    
    # Проверка загрузки конфигурации
    if not DB_CONFIG.get('password'):
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
    st.markdown("**All Plans (Calculated Overage for SBD-1 and SBD-10 only)**")
    st.markdown("---")
    
    # Создаем вкладки для отчета и загрузки данных
    # Фильтры в боковой панели (вне вкладок, чтобы были доступны всегда)
    with st.sidebar:
        st.header("⚙️ Filters")
        
        # Период
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
        
        # Тарифный план
        plans = get_plans()
        plan_options = ["All Plans"] + plans
        selected_plan = st.selectbox("Plan", plan_options, key='plan_selectbox')
        
        st.markdown("---")
        st.header("🔐 Database Connection")
        st.caption(f"📡 {DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
        
        # Кнопка тестирования подключения
        if st.button("🔌 Test Connection", key='test_connection_btn'):
            test_conn = get_connection()
            if test_conn:
                st.success("✅ Подключение успешно!")
                test_conn.close()
            else:
                st.error("❌ Ошибка подключения. Проверьте config.env")
        
        st.info("💡 Конфигурация загружается из config.env при запуске через run_streamlit.sh")
    
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
            
            # Убеждаемся, что все колонки видны, даже если они NULL
            display_df = df.copy()
            
            # Заполняем NULL пустыми строками для строковых колонок
            for col in display_df.columns:
                if display_df[col].dtype == 'object':  # строковые колонки
                    display_df[col] = display_df[col].fillna('')
            
            # Убеждаемся, что Code 1C всегда присутствует (на случай если pandas скрыл её)
            if 'Code 1C' in df.columns:
                # Колонка есть, просто заполняем NULL
                display_df['Code 1C'] = display_df['Code 1C'].fillna('')
            else:
                # Колонка отсутствует - добавляем (не должно случиться, но на всякий случай)
                display_df['Code 1C'] = ''
            
            # Таблица
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
                height=600
            )
            
            # Экспорт
            st.markdown("---")
            st.subheader("💾 Export")
            
            col1, col2 = st.columns(2)
            
            with col1:
                csv_data = export_to_csv(df)
                st.download_button(
                    label="📥 Download CSV",
                    data=csv_data,
                    file_name=f"iridium_overage_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            
            with col2:
                try:
                    excel_data = export_to_excel(df)
                    st.download_button(
                        label="📥 Download Excel",
                        data=excel_data,
                        file_name=f"iridium_overage_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                except Exception as e:
                    st.warning(f"Excel export unavailable: {e}")

            # Детализация плат по категориям (30-дневные циклы со 2-го по 2-е)
            st.markdown("---")
            st.subheader("🔎 Fees breakdown (by category)")
            contract_filter = st.text_input("Filter by Contract ID (optional)", value="")

            # Фильтруем только контракты из основного отчета
            contract_ids = df['Contract ID'].dropna().unique().tolist() if not df.empty else []
            
            # Если указан фильтр, добавляем его
            if contract_filter.strip():
                contract_ids.append(contract_filter.strip())
            
            # Подключаемся БЕЗ фильтра по всем контрактам, если их слишком много
            # Ограничиваем фильтрацию для избежания слишком длинных запросов
            conn2 = get_connection()
            if conn2:
                contract_condition = ""
                
                if contract_ids:
                    # Ограничиваем количество контрактов в запросе (макс 200)
                    limited_contract_ids = contract_ids[:200]
                    
                    # Если контрактов много (>100), используем временную таблицу
                    if len(contract_ids) > 100:
                        try:
                            cursor = conn2.cursor()
                            # Создаем временную таблицу
                            cursor.execute("DROP TABLE IF EXISTS temp_contract_filter")
                            cursor.execute("CREATE TEMP TABLE temp_contract_filter (contract_id TEXT)")
                            
                            # Вставляем значения через executemany
                            insert_data = [(str(c),) for c in limited_contract_ids]
                            cursor.executemany(
                                "INSERT INTO temp_contract_filter VALUES (%s)",
                                insert_data
                            )
                            conn2.commit()
                            cursor.close()
                            
                            contract_condition = "AND f.contract_id IN (SELECT contract_id FROM temp_contract_filter)"
                        except Exception as e:
                            st.warning(f"Ошибка создания временной таблицы: {e}. Используем прямое условие (ограничено 100 контрактами).")
                            # Fallback на прямое условие (ограниченное)
                            contract_list = "', '".join([str(c).replace("'", "''") for c in limited_contract_ids[:100]])
                            contract_condition = f"AND f.contract_id IN ('{contract_list}')"
                    else:
                        # Если контрактов немного, используем обычный IN (экранируем кавычки)
                        contract_list = "', '".join([str(c).replace("'", "''") for c in contract_ids])
                        contract_condition = f"AND f.contract_id IN ('{contract_list}')"
                
                # Фильтр по периоду (если выбран период в основном отчете)
                period_condition = ""
                if period_filter and period_filter != "All Periods":
                    year, month = period_filter.split('-')
                    # Ищем fees с периодами близкими к выбранному месяцу (формат YYYYMMDD)
                    # Например, для 2025-05 ищем периоды начинающиеся с 202505 или 202506
                    period_pattern = f"{year}{month:0>2}"
                    period_condition = f"AND f.fee_period_date::text LIKE '{period_pattern}%'"

                fees_q = f"""
                SELECT 
                    f.fee_period_date AS "Period",
                    f.contract_id AS "Contract ID",
                    f.imei AS "IMEI",
                    f.category AS "Category", 
                    SUM(f.amount) AS "Amount"
                FROM v_steccom_access_fees_norm f
                WHERE f.imei IS NOT NULL
                {contract_condition}
                {period_condition}
                GROUP BY f.fee_period_date, f.contract_id, f.imei, f.category
                ORDER BY f.fee_period_date DESC, f.contract_id, f.imei, f.category
                """
                try:
                    fees_detail_df = pd.read_sql_query(fees_q, conn2)
                    
                    # Форматируем период: 20250302 -> "2025-03-02"
                    if 'Period' in fees_detail_df.columns and not fees_detail_df.empty:
                        fees_detail_df['Period'] = fees_detail_df['Period'].apply(
                            lambda x: f"{str(x)[:4]}-{str(x)[4:6]}-{str(x)[6:8]}" if pd.notna(x) and len(str(x)) >= 8 else str(x)
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
            st.warning("⚠️ No data found with selected filters")
        else:
            st.error("❌ Error loading data")
    
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
                                cursor.execute("""
                                    SELECT LOWER(source_file) FROM load_logs 
                                    WHERE LOWER(table_name) = LOWER('spnet_traffic') 
                                    AND load_status = 'SUCCESS'
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
                            records_in_db = get_records_in_db(f.name, 'spnet_traffic')
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
                steccom_files = list(ACCESS_FEES_DIR.glob("*.csv"))
                if steccom_files:
                    # Кэшируем список загруженных файлов, чтобы не делать запрос при каждом rerun
                    cache_key = 'steccom_loaded_files'
                    if cache_key not in st.session_state:
                        # Получаем список загруженных файлов из load_logs только один раз
                        conn_status = get_connection()
                        loaded_files = set()
                        if conn_status:
                            try:
                                cursor = conn_status.cursor()
                                cursor.execute("""
                                    SELECT LOWER(source_file) FROM load_logs 
                                    WHERE LOWER(table_name) = LOWER('steccom_expenses') 
                                    AND load_status = 'SUCCESS'
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
                    
                    st.markdown(f"**Found files: {len(steccom_files)}**")
                    files_info = []
                    for f in sorted(steccom_files, key=lambda x: x.stat().st_mtime, reverse=True)[:10]:
                        is_loaded = f.name.lower() in loaded_files
                        
                        # Подсчитываем записи в файле
                        records_in_file = count_file_records(f)
                        records_in_file_str = f"{records_in_file:,}" if records_in_file is not None else "N/A"
                        
                        # Получаем количество записей в базе
                        records_in_db = None
                        if is_loaded:
                            records_in_db = get_records_in_db(f.name, 'steccom_expenses')
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
                    if st.button("🔄 Refresh Load Status", key='refresh_steccom_status'):
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
            import io
            import sys
            all_logs = []
            
            # Импорт SPNet
            with st.spinner("Импорт данных SPNet в PostgreSQL..."):
                try:
                    from python.load_data_postgres import PostgresDataLoader
                    
                    loader = PostgresDataLoader(DB_CONFIG)
                    if loader.connect():
                        loader.spnet_path = str(SPNET_DIR)
                        
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
                                loader.close()
                    else:
                        all_logs.append(("SPNet", False, "❌ Не удалось подключиться к базе данных"))
                except Exception as e:
                    import traceback
                    all_logs.append(("SPNet", False, f"❌ Ошибка: {e}\n{traceback.format_exc()}"))
            
            # Импорт Access Fees
            with st.spinner("Импорт данных Access Fees в PostgreSQL..."):
                try:
                    from python.load_data_postgres import PostgresDataLoader
                    
                    loader = PostgresDataLoader(DB_CONFIG)
                    if loader.connect():
                        loader.steccom_path = str(ACCESS_FEES_DIR)
                        
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
                                loader.close()
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
