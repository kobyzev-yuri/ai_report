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


def get_main_report(period_filter=None, plan_filter=None):
    """Получение основного отчета"""
    conn = get_connection()
    if not conn:
        return None
    
    # Фильтр по периодам
    period_condition = ""
    if period_filter and period_filter != "All Periods":
        # Конвертируем YYYY-MM в формат базы YYYYMM (например: 2025-09 -> 202509)
        year, month = period_filter.split('-')
        bill_month = int(year) * 100 + int(month)
        period_condition = f"AND CAST(v.BILL_MONTH AS integer) = {bill_month}"
    
    # Фильтр по тарифам (все тарифы)
    plan_condition = ""
    if plan_filter and plan_filter != "All Plans":
        plan_condition = f"AND v.PLAN_NAME = '{plan_filter}'"
    
    query = f"""
    SELECT 
        v.IMEI AS "IMEI",
        v.CONTRACT_ID AS "Contract ID",
        v.PLAN_NAME AS "Plan Name",
        CAST(v.BILL_MONTH AS integer) AS "Bill Month",
        -- Разделение трафика и событий
        ROUND(CAST(v.TRAFFIC_USAGE_BYTES AS NUMERIC) / 1000, 2) AS "Traffic Usage (KB)",
        v.EVENTS_COUNT AS "Events (Count)",
        v.DATA_USAGE_EVENTS AS "Data Events",
        v.MAILBOX_EVENTS AS "Mailbox Events",
        v.REGISTRATION_EVENTS AS "Registration Events",
        -- Превышения
        v.INCLUDED_KB AS "Included (KB)",
        v.OVERAGE_KB AS "Overage (KB)",
        v.CALCULATED_OVERAGE AS "Calculated Overage ($)",
        v.SPNET_TOTAL_AMOUNT AS "SPNet Total Amount ($)",
        v.STECCOM_TOTAL_AMOUNT AS "STECCOM Total Amount ($)",
        -- Доп. поля из биллинга
        v.organization_name    AS "Organization/Person",
        v.code_1c              AS "Code 1C",
        v.service_id           AS "Service ID",
        v.agreement_number     AS "Agreement #"
    FROM v_consolidated_report_with_billing v
    WHERE 1=1
        {plan_condition}
        {period_condition}
    ORDER BY CAST(v.BILL_MONTH AS integer) DESC, "Calculated Overage ($)" DESC NULLS LAST
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            return df
        
        # Bill Month в базе уже в формате YYYYMM, для merge используем напрямую
        df['bill_month_num'] = df['Bill Month'].apply(lambda x: int(x) if pd.notna(x) else None)
        
        # Загружаем fees и делаем pivot по категориям
        # Группируем по IMEI и bill_month (периоду), чтобы не суммировать по всем периодам
        fees_query = f"""
        SELECT bill_month, contract_id, imei, category, SUM(amount) AS total_amount
        FROM v_steccom_access_fees_norm
        WHERE bill_month IS NOT NULL AND imei IS NOT NULL
        GROUP BY bill_month, contract_id, imei, category
        """
        
        try:
            fees_df = pd.read_sql_query(fees_query, conn)
            
            if not fees_df.empty:
                # Pivot: категории -> колонки (группируем по IMEI и bill_month, НЕ суммируем все периоды!)
                fees_pivot = fees_df.pivot_table(
                    index=['imei', 'bill_month', 'contract_id'],
                    columns='category',
                    values='total_amount',
                    aggfunc='sum',
                    fill_value=0
                ).reset_index()
                
                # Переименовываем колонки категорий
                fees_pivot.columns = [f"Fee: {col}" if col not in ['imei', 'bill_month', 'contract_id'] else col 
                                      for col in fees_pivot.columns]
                
                # Merge с основным отчетом по IMEI и bill_month (а не только по contract_id)
                df = df.merge(
                    fees_pivot,
                    left_on=['IMEI', 'bill_month_num'],
                    right_on=['imei', 'bill_month'],
                    how='left'
                )
                
                # Заполняем отсутствующие суммы нулями для колонок Fee:*
                fee_cols = [c for c in fees_pivot.columns if c.startswith('Fee: ')]
                for c in fee_cols:
                    if c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

                # Удаляем служебные колонки
                df = df.drop(columns=['contract_id', 'imei', 'bill_month', 'bill_month_num'], errors='ignore')
            
        except Exception as e:
            st.warning(f"Не удалось загрузить категории плат: {e}")
            df = df.drop(columns=['bill_month_num'], errors='ignore')
        
        # Итог по платам и дельта к STECCOM Total
        fee_cols_all = [c for c in df.columns if c.startswith('Fee: ')]
        if fee_cols_all:
            df['Fees Total ($)'] = df[fee_cols_all].sum(axis=1, numeric_only=True)
            if 'STECCOM Total Amount ($)' in df.columns:
                df['Δ vs STECCOM ($)'] = df['STECCOM Total Amount ($)'] - df['Fees Total ($)']

        # Форматируем Bill Month для отображения (YYYY-MM) из YYYYMM
        if 'Bill Month' in df.columns:
            df['Bill Month'] = df['Bill Month'].apply(lambda x: 
                f"{int(x) // 100:04d}-{int(x) % 100:02d}" if pd.notna(x) and pd.notnull(x) else ""
            )
        
        return df
    except Exception as e:
        st.error(f"Ошибка получения отчета: {e}")
        return None
    finally:
        conn.close()


def get_periods():
    """Получение списка периодов"""
    conn = get_connection()
    if not conn:
        return []
    
    query = """
    SELECT DISTINCT BILL_MONTH
    FROM V_CONSOLIDATED_OVERAGE_REPORT
    WHERE BILL_MONTH IS NOT NULL
    ORDER BY BILL_MONTH DESC
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        periods = []
        for row in cursor.fetchall():
            if row[0]:
                bill_month = int(row[0])  # YYYYMM
                year = bill_month // 100
                month = bill_month % 100
                periods.append(f"{year:04d}-{month:02d}")
        return periods
    except Exception as e:
        st.error(f"Ошибка получения периодов: {e}")
        return []
    finally:
        conn.close()


def get_plans():
    """Получение списка тарифных планов"""
    conn = get_connection()
    if not conn:
        return []
    
    query = """
    SELECT DISTINCT PLAN_NAME
    FROM V_CONSOLIDATED_OVERAGE_REPORT
    WHERE PLAN_NAME IS NOT NULL
    ORDER BY PLAN_NAME
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
    tab_report, tab_loader = st.tabs(["📊 Report", "📥 Data Loader"])
    
    # ========== REPORT TAB ==========
    with tab_report:
        # Фильтры в боковой панели
        with st.sidebar:
            st.header("⚙️ Filters")
        
        # Период
        periods = get_periods()
        period_options = ["All Periods"] + periods
        selected_period = st.selectbox("Period", period_options)
        
        # Тарифный план
        plans = get_plans()
        plan_options = ["All Plans"] + plans
        selected_plan = st.selectbox("Plan", plan_options)
        
        st.markdown("---")
        st.header("🔐 Database Connection")
        st.caption(f"📡 {DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
        
        # Кнопка тестирования подключения
        if st.button("🔌 Test Connection"):
            test_conn = get_connection()
            if test_conn:
                st.success("✅ Подключение успешно!")
                test_conn.close()
            else:
                st.error("❌ Ошибка подключения. Проверьте config.env")
        
        st.info("💡 Конфигурация загружается из config.env при запуске через run_streamlit.sh")
        
        period_filter = None if selected_period == "All Periods" else selected_period
        plan_filter = None if selected_plan == "All Plans" else selected_plan
        
        df = get_main_report(period_filter, plan_filter)
        
        if df is not None and not df.empty:
        # Информация о выборке
        st.info(f"📊 Records: **{len(df)}** | IMEI: **{df['IMEI'].nunique()}**")
        
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
        st.markdown("Загрузка и импорт данных SPNet и STECCOM в базу данных")
        st.markdown("---")
        
        # Директории для данных
        from pathlib import Path
        DATA_DIR = Path(__file__).parent / 'data'
        SPNET_DIR = DATA_DIR / 'SPNet reports'
        STECCOM_DIR = DATA_DIR / 'STECCOMLLCRussiaSBD.AccessFees_reports'
        
        # Выбор типа данных
        data_type = st.radio(
            "Select data type to upload",
            ["SPNet Traffic", "STECCOM Access Fees"],
            horizontal=True
        )
        
        st.markdown("---")
        
        if data_type == "SPNet Traffic":
            st.subheader("📊 SPNet Traffic Reports")
            st.markdown(f"**Directory:** `{SPNET_DIR}`")
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Список существующих файлов с статусом загрузки
                if SPNET_DIR.exists():
                    spnet_files = list(SPNET_DIR.glob("*.csv")) + list(SPNET_DIR.glob("*.xlsx"))
                    if spnet_files:
                        # Получаем список загруженных файлов из load_logs
                        conn_status = get_connection()
                        loaded_files = set()
                        if conn_status:
                            try:
                                cursor = conn_status.cursor()
                                cursor.execute("""
                                    SELECT LOWER(file_name) FROM load_logs 
                                    WHERE LOWER(table_name) = LOWER('spnet_traffic') 
                                    AND load_status = 'SUCCESS'
                                """)
                                loaded_files = {row[0] for row in cursor.fetchall()}
                                cursor.close()
                            except:
                                pass
                            finally:
                                conn_status.close()
                        
                        st.markdown(f"**Found files: {len(spnet_files)}**")
                        files_info = []
                        for f in sorted(spnet_files, key=lambda x: x.stat().st_mtime, reverse=True)[:20]:
                            is_loaded = f.name.lower() in loaded_files
                            files_info.append({
                                'File Name': f.name,
                                'Size (MB)': round(f.stat().st_size / (1024 * 1024), 2),
                                'Modified': datetime.fromtimestamp(f.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                                'Status': '✅ Loaded' if is_loaded else '⏳ Not loaded'
                            })
                        df_files = pd.DataFrame(files_info)
                        st.dataframe(df_files, use_container_width=True, hide_index=True, height=200)
                    else:
                        st.info("📁 Directory is empty")
                else:
                    st.info(f"📁 Directory does not exist: {SPNET_DIR}")
            
            with col2:
                st.markdown("### Actions")
                
                # Универсальный загрузчик файлов (drag & drop)
                uploaded_file = st.file_uploader(
                    "📤 Upload file (drag & drop)",
                    type=['csv', 'xlsx'],
                    key='spnet_upload',
                    help="Files will be automatically saved to SPNet reports directory"
                )
                
                if uploaded_file:
                    # Определяем тип файла автоматически
                    try:
                        from python.load_data_postgres import PostgresDataLoader
                        temp_loader = PostgresDataLoader(DB_CONFIG)
                        file_type = temp_loader.detect_file_type(uploaded_file)
                    except:
                        file_type = None
                    
                    # Определяем директорию назначения
                    if file_type == 'STECCOM':
                        target_dir = STECCOM_DIR
                        file_type_msg = "⚠️ **Detected as STECCOM file!** Will save to STECCOM directory"
                    else:
                        target_dir = SPNET_DIR
                        file_type_msg = "✅ Detected as SPNet file"
                    
                    save_path = target_dir / uploaded_file.name
                    
                    if save_path.exists():
                        st.warning(f"⚠️ File `{uploaded_file.name}` already exists")
                    else:
                        if file_type and file_type == 'STECCOM':
                            st.info(file_type_msg)
                        
                        if st.button("💾 Save File", key='save_spnet', use_container_width=True):
                            try:
                                target_dir.mkdir(parents=True, exist_ok=True)
                                with open(save_path, 'wb') as f:
                                    f.write(uploaded_file.getbuffer())
                                st.success(f"✅ File saved to {target_dir.name}/: {uploaded_file.name}")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error saving: {e}")
        
        else:  # STECCOM Access Fees
            st.subheader("💰 STECCOM Access Fees Reports")
            st.markdown(f"**Directory:** `{STECCOM_DIR}`")
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Список существующих файлов с статусом загрузки
                if STECCOM_DIR.exists():
                    steccom_files = list(STECCOM_DIR.glob("*.csv"))
                    if steccom_files:
                        # Получаем список загруженных файлов из load_logs
                        conn_status = get_connection()
                        loaded_files = set()
                        if conn_status:
                            try:
                                cursor = conn_status.cursor()
                                cursor.execute("""
                                    SELECT LOWER(file_name) FROM load_logs 
                                    WHERE LOWER(table_name) = LOWER('steccom_expenses') 
                                    AND load_status = 'SUCCESS'
                                """)
                                loaded_files = {row[0] for row in cursor.fetchall()}
                                cursor.close()
                            except:
                                pass
                            finally:
                                conn_status.close()
                        
                        st.markdown(f"**Found files: {len(steccom_files)}**")
                        files_info = []
                        for f in sorted(steccom_files, key=lambda x: x.stat().st_mtime, reverse=True)[:20]:
                            is_loaded = f.name.lower() in loaded_files
                            files_info.append({
                                'File Name': f.name,
                                'Size (MB)': round(f.stat().st_size / (1024 * 1024), 2),
                                'Modified': datetime.fromtimestamp(f.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                                'Status': '✅ Loaded' if is_loaded else '⏳ Not loaded'
                            })
                        df_files = pd.DataFrame(files_info)
                        st.dataframe(df_files, use_container_width=True, hide_index=True, height=200)
                    else:
                        st.info("📁 Directory is empty")
                else:
                    st.info(f"📁 Directory does not exist: {STECCOM_DIR}")
            
            with col2:
                st.markdown("### Actions")
                
                # Универсальный загрузчик файлов (drag & drop)
                uploaded_file = st.file_uploader(
                    "📤 Upload file (drag & drop)",
                    type=['csv'],
                    key='steccom_upload',
                    help="Files will be automatically saved to STECCOM directory"
                )
                
                if uploaded_file:
                    # Определяем тип файла автоматически
                    file_type = None
                    try:
                        import tempfile
                        import io
                        from python.load_data_postgres import PostgresDataLoader
                        
                        # Сохраняем во временный файл для определения типа
                        with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as tmp_file:
                            tmp_file.write(uploaded_file.getbuffer())
                            tmp_path = tmp_file.name
                        
                        temp_loader = PostgresDataLoader(DB_CONFIG)
                        file_type = temp_loader.detect_file_type(tmp_path)
                        
                        # Удаляем временный файл
                        import os
                        os.unlink(tmp_path)
                    except Exception as e:
                        # Если не удалось определить, пробуем по имени файла
                        file_name_lower = uploaded_file.name.lower()
                        if 'spnet' in file_name_lower or 'traffic' in file_name_lower:
                            file_type = 'SPNet'
                        elif 'steccom' in file_name_lower or 'access' in file_name_lower or 'fee' in file_name_lower:
                            file_type = 'STECCOM'
                    
                    # Определяем директорию назначения
                    if file_type == 'SPNet':
                        target_dir = SPNET_DIR
                        file_type_msg = "⚠️ **Detected as SPNet file!** Will save to SPNet directory"
                    else:
                        target_dir = STECCOM_DIR
                        file_type_msg = "✅ Detected as STECCOM file"
                    
                    save_path = target_dir / uploaded_file.name
                    
                    if save_path.exists():
                        st.warning(f"⚠️ File `{uploaded_file.name}` already exists")
                    else:
                        if file_type and file_type == 'SPNet':
                            st.info(file_type_msg)
                        
                        if st.button("💾 Save File", key='save_steccom', use_container_width=True):
                            try:
                                target_dir.mkdir(parents=True, exist_ok=True)
                                with open(save_path, 'wb') as f:
                                    f.write(uploaded_file.getbuffer())
                                st.success(f"✅ File saved to {target_dir.name}/: {uploaded_file.name}")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error saving: {e}")
        
        st.markdown("---")
        st.subheader("🔄 Import to Database")
        
        # Импорт данных в базу
        col_imp1, col_imp2 = st.columns(2)
        
        with col_imp1:
            if st.button("📥 Import SPNet Files", use_container_width=True, type="primary"):
                with st.spinner("Импорт данных SPNet в PostgreSQL..."):
                    try:
                        from python.load_data_postgres import PostgresDataLoader
                        
                        loader = PostgresDataLoader(DB_CONFIG)
                        if loader.connect():
                            import io
                            from contextlib import redirect_stdout, redirect_stderr
                            import sys
                            
                            # Обновляем путь к директории
                            loader.spnet_path = str(SPNET_DIR)
                            
                            # Перехватываем вывод
                            log_capture = io.StringIO()
                            old_stdout = sys.stdout
                            old_stderr = sys.stderr
                            
                            try:
                                sys.stdout = log_capture
                                sys.stderr = log_capture
                                
                                result = loader.load_spnet_files()
                                
                                log_output = log_capture.getvalue()
                                
                                if result:
                                    st.success("✅ Импорт SPNet завершен успешно!")
                                    st.text_area("Log output", log_output, height=200, key='spnet_log')
                                else:
                                    st.error(f"❌ Ошибка импорта SPNet")
                                    st.text_area("Log output", log_output, height=200, key='spnet_log_err')
                            finally:
                                sys.stdout = old_stdout
                                sys.stderr = old_stderr
                                if loader.connection:
                                    loader.close()
                        else:
                            st.error("❌ Не удалось подключиться к базе данных")
                    except Exception as e:
                        import traceback
                        st.error(f"❌ Ошибка: {e}")
                        st.text_area("Error details", traceback.format_exc(), height=200)
        
        with col_imp2:
            if st.button("📥 Import STECCOM Files", use_container_width=True, type="primary"):
                with st.spinner("Импорт данных STECCOM в PostgreSQL..."):
                    try:
                        from python.load_data_postgres import PostgresDataLoader
                        
                        loader = PostgresDataLoader(DB_CONFIG)
                        if loader.connect():
                            import io
                            from contextlib import redirect_stdout, redirect_stderr
                            import sys
                            
                            # Обновляем путь к директории
                            loader.steccom_path = str(STECCOM_DIR)
                            
                            # Перехватываем вывод
                            log_capture = io.StringIO()
                            old_stdout = sys.stdout
                            old_stderr = sys.stderr
                            
                            try:
                                sys.stdout = log_capture
                                sys.stderr = log_capture
                                
                                result = loader.load_steccom_files()
                                
                                log_output = log_capture.getvalue()
                                
                                if result:
                                    st.success("✅ Импорт STECCOM завершен успешно!")
                                    st.text_area("Log output", log_output, height=200, key='steccom_log')
                                else:
                                    st.error(f"❌ Ошибка импорта STECCOM")
                                    st.text_area("Log output", log_output, height=200, key='steccom_log_err')
                            finally:
                                sys.stdout = old_stdout
                                sys.stderr = old_stderr
                                if loader.connection:
                                    loader.close()
                        else:
                            st.error("❌ Не удалось подключиться к базе данных")
                    except Exception as e:
                        import traceback
                        st.error(f"❌ Ошибка: {e}")
                        st.text_area("Error details", traceback.format_exc(), height=200)
        
        st.markdown("---")
        st.caption("💡 **Tip:** After importing, refresh the Report tab to see updated data")


if __name__ == "__main__":
    main()
