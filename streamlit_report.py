#!/usr/bin/env python3
"""
Streamlit отчет по превышению трафика Iridium M2M
Поддержка PostgreSQL и Oracle
"""

import streamlit as st
import pandas as pd
from datetime import datetime
import io
import os
from pathlib import Path

# Импортируем абстракцию подключения к БД
from db_connection import get_db_connection, get_postgres_config, get_oracle_config, get_db_type

# Получаем тип БД из config.env
DB_TYPE = get_db_type()

def get_connection():
    """Создание подключения к базе данных (PostgreSQL или Oracle)"""
    try:
        return get_db_connection(DB_TYPE)
    except Exception as e:
        st.error(f"Ошибка подключения к базе данных: {e}")
        config = get_postgres_config() if DB_TYPE == 'postgresql' else get_oracle_config()
        if DB_TYPE == 'postgresql':
            st.info(f"Проверьте конфигурацию: {config['user']}@{config['host']}:{config['port']}/{config['dbname']}")
        else:
            st.info(f"Проверьте конфигурацию: {config['user']}@{config['host']}:{config['port']}/{config['service_name']}")
        return None


def get_main_report(period_filter=None, plan_filter=None):
    """Получение основного отчета (работает для PostgreSQL и Oracle)"""
    conn = get_connection()
    if not conn:
        return None
    
    # Определяем имена полей в зависимости от БД
    # PostgreSQL автоматически приводит имена без кавычек к нижнему регистру
    # Oracle сохраняет регистр как в VIEW
    if DB_TYPE == 'oracle':
        # Oracle использует заглавные имена
        imei_col = "v.IMEI"
        contract_col = "v.CONTRACT_ID"
        plan_monthly_col = "v.STECCOM_PLAN_NAME_MONTHLY"
        plan_suspended_col = "v.STECCOM_PLAN_NAME_SUSPENDED"
        bill_month_col = "v.BILL_MONTH"
        bill_month_yyyymm_col = "v.BILL_MONTH_YYYMM"
        display_name_col = "COALESCE(v.ORGANIZATION_NAME, v.CUSTOMER_NAME, '')"
        code_1c_col = "v.CODE_1C"
        service_id_col = "v.SERVICE_ID"
        agreement_col = "v.AGREEMENT_NUMBER"
        fee_prefix = "v.FEE_"
        fees_total_col = "v.FEES_TOTAL"
        delta_col = "v.DELTA_VS_STECCOM"
    else:
        # PostgreSQL - используем нижний регистр (PostgreSQL автоматически приведет)
        imei_col = "v.imei"
        contract_col = "v.contract_id"
        plan_monthly_col = "v.steccom_plan_name_monthly"
        plan_suspended_col = "v.steccom_plan_name_suspended"
        bill_month_col = "v.bill_month"
        bill_month_yyyymm_col = "v.bill_month_yyyymm"
        display_name_col = "v.display_name"
        code_1c_col = "v.code_1c"
        service_id_col = "v.service_id"
        agreement_col = "v.agreement_number"
        fee_prefix = "v.fee_"
        fees_total_col = "v.fees_total"
        delta_col = "v.delta_vs_steccom"
    
    # Фильтр по периодам
    period_condition = ""
    if period_filter and period_filter != "All Periods":
        # Конвертируем YYYY-MM в формат базы YYYYMM (например: 2025-09 -> 202509)
        year, month = period_filter.split('-')
        bill_month = int(year) * 100 + int(month)
        period_condition = f"AND {bill_month_yyyymm_col} = '{bill_month}'"
    
    # Фильтр по тарифам
    plan_condition = ""
    if plan_filter and plan_filter != "All Plans":
        plan_name_col = "v.PLAN_NAME" if DB_TYPE == 'oracle' else "v.plan_name"
        plan_condition = f"AND {plan_name_col} = '{plan_filter}'"
    
    # Общий запрос (работает для обеих БД, так как VIEW одинаковые)
    query = f"""
    SELECT 
        {imei_col} AS "IMEI",
        {contract_col} AS "Contract ID",
        COALESCE({plan_monthly_col}, '') AS "Plan Monthly",
        COALESCE({plan_suspended_col}, '') AS "Plan Suspended",
        {bill_month_col} AS "Bill Month",
        -- Разделение трафика и событий (PostgreSQL приводит к нижнему регистру автоматически)
        ROUND(CAST(v.traffic_usage_bytes AS NUMERIC) / 1000, 2) AS "Traffic Usage (KB)",
        v.events_count AS "Events (Count)",
        v.data_usage_events AS "Data Events",
        v.mailbox_events AS "Mailbox Events",
        v.registration_events AS "Registration Events",
        -- Превышения
        v.included_kb AS "Included (KB)",
        v.overage_kb AS "Overage (KB)",
        v.calculated_overage AS "Calculated Overage ($)",
        v.spnet_total_amount AS "SPNet Total Amount ($)",
        v.steccom_monthly_amount AS "STECCOM Monthly ($)",
        v.steccom_suspended_amount AS "STECCOM Suspended ($)",
        v.steccom_total_amount AS "STECCOM Total Amount ($)",
        -- Доп. поля из биллинга
        {display_name_col} AS "Organization/Person",
        {code_1c_col} AS "Code 1C",
        {service_id_col} AS "Service ID",
        {agreement_col} AS "Agreement #",
        -- Fees из STECCOM_EXPENSES
        {fee_prefix}ACTIVATION_FEE AS "Fee: Activation Fee",
        {fee_prefix}ADVANCE_CHARGE AS "Fee: Advance Charge",
        {fee_prefix}CREDIT AS "Fee: Credit",
        {fee_prefix}CREDITED AS "Fee: Credited",
        {fee_prefix}PRORATED AS "Fee: Prorated",
        {fees_total_col} AS "Fees Total ($)",
        {delta_col} AS "Δ vs STECCOM ($)"
    FROM V_CONSOLIDATED_REPORT_WITH_BILLING v
    WHERE 1=1
        {plan_condition}
        {period_condition}
    ORDER BY {bill_month_yyyymm_col} DESC, "Calculated Overage ($)" DESC NULLS LAST
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            return df
        
        return df
    except Exception as e:
        st.error(f"Ошибка получения отчета: {e}")
        import traceback
        st.error(traceback.format_exc())
        return None
    finally:
        if conn:
            conn.close()


def get_periods():
    """Получение списка периодов (работает для PostgreSQL и Oracle)"""
    conn = get_connection()
    if not conn:
        return []
    
    # PostgreSQL использует нижний регистр, Oracle - верхний
    if DB_TYPE == 'oracle':
        col_name = "BILL_MONTH_YYYMM"
    else:
        col_name = "bill_month_yyyymm"
    
    query = f"""
    SELECT DISTINCT {col_name}
    FROM V_CONSOLIDATED_REPORT_WITH_BILLING
    WHERE {col_name} IS NOT NULL
    ORDER BY {col_name} DESC
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        periods = []
        # pandas может привести имена столбцов к нижнему регистру для PostgreSQL
        # Пробуем найти столбец в любом регистре
        col_key = None
        for col in df.columns:
            if col.lower() == col_name.lower():
                col_key = col
                break
        if col_key is None:
            col_key = col_name.lower()  # Fallback
        
        for bill_month in df[col_key].dropna():
            if isinstance(bill_month, str):
                bill_month = int(bill_month)
            elif isinstance(bill_month, (int, float)):
                bill_month = int(bill_month)
            else:
                continue
            year = bill_month // 100
            month = bill_month % 100
            periods.append(f"{year:04d}-{month:02d}")
        return periods
    except Exception as e:
        st.error(f"Ошибка получения периодов: {e}")
        return []
    finally:
        if conn:
            conn.close()


def get_plans():
    """Получение списка тарифных планов (работает для PostgreSQL и Oracle)"""
    conn = get_connection()
    if not conn:
        return []
    
    plan_name_col = "PLAN_NAME" if DB_TYPE == 'oracle' else "plan_name"
    query = f"""
    SELECT DISTINCT {plan_name_col}
    FROM V_CONSOLIDATED_REPORT_WITH_BILLING
    WHERE {plan_name_col} IS NOT NULL
    ORDER BY {plan_name_col}
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        # pandas может привести имена столбцов к нижнему регистру для PostgreSQL
        # Пробуем найти столбец в любом регистре
        col_key = None
        for col in df.columns:
            if col.lower() == plan_name_col.lower():
                col_key = col
                break
        if col_key is None:
            col_key = plan_name_col.lower()  # Fallback
        
        plans = [row for row in df[col_key].dropna().unique() if row]
        return sorted(plans)
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
        page_title="Iridium M2M Overage Report",
        page_icon="📊",
        layout="wide"
    )
    
    # Проверка конфигурации
    if DB_TYPE == 'postgresql':
        config = get_postgres_config()
        if not config.get('password'):
            st.error("⚠️ **PostgreSQL конфигурация не загружена!**")
            st.warning("""
            Запустите приложение через скрипт:
            ```bash
            ./run_streamlit.sh
            ```
            
            Или установите переменные окружения POSTGRES_* в config.env
            """)
            st.stop()
    else:
        config = get_oracle_config()
        if not config.get('password'):
            st.error("⚠️ **Oracle конфигурация не загружена!**")
            st.warning("Установите переменные окружения ORACLE_* в config.env")
            st.stop()
    
    # Заголовок
    st.title("📊 Iridium M2M Overage Report")
    db_badge = "🟢 PostgreSQL" if DB_TYPE == 'postgresql' else "🔵 Oracle"
    st.markdown(f"**{db_badge}** | All Plans (Calculated Overage for SBD-1 and SBD-10 only)")
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
        config = get_postgres_config() if DB_TYPE == 'postgresql' else get_oracle_config()
        if DB_TYPE == 'postgresql':
            st.caption(f"📡 {config['user']}@{config['host']}:{config['port']}/{config['dbname']}")
        else:
            st.caption(f"📡 {config['user']}@{config['host']}:{config['port']}/{config['service_name']}")
        
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
                                if DB_TYPE == 'oracle':
                                    query = """
                                    SELECT LOWER(FILE_NAME) FROM LOAD_LOGS 
                                    WHERE LOWER(TABLE_NAME) = LOWER('SPNET_TRAFFIC') 
                                    AND LOAD_STATUS = 'SUCCESS'
                                    """
                                else:
                                    query = """
                                    SELECT LOWER(file_name) FROM load_logs 
                                    WHERE LOWER(table_name) = LOWER('spnet_traffic') 
                                    AND load_status = 'SUCCESS'
                                    """
                                cursor = conn_status.cursor()
                                cursor.execute(query)
                                loaded_files = {row[0] for row in cursor.fetchall()}
                                cursor.close()
                            except:
                                pass
                            finally:
                                if conn_status:
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
                        if DB_TYPE == 'postgresql':
                            from python.load_data_postgres import PostgresDataLoader
                            temp_loader = PostgresDataLoader(get_postgres_config())
                            file_type = temp_loader.detect_file_type(uploaded_file)
                        else:
                            # Для Oracle используем простую проверку по имени файла
                            file_name_lower = uploaded_file.name.lower()
                            if 'spnet' in file_name_lower or 'traffic' in file_name_lower:
                                file_type = 'SPNet'
                            elif 'steccom' in file_name_lower or 'access' in file_name_lower or 'fee' in file_name_lower:
                                file_type = 'STECCOM'
                            else:
                                file_type = None
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
                                if DB_TYPE == 'oracle':
                                    query = """
                                    SELECT LOWER(FILE_NAME) FROM LOAD_LOGS 
                                    WHERE LOWER(TABLE_NAME) = LOWER('STECCOM_EXPENSES') 
                                    AND LOAD_STATUS = 'SUCCESS'
                                    """
                                else:
                                    query = """
                                    SELECT LOWER(file_name) FROM load_logs 
                                    WHERE LOWER(table_name) = LOWER('steccom_expenses') 
                                    AND load_status = 'SUCCESS'
                                    """
                                cursor = conn_status.cursor()
                                cursor.execute(query)
                                loaded_files = {row[0] for row in cursor.fetchall()}
                                cursor.close()
                            except:
                                pass
                            finally:
                                if conn_status:
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
                        if DB_TYPE == 'postgresql':
                            import tempfile
                            import io
                            from python.load_data_postgres import PostgresDataLoader
                            
                            # Сохраняем во временный файл для определения типа
                            with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as tmp_file:
                                tmp_file.write(uploaded_file.getbuffer())
                                tmp_path = tmp_file.name
                            
                            temp_loader = PostgresDataLoader(get_postgres_config())
                            file_type = temp_loader.detect_file_type(tmp_path)
                            
                            # Удаляем временный файл
                            import os
                            os.unlink(tmp_path)
                        else:
                            # Для Oracle используем простую проверку по имени файла
                            file_name_lower = uploaded_file.name.lower()
                            if 'spnet' in file_name_lower or 'traffic' in file_name_lower:
                                file_type = 'SPNet'
                            elif 'steccom' in file_name_lower or 'access' in file_name_lower or 'fee' in file_name_lower:
                                file_type = 'STECCOM'
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
            db_name = "PostgreSQL" if DB_TYPE == 'postgresql' else "Oracle"
            if st.button("📥 Import SPNet Files", use_container_width=True, type="primary"):
                with st.spinner(f"Импорт данных SPNet в {db_name}..."):
                    try:
                        if DB_TYPE == 'postgresql':
                            from python.load_data_postgres import PostgresDataLoader
                            loader = PostgresDataLoader(get_postgres_config())
                            connect_method = loader.connect
                        else:
                            from python.load_spnet_traffic import SPNetDataLoader
                            loader = SPNetDataLoader(get_oracle_config())
                            connect_method = loader.connect_to_oracle
                            loader.gdrive_path = str(SPNET_DIR)
                        
                        if connect_method():
                            import io
                            from contextlib import redirect_stdout, redirect_stderr
                            import sys
                            
                            # Обновляем путь к директории
                            if DB_TYPE == 'postgresql':
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
                                    st.success(f"✅ Импорт SPNet в {db_name} завершен успешно!")
                                    st.text_area("Log output", log_output, height=200, key='spnet_log')
                                else:
                                    st.error(f"❌ Ошибка импорта SPNet")
                                    st.text_area("Log output", log_output, height=200, key='spnet_log_err')
                            finally:
                                sys.stdout = old_stdout
                                sys.stderr = old_stderr
                                if hasattr(loader, 'connection') and loader.connection:
                                    if hasattr(loader, 'close'):
                                        loader.close()
                                    else:
                                        loader.connection.close()
                        else:
                            st.error("❌ Не удалось подключиться к базе данных")
                    except Exception as e:
                        import traceback
                        st.error(f"❌ Ошибка: {e}")
                        st.text_area("Error details", traceback.format_exc(), height=200)
        
        with col_imp2:
            db_name = "PostgreSQL" if DB_TYPE == 'postgresql' else "Oracle"
            if st.button("📥 Import STECCOM Files", use_container_width=True, type="primary"):
                with st.spinner(f"Импорт данных STECCOM в {db_name}..."):
                    try:
                        if DB_TYPE == 'postgresql':
                            from python.load_data_postgres import PostgresDataLoader
                            loader = PostgresDataLoader(get_postgres_config())
                            connect_method = loader.connect
                            loader.steccom_path = str(STECCOM_DIR)
                        else:
                            from python.load_steccom_expenses import STECCOMDataLoader
                            loader = STECCOMDataLoader(get_oracle_config())
                            connect_method = loader.connect_to_oracle
                            loader.gdrive_path = str(STECCOM_DIR)
                        
                        if connect_method():
                            import io
                            from contextlib import redirect_stdout, redirect_stderr
                            import sys
                            
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
                                    st.success(f"✅ Импорт STECCOM в {db_name} завершен успешно!")
                                    st.text_area("Log output", log_output, height=200, key='steccom_log')
                                else:
                                    st.error(f"❌ Ошибка импорта STECCOM")
                                    st.text_area("Log output", log_output, height=200, key='steccom_log_err')
                            finally:
                                sys.stdout = old_stdout
                                sys.stderr = old_stderr
                                if hasattr(loader, 'connection') and loader.connection:
                                    if hasattr(loader, 'close'):
                                        loader.close()
                                    else:
                                        loader.connection.close()
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
