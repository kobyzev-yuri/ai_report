"""
Закладка: Отчет по расходам Иридиум
"""
import streamlit as st
import pandas as pd
from datetime import datetime
from tabs.common import export_to_csv, export_to_excel
from tabs.report_filters import render_report_filters


def show_tab(get_connection, get_main_report, get_periods, get_plans):
    """
    Отображение закладки отчета по расходам.
    Фильтры — в теле вкладки (render_report_filters).
    """
    selected_period, selected_plan, contract_id_filter, imei_filter, customer_name_filter, code_1c_filter = (
        render_report_filters(get_connection, get_periods, get_plans, include_plan=True)
    )
    period_filter = selected_period
    plan_filter = None if selected_plan == "All Plans" else selected_plan
    contract_id_filter = contract_id_filter if contract_id_filter else None
    imei_filter = imei_filter if imei_filter else None
    customer_name_filter = customer_name_filter if customer_name_filter else None
    code_1c_filter = code_1c_filter if code_1c_filter else None
    
    # Отчет загружается только по нажатию кнопки, не автоматически
    filter_key = f"{period_filter}_{plan_filter}_{contract_id_filter}_{imei_filter}_{customer_name_filter}_{code_1c_filter}"
    
    # Кнопка загрузки отчета
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("**Настройте фильтры и нажмите кнопку для загрузки отчета:**")
    with col2:
        load_report = st.button("📊 Загрузить отчет", type="primary", use_container_width=True)
    
    # Загружаем отчет только при нажатии кнопки
    if load_report:
        if period_filter is not None:
            with st.spinner("Загрузка данных отчета..."):
                df = get_main_report(
                    get_connection,
                    period_filter, 
                    plan_filter,
                    contract_id_filter,
                    imei_filter,
                    customer_name_filter,
                    code_1c_filter
                )
                st.session_state.last_report_key = filter_key
                st.session_state.last_report_df = df
                st.session_state.report_loaded = True
        else:
            st.warning("⚠️ Выберите период для загрузки отчета")
            df = None
            st.session_state.report_loaded = False
    else:
        # Показываем сохраненный отчет ТОЛЬКО если фильтры точно совпадают и отчет был загружен
        saved_key = st.session_state.get('last_report_key')
        if (st.session_state.get('report_loaded', False) and 
            saved_key is not None and 
            saved_key == filter_key):
            df = st.session_state.get('last_report_df', None)
        else:
            df = None
            # Сбрасываем флаг загрузки, если фильтры изменились
            if saved_key is not None and saved_key != filter_key:
                st.session_state.report_loaded = False
                st.session_state.last_report_df = None
            if not st.session_state.get('report_loaded', False):
                st.info("ℹ️ Настройте фильтры и нажмите кнопку 'Загрузить отчет' для просмотра данных")
    
    # Показываем отчет ТОЛЬКО если он был загружен по кнопке
    if df is not None and not df.empty and st.session_state.get('report_loaded', False):
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
        
        # Заполняем NULL пустыми строками для строковых колонок
        for col in display_df.columns:
            if display_df[col].dtype == 'object':
                display_df[col] = display_df[col].fillna('')
        
        # Убеждаемся, что Activation Date всегда присутствует
        if 'Activation Date' in display_df.columns:
            display_df['Activation Date'] = display_df['Activation Date'].fillna('')
        else:
            display_df['Activation Date'] = ''
        
        # Упорядочиваем колонки
        expected_order = [
            "Отчетный Период", "Bill Month", "IMEI", "Contract ID",
            "Organization/Person", "Code 1C", "Service ID", "Agreement #",
            "Activation Date",
            "Plan Name", "Plan Monthly", "Plan Suspended",
            "Traffic Usage (KB)", 
            "Mailbox Events", "Registration Events",
            "Overage (KB)", "Calculated Overage ($)", "Total Amount ($)",
            "Activation Fee", "Advance Charge", "Advance Charge Previous Month",
            "Credit", "Credited", "Prorated"
        ]
        
        ordered_columns = [col for col in expected_order if col in display_df.columns]
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
        
        contract_ids = df['Contract ID'].dropna().unique().tolist() if not df.empty else []
        
        if contract_filter.strip():
            contract_ids.append(contract_filter.strip())
        
        conn2 = get_connection()
        if conn2:
            contract_condition = ""
            
            if contract_ids:
                limited_contract_ids = contract_ids[:200]
                contract_list = "', '".join([str(c).replace("'", "''") for c in limited_contract_ids[:100]])
                contract_condition = f"AND f.CONTRACT_ID IN ('{contract_list}')"
            
            period_condition = ""
            if period_filter and period_filter != "All Periods":
                year_month = period_filter.replace('-', '')
                period_condition = f"AND f.SOURCE_FILE LIKE '%.{year_month}%.csv'"
            
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
























