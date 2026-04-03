"""
Закладка: Отчет по доходам
"""
import streamlit as st
import pandas as pd
from datetime import datetime
from tabs.common import export_to_csv, export_to_excel
from tabs.report_filters import render_report_filters


def show_tab(get_connection, get_revenue_report, get_periods, get_plans):
    """
    Отображение закладки отчета по доходам.
    Фильтры — в теле вкладки.
    """
    st.header("💰 Доходы из счетов-фактур")
    st.markdown("Отчет по доходам из счетов-фактур (BM_INVOICE_ITEM). Все суммы в рублях.")
    st.caption("В отчёте: SBD = REVENUE_SBD_*; Stectrace = REVENUE_MSG_ABON; мониторинг (9004/9005/9010) = REVENUE_MONITORING_ABON. Одна строка на (SUB-/контракт, IMEI, период); при смене SUB на том же IMEI в периоде возможны две строки. Фильтр по активной главной услуге в периоде.")

    selected_period, _selected_plan, contract_id_filter, imei_filter, customer_name_filter, code_1c_filter = (
        render_report_filters(get_connection, get_periods, get_plans, include_plan=False)
    )
    period_filter = selected_period
    contract_id_filter = contract_id_filter if contract_id_filter else None
    imei_filter = imei_filter if imei_filter else None
    customer_name_filter = customer_name_filter if customer_name_filter else None
    code_1c_filter = code_1c_filter if code_1c_filter else None
    
    filter_key = f"revenue_{period_filter}_{contract_id_filter}_{imei_filter}_{customer_name_filter}_{code_1c_filter}"
    
    # Кнопка загрузки отчета
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("**Настройте фильтры и нажмите кнопку для загрузки отчета:**")
    with col2:
        load_revenue = st.button("📊 Загрузить отчет", type="primary", use_container_width=True, key="load_revenue_btn")
    
    if load_revenue:
        if period_filter is not None:
            with st.spinner("Загрузка данных по доходам..."):
                df_revenue = get_revenue_report(
                    get_connection,
                    period_filter,
                    contract_id_filter,
                    imei_filter,
                    customer_name_filter,
                    code_1c_filter
                )
                st.session_state.last_revenue_key = filter_key
                st.session_state.last_revenue_df = df_revenue
                st.session_state.revenue_loaded = True
        else:
            st.warning("⚠️ Выберите период для загрузки отчета")
            df_revenue = None
            st.session_state.revenue_loaded = False
    else:
        saved_key = st.session_state.get('last_revenue_key')
        if (st.session_state.get('revenue_loaded', False) and 
            saved_key is not None and 
            saved_key == filter_key):
            df_revenue = st.session_state.get('last_revenue_df', None)
        else:
            df_revenue = None
            if saved_key is not None and saved_key != filter_key:
                st.session_state.revenue_loaded = False
            if not st.session_state.get('revenue_loaded', False):
                st.info("ℹ️ Настройте фильтры и нажмите кнопку 'Загрузить отчет' для просмотра данных")
    
    if df_revenue is not None and not df_revenue.empty and st.session_state.get('revenue_loaded', False):
        st.success(f"✅ Загружено записей: {len(df_revenue):,}")
        
        # Метрики - используем правильные названия колонок из VIEW
        # VIEW возвращает колонки с префиксом REVENUE_ (английские названия)
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            # Итого доходов
            if 'REVENUE_TOTAL' in df_revenue.columns:
                st.metric("Итого доходов (руб)", f"{df_revenue['REVENUE_TOTAL'].sum():,.2f}")
            else:
                st.metric("Всего записей", f"{len(df_revenue):,}")
            
            # SBD Всего (трафик + абонплата)
            sbd_total = 0
            if 'REVENUE_SBD_TRAFFIC' in df_revenue.columns:
                sbd_total += df_revenue['REVENUE_SBD_TRAFFIC'].sum()
            if 'REVENUE_SBD_ABON' in df_revenue.columns:
                sbd_total += df_revenue['REVENUE_SBD_ABON'].sum()
            if sbd_total > 0:
                st.metric("SBD Всего", f"{sbd_total:,.2f}")
        with col2:
            # SBD Трафик превышения
            if 'REVENUE_SBD_TRAFFIC' in df_revenue.columns:
                st.metric("SBD Трафик превышения", f"{df_revenue['REVENUE_SBD_TRAFFIC'].sum():,.2f}")
            # SBD Трафик SBD-1
            if 'REVENUE_SBD_TRAFFIC_SBD1' in df_revenue.columns:
                st.metric("SBD Трафик SBD-1", f"{df_revenue['REVENUE_SBD_TRAFFIC_SBD1'].sum():,.2f}")
        with col3:
            # SBD Трафик SBD-10
            if 'REVENUE_SBD_TRAFFIC_SBD10' in df_revenue.columns:
                st.metric("SBD Трафик SBD-10", f"{df_revenue['REVENUE_SBD_TRAFFIC_SBD10'].sum():,.2f}")
            # SBD Абонплата
            if 'REVENUE_SBD_ABON' in df_revenue.columns:
                st.metric("SBD Абонплата", f"{df_revenue['REVENUE_SBD_ABON'].sum():,.2f}")
        with col4:
            # SUSPEND Абонплата
            if 'REVENUE_SUSPEND_ABON' in df_revenue.columns:
                st.metric("SUSPEND Абонплата", f"{df_revenue['REVENUE_SUSPEND_ABON'].sum():,.2f}")
            # Мониторинг Абонплата
            if 'REVENUE_MONITORING_ABON' in df_revenue.columns:
                st.metric("Мониторинг Абонплата (9004/9005/9010)", f"{df_revenue['REVENUE_MONITORING_ABON'].sum():,.2f}")
            # Блокировка мониторинга
            if 'REVENUE_MONITORING_BLOCK_ABON' in df_revenue.columns:
                st.metric("Блокировка мониторинга", f"{df_revenue['REVENUE_MONITORING_BLOCK_ABON'].sum():,.2f}")
            # Сообщения Абонплата
            if 'REVENUE_MSG_ABON' in df_revenue.columns:
                st.metric("Сообщения Абонплата", f"{df_revenue['REVENUE_MSG_ABON'].sum():,.2f}")
            st.metric("Записей", f"{len(df_revenue):,}")
        
        st.markdown("---")
        
        display_df_revenue = df_revenue.copy()
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





