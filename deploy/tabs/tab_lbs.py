"""
Закладка: Услуги LBS
Сервисный отчет по LBS услугам (TYPE_ID=9002, тарифы like %LBS%)
"""
import streamlit as st
import pandas as pd
import re
from datetime import datetime
from tabs.common import export_to_csv, export_to_excel

def show_tab(get_connection, get_lbs_services_report, get_periods=None):
    """
    Отображение закладки отчета по услугам LBS
    
    Args:
        get_connection: Функция получения подключения к БД
        get_lbs_services_report: Функция получения отчета по LBS услугам
        get_periods: Список периодов BM_PERIOD (YYYY-MM) (опционально; если None — период вводится вручную)
    """
    st.header("📍 Услуги LBS")
    st.markdown("Сервисный отчет по LBS услугам (в биллинге TYPE_ID=9002, тарифы BM_TARIFF.NAME like '%LBS%'). Без денежных колонок.")
    
    # Информация о отчете
    with st.expander("ℹ️ О отчете", expanded=False):
        st.markdown("""
        **Назначение отчета:**
        
        Отчет показывает услуги LBS (TYPE_ID=9002), определяемые по названию тарифного плана (`BM_TARIFF.NAME like '%LBS%'`).
        Цель — получить перечень сервисов с атрибутами клиента и флагом попадания в счет-фактуру.
        
        **Столбцы отчета:**
        - **IMEI** - номер устройства Iridium
        - **SERVICE_ID** - идентификатор услуги в биллинге
        - **CONTRACT_ID** - договор в Iridium (SUB-XXXXX)
        - **CUSTOMER_NAME** - название клиента (организация/ФИО)
        - **CODE_1C** - код клиента из системы 1С
        - **AGREEMENT_NUMBER** - договор в СТЭККОМ (ACCOUNT.DESCRIPTION)
        - **ORDER_NUMBER** - номер заказа/приложения
        - **OPEN_DATE / CLOSE_DATE** - даты открытия/закрытия услуги
        - **IN_INVOICE** - Y/N, попадала ли услуга (SERVICE_ID) в BM_INVOICE_ITEM
        - **FIRST_INVOICE_PERIOD_YYYYMM** - первый период (минимальный PERIOD_ID), когда услуга попала в СФ
        
        **Примечание:** 
        Отчет не содержит денежных сумм и трафика — это “справочник сервисов” с признаком выставления в СФ.

        **Фильтр по периоду (YYYY-MM):**  
        В выборку попадают только услуги, у которых интервал `[OPEN_DATE, CLOSE_DATE]` **пересекается** с выбранным календарным месяцем:  
        `OPEN_DATE` не позже конца месяца и (`CLOSE_DATE` пустая **или** `CLOSE_DATE` не раньше начала месяца).  
        Услуги, закрытые до начала месяца, и услуги, открытые после конца месяца, **не попадают**.
        """)
    
    st.markdown("---")

    period_filter = None
    if get_periods:
        periods = get_periods(get_connection) or []
        period_options = [p[0] for p in periods] if periods else []
        if period_options:
            period_filter = st.selectbox(
                "Период (YYYY-MM)",
                options=period_options,
                index=0,
                key="lbs_period",
                help="Услуга попадает в отчёт, если её OPEN_DATE/CLOSE_DATE пересекаются с этим календарным месяцем",
            )
        else:
            st.warning("Не удалось загрузить список периодов из BM_PERIOD. Введите период вручную ниже.")
            period_filter = st.text_input(
                "Период вручную (YYYY-MM)",
                key="lbs_period_manual",
                placeholder="2026-01",
                help="Формат строго YYYY-MM (календарный месяц)",
            ).strip() or None
    else:
        st.info("Список периодов недоступен в этой версии UI — введите период вручную.")
        period_filter = st.text_input(
            "Период вручную (YYYY-MM)",
            key="lbs_period_manual_fallback",
            placeholder="2026-01",
            help="Формат строго YYYY-MM (календарный месяц)",
        ).strip() or None
    
    # Фильтры
    col1, col2 = st.columns(2)
    
    with col1:
        contract_id_filter = st.text_input(
            "Contract ID (SUB-XXXXX)",
            key='lbs_contract_id',
            help="Фильтр по логину сервиса (SUB-XXXXX)"
        )
        imei_filter = st.text_input(
            "IMEI",
            key='lbs_imei',
            help="Фильтр по IMEI устройства"
        )
    
    with col2:
        customer_name_filter = st.text_input(
            "Название клиента",
            key='lbs_customer_name',
            help="Фильтр по названию организации или ФИО"
        )
        code_1c_filter = st.text_input(
            "Код 1С",
            key='lbs_code_1c',
            help="Фильтр по коду клиента из системы 1С"
        )
    
    st.markdown("---")
    
    # Дополнительные опции
    col_opts1, col_opts2 = st.columns([3, 1])
    with col_opts1:
        exclude_steccom = st.checkbox(
            "Без СТЭККОМ (customer_id=521)",
            value=True,
            help="Если включено, исключаются тестовые/внутренние услуги клиента СТЭККОМ (customer_id=521)"
        )
        only_in_invoice = st.checkbox(
            "Только услуги, попавшие в СФ (IN_INVOICE=Y)",
            value=False,
            help="Если включено, показываются только услуги, которые встречаются в BM_INVOICE_ITEM"
        )
        only_active = st.checkbox(
            "Только активные (CLOSE_DATE пустая)",
            value=False,
            help="Если включено, показываются только услуги без даты закрытия"
        )
        st.markdown("**Настройте фильтры и нажмите кнопку для загрузки отчета:**")
    with col_opts2:
        load_report = st.button("📊 Загрузить отчет", type="primary", use_container_width=True, key='lbs_load_report')
    
    # Загружаем отчет только при нажатии кнопки
    if load_report:
        if not period_filter:
            st.warning("Выберите период для загрузки отчёта")
        elif not re.fullmatch(r"\d{4}-\d{2}", str(period_filter).strip()):
            st.warning("Период должен быть в формате YYYY-MM (например, 2026-03)")
        else:
            with st.spinner("Загрузка данных отчета..."):
                df = get_lbs_services_report(
                    get_connection,
                    period_filter,
                    contract_id_filter if contract_id_filter else None,
                    imei_filter if imei_filter else None,
                    customer_name_filter if customer_name_filter else None,
                    code_1c_filter if code_1c_filter else None,
                    exclude_steccom,
                    only_in_invoice,
                    only_active,
                )

                if df is not None:
                    if not df.empty:
                        st.success(f"✅ Загружено записей: {len(df):,}")

                        # Отображаем статистику
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Всего сервисов", len(df))
                        with col2:
                            unique_imei = df['IMEI'].nunique() if 'IMEI' in df.columns else 0
                            st.metric("Уникальных IMEI", unique_imei)
                        with col3:
                            unique_customers = df['CUSTOMER_NAME'].nunique() if 'CUSTOMER_NAME' in df.columns else 0
                            st.metric("Уникальных клиентов", unique_customers)

                        st.markdown("---")

                        # Таблица с данными
                        st.subheader("📋 Данные отчета")

                        # Форматируем дату для отображения
                        if 'OPEN_DATE' in df.columns:
                            df_display = df.copy()
                            df_display['OPEN_DATE'] = pd.to_datetime(df_display['OPEN_DATE']).dt.strftime('%Y-%m-%d')
                            if 'CLOSE_DATE' in df_display.columns:
                                df_display['CLOSE_DATE'] = pd.to_datetime(df_display['CLOSE_DATE']).dt.strftime('%Y-%m-%d')
                        else:
                            df_display = df.copy()

                        st.dataframe(
                            df_display,
                            use_container_width=True,
                            hide_index=True,
                            height=400
                        )

                        # Экспорт данных
                        st.markdown("---")
                        st.subheader("💾 Экспорт данных")

                        col1, col2 = st.columns(2)

                        with col1:
                            csv_data = export_to_csv(df)
                            st.download_button(
                                label="📥 Скачать CSV",
                                data=csv_data,
                                file_name=f"lbs_services_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv",
                                use_container_width=True
                            )

                        with col2:
                            excel_data = export_to_excel(df)
                            st.download_button(
                                label="📥 Скачать Excel",
                                data=excel_data,
                                file_name=f"lbs_services_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True
                            )
                    else:
                        st.info("📭 Нет данных, соответствующих заданным фильтрам")
                else:
                    st.error("❌ Ошибка при загрузке данных. Проверьте подключение к базе данных.")
    
    st.markdown("---")
    st.caption("💡 **Tip:** Используйте фильтр IN_INVOICE, чтобы быстро найти услуги, которые никогда не попадали в СФ")

