"""
Закладка: Услуги LBS
Отчет по активным SBD IMEI сервисам без расходов за последний месяц
"""
import streamlit as st
import pandas as pd
from datetime import datetime
from tabs.common import export_to_csv, export_to_excel

def show_tab(get_connection, get_lbs_services_report):
    """
    Отображение закладки отчета по услугам LBS
    
    Args:
        get_connection: Функция получения подключения к БД
        get_lbs_services_report: Функция получения отчета по LBS услугам
    """
    st.header("📍 Услуги LBS")
    st.markdown("Отчет по активным SBD IMEI сервисам без расходов за последний месяц")
    
    # Информация о отчете
    with st.expander("ℹ️ О отчете", expanded=False):
        st.markdown("""
        **Назначение отчета:**
        
        Отчет показывает активные SBD IMEI сервисы (TYPE_ID=9002), которые:
        - Имеют заполненную дату начала (OPEN_DATE)
        - Не имеют даты закрытия (CLOSE_DATE пустая)
        - Отсутствуют в отчете по расходам за последний месяц
        
        **Столбцы отчета:**
        - **IMEI** - номер устройства Iridium
        - **SERVICE_ID** - идентификатор услуги в биллинге
        - **CUSTOMER_NAME** - название клиента (организация или ФИО)
        - **AGREEMENT_NUMBER** - номер договора в биллинге
        - **SUB_IRIDIUM** - логин сервиса (SUB-XXXXX)
        - **CODE_1C** - код клиента из системы 1С
        - **OPEN_DATE** - дата начала предоставления услуги
        
        **Примечание:** 
        По некоторым IMEI трафик может отсутствовать, а абонплата тоже. 
        Тем не менее, эти сервисы могли быть заведены, и их количество интересует финансистов.
        """)
    
    st.markdown("---")
    
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
        st.markdown("**Настройте фильтры и нажмите кнопку для загрузки отчета:**")
    with col_opts2:
        load_report = st.button("📊 Загрузить отчет", type="primary", use_container_width=True, key='lbs_load_report')
    
    # Загружаем отчет только при нажатии кнопки
    if load_report:
        with st.spinner("Загрузка данных отчета..."):
            df = get_lbs_services_report(
                get_connection,
                contract_id_filter if contract_id_filter else None,
                imei_filter if imei_filter else None,
                customer_name_filter if customer_name_filter else None,
                code_1c_filter if code_1c_filter else None,
                exclude_steccom
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
    st.caption("💡 **Tip:** Отчет показывает только активные сервисы без расходов за последний месяц")

