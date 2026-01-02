"""
Закладка: Замена индексов интерфейсов 7206
"""
import streamlit as st
import pandas as pd
from datetime import datetime
from tabs.common import export_to_excel

def show_tab(get_connection):
    """
    Отображение закладки замены индексов интерфейсов 7206
    """
    st.header("🔧 Замена индексов интерфейсов 7206")
    st.markdown("Просмотр сервисов, которые нужно обновить при смене индексов интерфейсов с working на spare")
    st.markdown("---")
    
    conn = get_connection()
    if conn:
        try:
            query = """
                SELECT 
                    SERVICES_EXT_ID,
                    SERVICE_ID,
                    CUSTOMER_ID,
                    ACCOUNT_ID,
                    CUSTOMER_NAME,
                    OLD_VALUE,
                    NEW_VALUE,
                    INDEX_CHANGES,
                    DATE_BEG
                FROM V_7206_IFINDEX_REPLACEMENT
                ORDER BY CUSTOMER_NAME, SERVICE_ID
            """
            
            df = pd.read_sql(query, conn)
            
            if len(df) > 0:
                st.success(f"✅ Найдено записей для обновления: **{len(df)}**")
                
                # Фильтры
                col1, col2 = st.columns(2)
                with col1:
                    customer_filter = st.multiselect(
                        "Фильтр по клиенту",
                        options=sorted(df['CUSTOMER_NAME'].unique()),
                        key='ifindex_customer_filter'
                    )
                with col2:
                    service_filter = st.multiselect(
                        "Фильтр по SERVICE_ID",
                        options=sorted(df['SERVICE_ID'].unique()),
                        key='ifindex_service_filter'
                    )
                
                # Применение фильтров
                filtered_df = df.copy()
                if customer_filter:
                    filtered_df = filtered_df[filtered_df['CUSTOMER_NAME'].isin(customer_filter)]
                if service_filter:
                    filtered_df = filtered_df[filtered_df['SERVICE_ID'].isin(service_filter)]
                
                st.markdown(f"**Отображается записей: {len(filtered_df)}**")
                st.markdown("---")
                
                # Отображение данных
                display_columns = [
                    'SERVICE_ID', 'CUSTOMER_NAME', 'CUSTOMER_ID', 'ACCOUNT_ID',
                    'INDEX_CHANGES'
                ]
                
                st.subheader("📊 Список сервисов для обновления")
                st.dataframe(
                    filtered_df[display_columns],
                    use_container_width=True,
                    hide_index=True
                )
                
                # Показываем OLD_VALUE и NEW_VALUE для всех записей
                st.markdown("---")
                st.subheader("📝 Параметры подключения (OLD_VALUE и NEW_VALUE)")
                for idx, row in filtered_df.iterrows():
                    with st.expander(f"SERVICE_ID: {row['SERVICE_ID']} - {row['CUSTOMER_NAME']} (INDEX_CHANGES: {row.get('INDEX_CHANGES', 'N/A')})"):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.markdown("**OLD_VALUE (текущее значение в services_ext):**")
                            st.code(row['OLD_VALUE'], language='text')
                        with col2:
                            st.markdown("**NEW_VALUE (значение после замены):**")
                            st.code(row['NEW_VALUE'], language='text')
                
                # Детальная информация
                st.markdown("---")
                st.subheader("📝 Детальная информация")
                
                if len(filtered_df) > 0:
                    selected_idx = st.selectbox(
                        "Выберите запись для просмотра деталей",
                        range(len(filtered_df)),
                        format_func=lambda x: f"SERVICE_ID: {filtered_df.iloc[x]['SERVICE_ID']} - {filtered_df.iloc[x]['CUSTOMER_NAME']}"
                    )
                    
                    selected_row = filtered_df.iloc[selected_idx]
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("**Старый параметр подключения:**")
                        st.code(selected_row['OLD_VALUE'], language='text')
                    with col2:
                        st.markdown("**Новый параметр подключения:**")
                        st.code(selected_row['NEW_VALUE'], language='text')
                    
                    st.markdown("---")
                    if pd.notna(selected_row.get('INDEX_CHANGES')):
                        st.markdown("**Схема замены индексов:**")
                        st.info(selected_row['INDEX_CHANGES'])
                
                # Экспорт данных
                st.markdown("---")
                st.subheader("💾 Экспорт данных")
                col1, col2 = st.columns(2)
                with col1:
                    csv = filtered_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Скачать CSV",
                        data=csv,
                        file_name=f"7206_ifindex_replacement_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        key='download_csv_ifindex'
                    )
                with col2:
                    try:
                        excel_data = export_to_excel(filtered_df)
                        st.download_button(
                            label="📊 Скачать Excel",
                            data=excel_data,
                            file_name=f"7206_ifindex_replacement_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key='download_excel_ifindex'
                        )
                    except ImportError:
                        st.warning("⚠️ Для экспорта в Excel требуется библиотека openpyxl. Установите: pip install openpyxl")
                    except Exception as e:
                        st.error(f"❌ Ошибка при создании Excel файла: {e}")
                        import traceback
                        with st.expander("Детали ошибки"):
                            st.code(traceback.format_exc())
            else:
                st.info("ℹ️ Нет записей для обновления")
                
        except Exception as e:
            st.error(f"❌ Ошибка при загрузке данных: {e}")
            import traceback
            with st.expander("Детали ошибки"):
                st.code(traceback.format_exc())
        finally:
            conn.close()
    else:
        st.error("❌ Ошибка подключения к базе данных")


