"""
Закладка: Маппинг индексов интерфейсов 7206
"""
import streamlit as st
import pandas as pd
from datetime import datetime
from tabs.common import export_to_excel

def show_tab(get_connection):
    """
    Отображение закладки маппинга индексов интерфейсов 7206
    """
    st.header("🔀 Маппинг индексов интерфейсов 7206")
    st.markdown("Отображение соответствия индексов интерфейсов между working и spare конфигурациями")
    st.markdown("---")
    
    conn = get_connection()
    if conn:
        try:
            query = """
                SELECT 
                    working_index,
                    spare_index,
                    working_mac,
                    spare_mac,
                    interface
                FROM V_7206_IFINDEX_MAPPING
                ORDER BY working_index
            """
            
            df = pd.read_sql(query, conn)
            
            if len(df) > 0:
                # Oracle возвращает колонки в верхнем регистре, приводим к нижнему
                df.columns = df.columns.str.lower()
                
                st.success(f"✅ Найдено записей маппинга: **{len(df)}**")
                
                # Фильтры
                col1, col2, col3 = st.columns(3)
                with col1:
                    working_filter = st.multiselect(
                        "Фильтр по working_index",
                        options=sorted(df['working_index'].unique()),
                        key='mapping_working_filter'
                    )
                with col2:
                    spare_filter = st.multiselect(
                        "Фильтр по spare_index",
                        options=sorted(df['spare_index'].unique()),
                        key='mapping_spare_filter'
                    )
                with col3:
                    interface_search = st.text_input(
                        "Поиск по интерфейсу",
                        key='mapping_interface_search',
                        placeholder="Например: GigabitEthernet"
                    )
                
                # Применение фильтров
                filtered_df = df.copy()
                if working_filter:
                    filtered_df = filtered_df[filtered_df['working_index'].isin(working_filter)]
                if spare_filter:
                    filtered_df = filtered_df[filtered_df['spare_index'].isin(spare_filter)]
                if interface_search:
                    filtered_df = filtered_df[filtered_df['interface'].str.contains(interface_search, case=False, na=False)]
                
                st.markdown(f"**Отображается записей: {len(filtered_df)}**")
                st.markdown("---")
                
                # Отображение данных в виде таблицы
                st.subheader("📊 Таблица маппинга индексов")
                
                # Форматируем данные для лучшей читаемости
                display_df = filtered_df.copy()
                display_df['Маппинг индексов'] = display_df['working_index'].astype(str) + ' → ' + display_df['spare_index'].astype(str)
                display_df['Маппинг MAC'] = display_df['working_mac'].astype(str) + ' → ' + display_df['spare_mac'].astype(str)
                
                display_columns = [
                    'working_index', 'spare_index', 'Маппинг индексов',
                    'working_mac', 'spare_mac', 'Маппинг MAC',
                    'interface'
                ]
                
                st.dataframe(
                    display_df[display_columns].rename(columns={
                        'working_index': 'Working Index',
                        'spare_index': 'Spare Index',
                        'working_mac': 'Working MAC',
                        'spare_mac': 'Spare MAC',
                        'interface': 'Интерфейс'
                    }),
                    use_container_width=True,
                    hide_index=True
                )
                
                # Визуализация маппинга
                st.markdown("---")
                st.subheader("📈 Визуализация замены индексов")
                
                # Группировка по интерфейсам
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("**Старая конфигурация (Working):**")
                    working_summary = filtered_df.groupby('interface').agg({
                        'working_index': 'count',
                        'working_mac': lambda x: ', '.join(x.astype(str))
                    }).rename(columns={'working_index': 'Количество индексов'})
                    st.dataframe(working_summary, use_container_width=True)
                
                with col2:
                    st.markdown("**Новая конфигурация (Spare):**")
                    spare_summary = filtered_df.groupby('interface').agg({
                        'spare_index': 'count',
                        'spare_mac': lambda x: ', '.join(x.astype(str))
                    }).rename(columns={'spare_index': 'Количество индексов'})
                    st.dataframe(spare_summary, use_container_width=True)
                
                # Экспорт данных
                st.markdown("---")
                st.subheader("💾 Экспорт данных")
                col1, col2 = st.columns(2)
                with col1:
                    csv = filtered_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Скачать CSV",
                        data=csv,
                        file_name=f"7206_ifindex_mapping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        key='download_csv_mapping'
                    )
                with col2:
                    try:
                        excel_data = export_to_excel(filtered_df)
                        st.download_button(
                            label="📊 Скачать Excel",
                            data=excel_data,
                            file_name=f"7206_ifindex_mapping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key='download_excel_mapping'
                        )
                    except ImportError:
                        st.warning("⚠️ Для экспорта в Excel требуется библиотека openpyxl. Установите: pip install openpyxl")
                    except Exception as e:
                        st.error(f"❌ Ошибка при создании Excel файла: {e}")
                        import traceback
                        with st.expander("Детали ошибки"):
                            st.code(traceback.format_exc())
            else:
                st.info("ℹ️ Нет данных маппинга")
                
        except Exception as e:
            st.error(f"❌ Ошибка при загрузке данных: {e}")
            import traceback
            with st.expander("Детали ошибки"):
                st.code(traceback.format_exc())
        finally:
            conn.close()
    else:
        st.error("❌ Ошибка подключения к базе данных")


