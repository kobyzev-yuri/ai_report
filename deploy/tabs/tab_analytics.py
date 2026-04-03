"""
Закладка: Счета за период
"""
import os
import streamlit as st
import pandas as pd
from datetime import datetime
from pathlib import Path

from tabs.common import export_to_excel
from tabs.report_filters import render_report_filters


def _get_project_root() -> Path:
    """Корень проекта (директория, где есть папка tabs/)."""
    script_path = Path(__file__).resolve()
    current = script_path
    while current.parent != current:
        if (current / "tabs").exists():
            return current
        current = current.parent
    return script_path.parent


def _resolve_export_dir() -> Path:
    raw = os.getenv("ANALYTICS_EXPORT_DIR", "exports/analytics_invoices").strip()
    p = Path(raw)
    if p.is_absolute():
        return p
    return _get_project_root() / p


def _safe_period_slug(period_filter) -> str:
    if period_filter is None:
        return "all"
    s = str(period_filter).strip().replace("/", "_").replace("\\", "_").replace(" ", "_")
    return (s[:120] if s else "all")


def show_tab(
    get_connection,
    get_analytics_invoice_period_report,
    get_analytics_duplicates,
    get_periods,
    get_plans,
    remove_analytics_duplicates,
):
    """
    Отображение закладки отчетов по счетам за период.
    Фильтры — в теле вкладки.
    """
    st.header("📋 Счета за период")
    st.markdown("Отчет по счетам за период на основе таблицы ANALYTICS. Иерархия: клиент → договор → сервис.")

    selected_period, _plan, contract_id_filter, imei_filter, customer_name_filter, code_1c_filter = (
        render_report_filters(get_connection, get_periods, get_plans, include_plan=False)
    )

    # Создаем подвкладки
    sub_tab_report, sub_tab_duplicates = st.tabs([
        "📊 Отчет по счетам",
        "🔍 Проверка дубликатов"
    ])

    period_filter = selected_period
    contract_id_filter = contract_id_filter if contract_id_filter else None
    imei_filter = imei_filter if imei_filter else None
    customer_name_filter = customer_name_filter if customer_name_filter else None
    code_1c_filter = code_1c_filter if code_1c_filter else None
    
    # ========== SUB TAB: ОТЧЕТ ПО СЧЕТАМ ==========
    with sub_tab_report:
        # Дополнительные фильтры
        col1, col2 = st.columns(2)
        with col1:
            tariff_filter = st.text_input(
                "Tariff ID",
                value="",
                key='tariff_filter',
                help="Фильтр по ID тарифа (BM_TARIFF.TARIFF_ID)"
            )
        with col2:
            zone_filter = st.text_input(
                "Zone ID",
                value="",
                key='zone_filter',
                help="Фильтр по ID зоны (BM_ZONE.ZONE_ID)"
            )
        
        filter_key = f"analytics_{period_filter}_{contract_id_filter}_{imei_filter}_{customer_name_filter}_{code_1c_filter}_{tariff_filter}_{zone_filter}"
        
        # Кнопка загрузки отчета
        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown("**Настройте фильтры и нажмите кнопку для загрузки отчета:**")
        with col2:
            load_analytics = st.button("📊 Загрузить отчет", type="primary", use_container_width=True, key="load_analytics_btn")
        
        if load_analytics:
            if period_filter is not None:
                with st.spinner("Загрузка данных по счетам за период..."):
                    df_analytics = get_analytics_invoice_period_report(
                        get_connection,
                        period_filter,
                        contract_id_filter,
                        imei_filter,
                        customer_name_filter,
                        code_1c_filter,
                        tariff_filter if tariff_filter else None,
                        zone_filter if zone_filter else None
                    )
                    st.session_state.last_analytics_key = filter_key
                    st.session_state.last_analytics_df = df_analytics
                    st.session_state.analytics_loaded = True
            else:
                st.warning("⚠️ Выберите период для загрузки отчета")
                df_analytics = None
                st.session_state.analytics_loaded = False
        else:
            saved_key = st.session_state.get('last_analytics_key')
            if (st.session_state.get('analytics_loaded', False) and 
                saved_key is not None and 
                saved_key == filter_key):
                df_analytics = st.session_state.get('last_analytics_df', None)
            else:
                df_analytics = None
                if saved_key is not None and saved_key != filter_key:
                    st.session_state.analytics_loaded = False
                if not st.session_state.get('analytics_loaded', False):
                    st.info("ℹ️ Настройте фильтры и нажмите кнопку 'Загрузить отчет' для просмотра данных")
        
        # Показываем отчет ТОЛЬКО если он был загружен по кнопке
        if df_analytics is not None and not df_analytics.empty and st.session_state.get('analytics_loaded', False):
            st.success(f"✅ Загружено записей: {len(df_analytics):,}")
            
            # Простая статистика - используем правильные названия колонок из VIEW
            st.markdown("---")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                # Ищем колонку с общей суммой
                total_sum = 0
                if 'MONEY' in df_analytics.columns:
                    total_sum = df_analytics['MONEY'].sum()
                elif 'MONEY_ABON' in df_analytics.columns and 'MONEY_TRAFFIC' in df_analytics.columns:
                    total_sum = df_analytics['MONEY_ABON'].sum() + df_analytics['MONEY_TRAFFIC'].sum()
                st.metric("Всего сумм (руб)", f"{total_sum:,.2f}")
            with col2:
                if 'MONEY_ABON' in df_analytics.columns:
                    st.metric("Абонплата (руб)", f"{df_analytics['MONEY_ABON'].sum():,.2f}")
                else:
                    st.metric("Абонплата", "N/A")
            with col3:
                if 'MONEY_TRAFFIC' in df_analytics.columns:
                    st.metric("Трафик (руб)", f"{df_analytics['MONEY_TRAFFIC'].sum():,.2f}")
                else:
                    st.metric("Трафик", "N/A")
            with col4:
                st.metric("Записей", f"{len(df_analytics):,}")
            
            st.markdown("---")
            
            # Детализированная таблица - используем все доступные колонки
            st.subheader("📋 Детализированный отчет")
            # Определяем доступные колонки для отображения
            available_columns = []
            column_mapping = {
                'PERIOD_YYYYMM': 'Период',
                'CUSTOMER_NAME': 'Клиент',
                'CODE_1C': 'Код 1С',
                'ACCOUNT_NAME': 'Договор',
                'CONTRACT_ID': 'Contract ID',
                'SERVICE_ID': 'Service ID',
                'IMEI': 'IMEI',
                'TARIFF_NAME': 'Тариф',
                'ZONE_NAME': 'Зона',
                'RESOURCE_TYPE_MNEMONIC': 'Тип ресурса',
                'RESOURCE_TYPE_NAME': 'Название ресурса',
                'MONEY': 'Сумма (руб)',
                'MONEY_ABON': 'Абонплата (руб)',
                'MONEY_TRAFFIC': 'Трафик (руб)',
                'TRAF': 'Трафик (объем)',
                'TOTAL_TRAF': 'Общий трафик',
                'IN_INVOICE': 'В счете',
                'SERVICE_STATUS': 'Статус услуги'
            }
            
            # Собираем только те колонки, которые есть в DataFrame
            for db_col, display_name in column_mapping.items():
                if db_col in df_analytics.columns:
                    available_columns.append(db_col)
            
            # Если есть другие колонки, добавляем их тоже
            other_cols = [col for col in df_analytics.columns if col not in column_mapping.keys()]
            available_columns.extend(other_cols)
            
            display_df = df_analytics[available_columns].copy()
            # Переименовываем колонки для отображения
            display_df = display_df.rename(columns=column_mapping)
            for col in display_df.columns:
                if display_df[col].dtype == 'object':
                    display_df[col] = display_df[col].fillna('')
            
            st.dataframe(display_df, use_container_width=True, height=400)
            
            # Экспорт
            st.markdown("---")
            col1, col2 = st.columns(2)
            with col1:
                csv_data = df_analytics.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Скачать CSV",
                    data=csv_data,
                    file_name=f"analytics_invoice_period_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            with col2:
                import io as io_module
                excel_buffer = io_module.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                    df_analytics.to_excel(writer, index=False, sheet_name='Analytics Invoice Period')
                    worksheet = writer.sheets['Analytics Invoice Period']
                    from openpyxl.utils import get_column_letter
                    for idx, col in enumerate(df_analytics.columns, start=1):
                        max_length = max(
                            df_analytics[col].astype(str).map(len).max() if len(df_analytics) > 0 else 0,
                            len(str(col))
                        )
                        col_letter = get_column_letter(idx)
                        worksheet.column_dimensions[col_letter].width = min(max_length + 2, 50)
                excel_buffer.seek(0)
                excel_data = excel_buffer.getvalue()
                st.download_button(
                    label="📥 Скачать Excel",
                    data=excel_data,
                    file_name=f"analytics_invoice_period_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            st.markdown("---")
            st.subheader("💾 Сохранение на сервер")
            st.caption(
                "Каталог задаётся переменной `ANALYTICS_EXPORT_DIR` "
                "(относительно корня приложения или абсолютный путь)."
            )

            export_dir = _resolve_export_dir()
            if st.button("💾 Сохранить отчёт на сервер (CSV + Excel)", key="analytics_save_server"):
                try:
                    export_dir.mkdir(parents=True, exist_ok=True)
                    slug = _safe_period_slug(period_filter)
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    base = f"analytics_invoice_period_{slug}_{ts}"
                    csv_path = export_dir / f"{base}.csv"
                    xlsx_path = export_dir / f"{base}.xlsx"
                    df_analytics.to_csv(csv_path, index=False, encoding="utf-8-sig")
                    xlsx_path.write_bytes(export_to_excel(df_analytics))
                    st.success(
                        f"Сохранено на сервер:\n- `{csv_path.resolve()}`\n- `{xlsx_path.resolve()}`"
                    )
                except Exception as e:
                    st.error(f"Не удалось сохранить на сервер: {e}")
        elif df_analytics is not None and df_analytics.empty:
            st.warning("⚠️ Данные не найдены для выбранных фильтров")
    
    # ========== SUB TAB: ПРОВЕРКА ДУБЛИКАТОВ ==========
    with sub_tab_duplicates:
        st.header("🔍 Проверка дубликатов в ANALYTICS")
        st.markdown("Поиск записей, где **все ключевые поля совпадают**, кроме AID (первичного ключа).")
        st.info("💡 Дубликаты могут возникать при повторной загрузке данных или ошибках в процессе формирования ANALYTICS.")
        st.warning("⚠️ **Важно**: Дубликаты определяются по всем бизнес-полям таблицы ANALYTICS (включая ZONE_ID, TARIFFEL_ID, COUNTER_CF и др.). Если записи различаются хотя бы одним полем, они НЕ считаются дубликатами.")
        
        conn = get_connection()
        if conn:
            try:
                periods_query = """
                SELECT 
                    p.PERIOD_ID,
                    TO_CHAR(p.START_DATE, 'YYYY-MM') AS PERIOD_YYYYMM,
                    p.MONTH AS PERIOD_NAME,
                    p.START_DATE,
                    p.STOP_DATE
                FROM BM_PERIOD p
                ORDER BY p.PERIOD_ID DESC
                """
                periods_df = pd.read_sql_query(periods_query, conn)
                
                if not periods_df.empty:
                    period_options = [
                        f"{row['PERIOD_ID']} - {row['PERIOD_YYYYMM']} ({row['PERIOD_NAME']})"
                        for _, row in periods_df.iterrows()
                    ]
                    period_options.insert(0, "Выберите период...")
                    
                    selected_period_option = st.selectbox(
                        "Выберите период (PERIOD_ID) для проверки дубликатов:",
                        period_options,
                        key='duplicates_period_select'
                    )
                    
                    if selected_period_option and selected_period_option != "Выберите период...":
                        period_id = int(selected_period_option.split(' - ')[0])
                        
                        st.markdown("---")
                        
                        col_btn1, col_btn2 = st.columns(2)
                        with col_btn1:
                            if st.button("🔍 Найти дубликаты", key='find_duplicates_btn', use_container_width=True):
                                # Очищаем предыдущие результаты
                                if 'duplicates_df' in st.session_state:
                                    del st.session_state.duplicates_df
                                if 'duplicates_found' in st.session_state:
                                    del st.session_state.duplicates_found
                                
                                with st.spinner("Поиск дубликатов..."):
                                    # Используем функцию, переданную как параметр
                                    duplicates_df = get_analytics_duplicates(get_connection, period_id)
                                    
                                    if duplicates_df is not None and not duplicates_df.empty:
                                        st.session_state.duplicates_found = True
                                        st.session_state.duplicates_df = duplicates_df
                                        st.session_state.duplicates_period_id = period_id
                                        st.success(f"✅ Найдено групп дубликатов: {len(duplicates_df):,}")
                                    elif duplicates_df is not None and duplicates_df.empty:
                                        st.session_state.duplicates_found = False
                                        st.info("✅ Дубликатов не найдено")
                                    else:
                                        st.session_state.duplicates_found = False
                                        st.error("❌ Ошибка при поиске дубликатов")
                        
                        # Показываем результаты поиска дубликатов
                        if st.session_state.get('duplicates_found', False) and st.session_state.get('duplicates_period_id') == period_id:
                            duplicates_df = st.session_state.get('duplicates_df')
                            if duplicates_df is not None and not duplicates_df.empty:
                                st.markdown("---")
                                st.subheader("📊 Найденные дубликаты")
                                
                                # Статистика
                                total_duplicate_records = duplicates_df['DUPLICATE_COUNT'].sum() if 'DUPLICATE_COUNT' in duplicates_df.columns else 0
                                total_groups = len(duplicates_df)
                                records_to_delete = total_duplicate_records - total_groups  # Удаляем все кроме одной в каждой группе
                                
                                col1, col2, col3 = st.columns(3)
                                with col1:
                                    st.metric("Групп дубликатов", total_groups)
                                with col2:
                                    st.metric("Всего дублирующихся записей", total_duplicate_records)
                                with col3:
                                    st.metric("Записей к удалению", records_to_delete)
                                
                                # Выводим все поля дубликатов
                                # Настраиваем отображение всех колонок
                                pd.set_option('display.max_columns', None)
                                pd.set_option('display.width', None)
                                pd.set_option('display.max_colwidth', 100)
                                
                                # Показываем информацию о колонках
                                st.info(f"📊 Всего полей в отчете: **{len(duplicates_df.columns)}** | Всего строк: **{len(duplicates_df)}**")
                                
                                # Выводим DataFrame со всеми колонками
                                # Принудительно показываем все колонки
                                display_df = duplicates_df.copy()
                                
                                # Убеждаемся, что все колонки видны
                                st.dataframe(
                                    display_df,
                                    use_container_width=True,
                                    height=600,
                                    hide_index=True,
                                    column_config={
                                        col: st.column_config.TextColumn(col, width="small")
                                        for col in display_df.columns
                                    }
                                )
                                
                                # Дополнительная информация для отладки
                                with st.expander("🔍 Отладочная информация"):
                                    st.write(f"**Количество колонок в DataFrame:** {len(display_df.columns)}")
                                    st.write(f"**Первые 10 колонок:** {', '.join(display_df.columns.tolist()[:10])}")
                                    
                                # Показываем список всех колонок
                                with st.expander("📋 Список всех полей в отчете"):
                                    cols_list = duplicates_df.columns.tolist()
                                    st.write(f"**Всего полей: {len(cols_list)}**")
                                    # Разбиваем на колонки для лучшего отображения
                                    col1, col2, col3 = st.columns(3)
                                    chunk_size = (len(cols_list) + 2) // 3
                                    with col1:
                                        st.write("**Колонки 1:**")
                                        for col in cols_list[:chunk_size]:
                                            st.write(f"- {col}")
                                    with col2:
                                        st.write("**Колонки 2:**")
                                        for col in cols_list[chunk_size:chunk_size*2]:
                                            st.write(f"- {col}")
                                    with col3:
                                        st.write("**Колонки 3:**")
                                        for col in cols_list[chunk_size*2:]:
                                            st.write(f"- {col}")
                                
                                # Экспорт
                                st.markdown("---")
                                col1, col2 = st.columns(2)
                                with col1:
                                    csv_data = duplicates_df.to_csv(index=False).encode('utf-8')
                                    st.download_button(
                                        label="📥 Скачать CSV",
                                        data=csv_data,
                                        file_name=f"analytics_duplicates_{period_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                        mime="text/csv"
                                    )
                                
                                # Удаление дубликатов
                                st.markdown("---")
                                st.subheader("🗑️ Удаление дубликатов")
                                st.warning(f"⚠️ Будет удалено {records_to_delete} записей (останутся только записи с максимальным AID в каждой группе)")
                                
                                if remove_analytics_duplicates:
                                    # Двойное подтверждение
                                    confirm_text = st.text_input(
                                        "Для подтверждения введите 'УДАЛИТЬ ДУБЛИКАТЫ'",
                                        key='confirm_delete_duplicates',
                                        placeholder="УДАЛИТЬ ДУБЛИКАТЫ"
                                    )
                                    
                                    if confirm_text == "УДАЛИТЬ ДУБЛИКАТЫ":
                                        if st.button("🗑️ Удалить дубликаты", type="primary", key='delete_duplicates_btn', use_container_width=True):
                                            with st.spinner("Удаление дубликатов..."):
                                                success, deleted_count, message = remove_analytics_duplicates(get_connection, period_id)
                                                
                                                if success:
                                                    st.success(message)
                                                    # Очищаем состояние после успешного удаления
                                                    st.session_state.duplicates_found = False
                                                    st.session_state.duplicates_df = None
                                                    st.rerun()
                                                else:
                                                    st.error(message)
                                    else:
                                        st.info("💡 Введите 'УДАЛИТЬ ДУБЛИКАТЫ' для активации кнопки удаления")
                                else:
                                    st.info("💡 Функция удаления дубликатов недоступна")
            except Exception as e:
                st.error(f"❌ Ошибка: {e}")
                import traceback
                with st.expander("Детали ошибки"):
                    st.code(traceback.format_exc())
            finally:
                conn.close()
        else:
            st.error("❌ Ошибка подключения к базе данных")





