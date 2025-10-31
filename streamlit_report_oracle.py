#!/usr/bin/env python3
"""
Streamlit приложение для отчета по превышению трафика Iridium M2M
Версия для Oracle Database
Согласно ТЗ для отчета по расходам, по IMEI
"""

import streamlit as st
import pandas as pd
import cx_Oracle
from datetime import datetime
import io

# Конфигурация Oracle базы данных
DB_CONFIG = {
    'user': 'billing7',
    'password': 'billing',
    'host': '192.168.3.35',
    'port': 1521,
    'service_name': 'bm7'
}


def get_connection():
    """Создание подключения к Oracle"""
    try:
        dsn = cx_Oracle.makedsn(
            DB_CONFIG['host'],
            DB_CONFIG['port'],
            service_name=DB_CONFIG['service_name']
        )
        conn = cx_Oracle.connect(
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            dsn=dsn
        )
        return conn
    except Exception as e:
        st.error(f"Ошибка подключения к Oracle: {e}")
        return None


def convert_bill_month(bill_month):
    """Конвертация BILL_MONTH в читаемый формат"""
    if pd.isna(bill_month):
        return ""
    month = int(bill_month) // 10000
    year = int(bill_month) % 10000
    return f"{year}-{month:02d}"


def get_main_report():
    """Получение основного отчета согласно ТЗ"""
    conn = get_connection()
    if not conn:
        return None
    
    # Oracle-адаптированный запрос
    query = """
    SELECT 
        cor.IMEI,
        cor.CONTRACT_ID,
        se.ACTIVATION_DATE,
        cor.PLAN_NAME AS tariff,
        cor.BILL_MONTH,
        
        -- Из STECCOM
        SUM(CASE WHEN se.DESCRIPTION = 'Activation Fee' THEN se.AMOUNT ELSE 0 END) AS activation_fee,
        SUM(CASE WHEN se.DESCRIPTION = 'Advance Charge' THEN se.AMOUNT ELSE 0 END) AS advance_charge,
        SUM(CASE WHEN se.DESCRIPTION = 'Prorated' THEN se.AMOUNT ELSE 0 END) AS prorated,
        SUM(CASE WHEN se.DESCRIPTION = 'Credit' THEN se.AMOUNT ELSE 0 END) AS credit,
        
        -- Из SPNet
        cor.TOTAL_USAGE_KB,
        cor.INCLUDED_KB,
        cor.OVERAGE_KB,
        cor.CALCULATED_OVERAGE AS calculated_overage_charge,
        
        -- Итоги
        cor.SPNET_TOTAL_AMOUNT AS spnet_total,
        cor.STECCOM_TOTAL_AMOUNT AS steccom_total,
        
        -- Полная стоимость за период
        (cor.CALCULATED_OVERAGE + 
         SUM(CASE WHEN se.DESCRIPTION = 'Advance Charge' THEN se.AMOUNT ELSE 0 END)) AS total_charge
        
    FROM V_CONSOLIDATED_OVERAGE_REPORT cor
    LEFT JOIN STECCOM_EXPENSES se 
        ON cor.IMEI = se.ICC_ID_IMEI 
        AND cor.CONTRACT_ID = se.CONTRACT_ID
        AND TO_CHAR(se.INVOICE_DATE, 'YYYYMM') = (
            LPAD(TO_CHAR(MOD(cor.BILL_MONTH, 10000)), 4, '0') || 
            LPAD(TO_CHAR(TRUNC(cor.BILL_MONTH / 10000)), 2, '0')
        )
    WHERE cor.STECCOM_TOTAL_AMOUNT IS NOT NULL
      AND cor.BILL_MONTH IS NOT NULL
      AND cor.IMEI IS NOT NULL
    GROUP BY 
        cor.IMEI,
        cor.CONTRACT_ID,
        se.ACTIVATION_DATE,
        cor.PLAN_NAME,
        cor.BILL_MONTH,
        cor.TOTAL_USAGE_KB,
        cor.INCLUDED_KB,
        cor.OVERAGE_KB,
        cor.CALCULATED_OVERAGE,
        cor.SPNET_TOTAL_AMOUNT,
        cor.STECCOM_TOTAL_AMOUNT
    ORDER BY cor.BILL_MONTH DESC, cor.CALCULATED_OVERAGE DESC
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        
        # Форматирование
        df['period'] = df['bill_month'].apply(convert_bill_month)
        df['activation_date'] = pd.to_datetime(df['activation_date']).dt.strftime('%Y-%m-%d')
        
        # Переименование колонок для читаемости
        df = df.rename(columns={
            'imei': 'IMEI',
            'contract_id': 'Contract ID',
            'activation_date': 'Activation Date',
            'tariff': 'Тариф',
            'period': 'Период',
            'activation_fee': 'Activation Fee',
            'advance_charge': 'Advance Charge',
            'prorated': 'Prorated',
            'credit': 'Credit',
            'total_usage_kb': 'Использование (КБ)',
            'included_kb': 'Включено (КБ)',
            'overage_kb': 'Превышение (КБ)',
            'calculated_overage_charge': 'Стоимость превышения ($)',
            'spnet_total': 'SPNet Total ($)',
            'steccom_total': 'STECCOM Total ($)',
            'total_charge': 'Итого к оплате ($)'
        })
        
        return df
    
    except Exception as e:
        st.error(f"Ошибка получения данных: {e}")
        import traceback
        st.error(traceback.format_exc())
        return None
    finally:
        conn.close()


def get_statistics():
    """Получение общей статистики"""
    conn = get_connection()
    if not conn:
        return None
    
    query = """
    SELECT 
        COUNT(DISTINCT IMEI) AS unique_devices,
        COUNT(DISTINCT CONTRACT_ID) AS unique_contracts,
        COUNT(DISTINCT BILL_MONTH) AS periods,
        SUM(CALCULATED_OVERAGE) AS total_overage,
        SUM(ADVANCE_CHARGE) AS total_advance,
        SUM(CALCULATED_OVERAGE + ADVANCE_CHARGE) AS total_charges,
        AVG(TOTAL_USAGE_KB) AS avg_usage_kb
    FROM V_CONSOLIDATED_OVERAGE_REPORT
    WHERE STECCOM_TOTAL_AMOUNT IS NOT NULL
      AND BILL_MONTH IS NOT NULL
      AND IMEI IS NOT NULL
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        return df.iloc[0].to_dict()
    except Exception as e:
        st.error(f"Ошибка получения статистики: {e}")
        return None
    finally:
        conn.close()


def get_tariff_plans():
    """Получение списка тарифных планов"""
    conn = get_connection()
    if not conn:
        return []
    
    query = "SELECT DISTINCT PLAN_NAME FROM TARIFF_PLANS WHERE ACTIVE = 'Y' ORDER BY PLAN_NAME"
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        plans = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return plans
    except:
        return []
    finally:
        conn.close()


def get_periods():
    """Получение списка доступных периодов"""
    conn = get_connection()
    if not conn:
        return []
    
    query = "SELECT DISTINCT BILL_MONTH FROM SPNET_TRAFFIC ORDER BY BILL_MONTH DESC"
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        periods = [convert_bill_month(row[0]) for row in cursor.fetchall() if row[0]]
        cursor.close()
        return periods
    except:
        return []
    finally:
        conn.close()


def main():
    """Основная функция приложения"""
    
    # Настройка страницы
    st.set_page_config(
        page_title="Отчет по превышению трафика Iridium M2M",
        page_icon="📊",
        layout="wide"
    )
    
    # Заголовок
    st.title("📊 Отчет по расходам по IMEI (Iridium M2M)")
    st.caption("🔶 Oracle Database Version")
    st.markdown("---")
    
    # Боковая панель с фильтрами
    with st.sidebar:
        st.header("⚙️ Фильтры")
        
        # Получаем данные для фильтров
        tariff_plans = get_tariff_plans()
        periods = get_periods()
        
        # Фильтр по периоду
        selected_period = st.selectbox(
            "Период",
            ["Все периоды"] + periods
        )
        
        # Фильтр по тарифу
        selected_tariff = st.selectbox(
            "Тарифный план",
            ["Все тарифы"] + tariff_plans
        )
        
        # Фильтр по превышению
        min_overage = st.number_input(
            "Минимальное превышение ($)",
            min_value=0.0,
            value=0.0,
            step=10.0
        )
        
        st.markdown("---")
        
        # Кнопка обновления
        refresh = st.button("🔄 Обновить данные", use_container_width=True)
        
        # Информация о подключении
        st.markdown("---")
        st.caption("📡 База данных:")
        st.caption(f"Oracle {DB_CONFIG['service_name']}")
        st.caption(f"@ {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    
    # Основная область
    
    # Статистика
    st.header("📈 Общая статистика")
    stats = get_statistics()
    
    if stats:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Устройств", f"{stats['unique_devices']:,}")
            st.metric("Контрактов", f"{stats['unique_contracts']:,}")
        
        with col2:
            st.metric("Периодов", f"{stats['periods']:,}")
            st.metric("Средний трафик", f"{stats['avg_usage_kb']:,.0f} КБ")
        
        with col3:
            st.metric("Превышение", f"${stats['total_overage']:,.2f}")
            st.metric("Абонплата", f"${stats['total_advance']:,.2f}")
        
        with col4:
            st.metric("Итого", f"${stats['total_charges']:,.2f}", 
                     delta=None, delta_color="off")
            efficiency = (stats['total_overage'] / stats['total_charges'] * 100) if stats['total_charges'] > 0 else 0
            st.metric("Доля превышения", f"{efficiency:.1f}%")
    
    st.markdown("---")
    
    # Загрузка данных
    st.header("📋 Детальный отчет")
    
    with st.spinner("Загрузка данных из Oracle..."):
        df = get_main_report()
    
    if df is not None and not df.empty:
        # Применяем фильтры
        filtered_df = df.copy()
        
        if selected_period != "Все периоды":
            filtered_df = filtered_df[filtered_df['Период'] == selected_period]
        
        if selected_tariff != "Все тарифы":
            filtered_df = filtered_df[filtered_df['Тариф'] == selected_tariff]
        
        if min_overage > 0:
            filtered_df = filtered_df[filtered_df['Стоимость превышения ($)'] >= min_overage]
        
        # Показываем количество записей
        st.info(f"Найдено записей: {len(filtered_df):,}")
        
        # Таблица с данными
        st.dataframe(
            filtered_df[[
                'IMEI', 'Contract ID', 'Activation Date', 'Тариф', 'Период',
                'Использование (КБ)', 'Включено (КБ)', 'Превышение (КБ)',
                'Стоимость превышения ($)', 'Advance Charge', 'Итого к оплате ($)'
            ]],
            use_container_width=True,
            hide_index=True
        )
        
        # Кнопки экспорта
        st.markdown("---")
        st.header("💾 Экспорт данных")
        
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            # CSV экспорт (основной отчет)
            csv_buffer = io.StringIO()
            filtered_df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            csv_data = csv_buffer.getvalue()
            
            st.download_button(
                label="📥 Скачать CSV (основной)",
                data=csv_data,
                file_name=f"iridium_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        with col2:
            # CSV экспорт (полный отчет со всеми полями)
            csv_buffer_full = io.StringIO()
            filtered_df.to_csv(csv_buffer_full, index=False, encoding='utf-8-sig')
            csv_data_full = csv_buffer_full.getvalue()
            
            st.download_button(
                label="📥 Скачать CSV (все поля)",
                data=csv_data_full,
                file_name=f"iridium_report_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        with col3:
            # Excel экспорт (если установлен openpyxl)
            try:
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                    filtered_df.to_excel(writer, index=False, sheet_name='Отчет')
                excel_data = excel_buffer.getvalue()
                
                st.download_button(
                    label="📥 Скачать Excel",
                    data=excel_data,
                    file_name=f"iridium_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            except:
                st.info("Excel экспорт недоступен (установите openpyxl)")
        
        # Сводная статистика по отфильтрованным данным
        if len(filtered_df) > 0:
            st.markdown("---")
            st.header("📊 Статистика по выборке")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Записей", f"{len(filtered_df):,}")
            
            with col2:
                total_overage = filtered_df['Стоимость превышения ($)'].sum()
                st.metric("Превышение", f"${total_overage:,.2f}")
            
            with col3:
                total_advance = filtered_df['Advance Charge'].sum()
                st.metric("Абонплата", f"${total_advance:,.2f}")
            
            with col4:
                total = filtered_df['Итого к оплате ($)'].sum()
                st.metric("Итого", f"${total:,.2f}")
        
        # График топ устройств по превышению
        st.markdown("---")
        st.header("📈 Топ-10 устройств по превышению")
        
        top_devices = filtered_df.nlargest(10, 'Стоимость превышения ($)')
        
        chart_data = pd.DataFrame({
            'IMEI': top_devices['IMEI'].str[-8:],  # Последние 8 символов для читаемости
            'Превышение ($)': top_devices['Стоимость превышения ($)']
        })
        
        st.bar_chart(chart_data.set_index('IMEI'))
        
    else:
        st.warning("Нет данных для отображения")
    
    # Футер
    st.markdown("---")
    st.caption(f"Обновлено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.caption(f"База данных: Oracle {DB_CONFIG['service_name']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}")


if __name__ == "__main__":
    main()

