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

# Конфигурация базы данных
# Используйте переменные окружения для паролей в production!
DB_CONFIG = {
    'dbname': os.getenv('POSTGRES_DB', 'billing'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'your-password-here'),
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432'))
}


def get_connection():
    """Создание подключения к базе данных"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        st.error(f"Ошибка подключения к базе данных: {e}")
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
        v.BILL_MONTH AS "Bill Month",
        v.TOTAL_USAGE_KB AS "Usage (KB)",
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
    ORDER BY v.BILL_MONTH DESC, "Calculated Overage ($)" DESC NULLS LAST
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            return df
        
        # Bill Month в базе уже в формате YYYYMM, для merge используем напрямую
        df['bill_month_num'] = df['Bill Month'].apply(lambda x: int(x) if pd.notna(x) else None)
        
        # Загружаем fees и делаем pivot по категориям
        # Мержим только по contract_id, так как периоды разные (календарный месяц vs 30-дневный цикл со 2-го по 2-е)
        fees_query = f"""
        SELECT fee_period_date, contract_id, category, SUM(amount) AS total_amount
        FROM v_steccom_access_fees_norm
        GROUP BY fee_period_date, contract_id, category
        """
        
        try:
            fees_df = pd.read_sql_query(fees_query, conn)
            
            if not fees_df.empty:
                # Pivot: категории -> колонки (агрегируем все периоды по contract_id)
                fees_pivot = fees_df.pivot_table(
                    index='contract_id',
                    columns='category',
                    values='total_amount',
                    aggfunc='sum',
                    fill_value=0
                ).reset_index()
                
                # Переименовываем колонки категорий
                fees_pivot.columns = [f"Fee: {col}" if col != 'contract_id' else col 
                                      for col in fees_pivot.columns]
                
                # Merge с основным отчетом только по contract_id
                df = df.merge(
                    fees_pivot,
                    left_on='Contract ID',
                    right_on='contract_id',
                    how='left'
                )
                
                # Заполняем отсутствующие суммы нулями для колонок Fee:*
                fee_cols = [c for c in fees_pivot.columns if c.startswith('Fee: ')]
                for c in fee_cols:
                    if c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

                # Удаляем служебные колонки
                df = df.drop(columns=['contract_id', 'bill_month_num'], errors='ignore')
            
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
        df['Bill Month'] = df['Bill Month'].apply(lambda x: 
            f"{int(x) // 100:04d}-{int(x) % 100:02d}" if pd.notna(x) else ""
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
    
    # Заголовок
    st.title("📊 Iridium M2M Overage Report")
    st.markdown("**All Plans (Calculated Overage for SBD-1 and SBD-10 only)**")
    st.markdown("---")
    
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
        st.caption("Database: PostgreSQL billing@localhost:5432")
    
    # Получение данных
    period_filter = None if selected_period == "All Periods" else selected_period
    plan_filter = None if selected_plan == "All Plans" else selected_plan
    
    df = get_main_report(period_filter, plan_filter)
    
    if df is not None and not df.empty:
        # Информация о выборке
        st.info(f"📊 Records: **{len(df)}** | IMEI: **{df['IMEI'].nunique()}**")
        
        # Таблица
        st.dataframe(
            df,
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
        
        # Собираем условие
        conditions = []
        if contract_ids:
            contract_list = "', '".join([str(c) for c in contract_ids])
            conditions.append(f"contract_id IN ('{contract_list}')")
        
        if contract_filter.strip():
            conditions.append(f"contract_id = '{contract_filter.strip()}'")
        
        # Фильтр по периоду (если выбран период в основном отчете)
        if period_filter and period_filter != "All Periods":
            year, month = period_filter.split('-')
            # Ищем fees с периодами близкими к выбранному месяцу (формат YYYYMMDD)
            # Например, для 2025-05 ищем периоды начинающиеся с 202505 или 202506
            period_pattern = f"{year}{month:0>2}"
            conditions.append(f"fee_period_date::text LIKE '{period_pattern}%'")
        
        fees_q_cond = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        fees_q = f"""
        SELECT 
            fee_period_date AS "Period",
            contract_id AS "Contract ID", 
            category AS "Category", 
            SUM(amount) AS "Amount"
        FROM v_steccom_access_fees_norm
        {fees_q_cond}
        GROUP BY fee_period_date, contract_id, category
        ORDER BY fee_period_date DESC, contract_id, category
        """

        conn2 = get_connection()
        if conn2:
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


if __name__ == "__main__":
    main()
