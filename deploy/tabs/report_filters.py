"""
Общие фильтры для отчётов (основная область страницы, не сайдбар).
"""
import streamlit as st


def render_report_filters(get_connection, get_periods, get_plans=None, *, include_plan=True):
    """
    Виджеты периода, опционально плана, доп. фильтров и проверки Oracle.

    Returns:
        selected_period, selected_plan, contract_id_filter, imei_filter,
        customer_name_filter, code_1c_filter
    """
    st.subheader("⚙️ Фильтры отчёта")

    periods_data = get_periods(get_connection)
    if not periods_data:
        st.error("⚠️ Не удалось загрузить периоды.")
        st.stop()

    period_display_list = sorted(list(set(d for d, _ in periods_data if d)), reverse=True)
    period_options = ["All Periods"] + period_display_list

    if "selected_period_index" not in st.session_state:
        st.session_state.selected_period_index = 0

    period_index = min(st.session_state.selected_period_index, len(period_options) - 1)

    if include_plan:
        colp1, colp2 = st.columns(2)
        with colp1:
            selected_period_display = st.selectbox(
                "Отчетный период",
                period_options,
                index=period_index,
                key="period_selectbox",
            )
        with colp2:
            if get_plans is None:
                raise ValueError("get_plans обязателен при include_plan=True")
            plans = get_plans(get_connection)
            plan_options = ["All Plans"] + plans
            selected_plan = st.selectbox("План", plan_options, key="plan_selectbox")
    else:
        selected_period_display = st.selectbox(
            "Отчетный период",
            period_options,
            index=period_index,
            key="period_selectbox",
        )
        selected_plan = "All Plans"

    selected_period = None if selected_period_display == "All Periods" else selected_period_display

    st.markdown("##### 🔍 Дополнительные фильтры")
    fc1, fc2 = st.columns(2)
    with fc1:
        contract_id_filter = st.text_input("Contract ID (SUB-*)", key="contract_id_filter")
        customer_name_filter = st.text_input("Organization/Person", key="customer_name_filter")
    with fc2:
        imei_filter = st.text_input("IMEI", key="imei_filter")
        code_1c_filter = st.text_input("Code 1C", key="code_1c_filter")

    if st.button("🔌 Test Connection", key="test_connection_btn"):
        test_conn = get_connection()
        if test_conn:
            st.success("✅ Подключение успешно!")
            test_conn.close()
        else:
            st.error("❌ Ошибка подключения.")

    st.markdown("---")

    return (
        selected_period,
        selected_plan,
        contract_id_filter,
        imei_filter,
        customer_name_filter,
        code_1c_filter,
    )
