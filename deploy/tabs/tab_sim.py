"""
Закладка: SIM (услуги TYPE_ID=9001, iridium_pstn)
Справочник сервисов с атрибутами из SERVICES_EXT / DICT (ICCID, IMSI, MSISDN…).
"""
import streamlit as st
from datetime import datetime
from tabs.common import export_to_csv, export_to_excel


_DISPLAY_COLS = [
    "CUSTOMER_NAME",
    "ACCOUNT_ID",
    "SERVICE_ID",
    "DESCRIPTION",
    "ICCID",
    "IMSI",
    "PSTN_NUMBER_RUS",
    "PSTN_NUMBER",
    "PSTN_NUMBER_DATA",
]
_RENAME = {
    "PSTN_NUMBER_RUS": "Телефон абонента РФ",
    "PSTN_NUMBER": "Телефон абонента MSISDN",
    "PSTN_NUMBER_DATA": "Телефон абонента MSISDN-DATA",
}


def _to_display(df):
    cols = [c for c in _DISPLAY_COLS if c in df.columns]
    out = df[cols].copy() if cols else df.copy()
    return out.rename(columns=_RENAME)


def show_tab(get_connection, get_sim_services_report):
    """
    Отображение закладки SIM-услуг (TYPE_ID=9001).

    Args:
        get_connection: Функция получения подключения к БД
        get_sim_services_report: Функция получения отчёта по SIM
    """
    st.header("📱 SIM")
    st.markdown(
        "Услуги телефонии Iridium (**TYPE_ID=9001**) с атрибутами из `SERVICES_EXT` "
        "(ICCID, IMSI, телефоны)."
    )

    with st.expander("ℹ️ О отчёте", expanded=False):
        st.markdown(
            """
        **Источник:** `SERVICES` + `SERVICES_EXT` + `DICT` (`TYPE_ID=9001`).

        **Столбцы:**
        - **CUSTOMER_NAME** — организация / ФИО (`BM_CUSTOMER_CONTACT`)
        - **ACCOUNT_ID** — лицевой счёт
        - **SERVICE_ID** — услуга
        - **DESCRIPTION** — описание услуги (`SERVICES.DESCRIPTION`)
        - **ICCID** — DICT_ID=123; если пусто — `SERVICES.VSAT`
        - **IMSI** — DICT_ID=83
        - **PSTN_NUMBER_RUS** — телефон РФ (DICT_ID=87)
        - **PSTN_NUMBER** — MSISDN (DICT_ID=84)
        - **PSTN_NUMBER_DATA** — MSISDN-DATA (DICT_ID=86)

        Учитываются только актуальные атрибуты: `SERVICES_EXT.DATE_END IS NULL`.
        """
        )

    st.markdown("---")

    col1, col2 = st.columns(2)
    with col1:
        customer_name_filter = st.text_input(
            "Название клиента",
            key="sim_customer_name",
            help="Подстрока в названии организации или ФИО",
        )
        account_id_filter = st.text_input(
            "Account ID",
            key="sim_account_id",
            help="Точное или частичное совпадение ACCOUNT_ID",
        )
        service_id_filter = st.text_input(
            "Service ID",
            key="sim_service_id",
            help="Точное или частичное совпадение SERVICE_ID",
        )
    with col2:
        iccid_filter = st.text_input("ICCID", key="sim_iccid")
        imsi_filter = st.text_input("IMSI", key="sim_imsi")
        msisdn_filter = st.text_input(
            "MSISDN / телефон",
            key="sim_msisdn",
            help="Поиск по PSTN_NUMBER_RUS, PSTN_NUMBER или PSTN_NUMBER_DATA",
        )

    col_opts1, col_opts2 = st.columns([3, 1])
    with col_opts1:
        only_active = st.checkbox(
            "Только активные (CLOSE_DATE пустая)",
            value=True,
            key="sim_only_active",
            help="Если включено — услуги без даты закрытия",
        )
        exclude_steccom = st.checkbox(
            "Без СТЭККОМ (customer_id=521)",
            value=True,
            key="sim_exclude_steccom",
        )
        st.markdown("**Настройте фильтры и нажмите кнопку для загрузки:**")
    with col_opts2:
        load_report = st.button(
            "📊 Загрузить",
            type="primary",
            use_container_width=True,
            key="sim_load_report",
        )

    filter_key = "|".join(
        [
            customer_name_filter or "",
            account_id_filter or "",
            service_id_filter or "",
            iccid_filter or "",
            imsi_filter or "",
            msisdn_filter or "",
            str(bool(only_active)),
            str(bool(exclude_steccom)),
        ]
    )

    if load_report:
        with st.spinner("Загрузка данных SIM..."):
            df = get_sim_services_report(
                get_connection,
                customer_name_filter=customer_name_filter or None,
                account_id_filter=account_id_filter or None,
                service_id_filter=service_id_filter or None,
                iccid_filter=iccid_filter or None,
                imsi_filter=imsi_filter or None,
                msisdn_filter=msisdn_filter or None,
                only_active=only_active,
                exclude_steccom=exclude_steccom,
            )
        st.session_state.sim_filter_key = filter_key
        st.session_state.sim_df = df
        st.session_state.sim_loaded = df is not None
    else:
        saved_key = st.session_state.get("sim_filter_key")
        if (
            st.session_state.get("sim_loaded", False)
            and saved_key is not None
            and saved_key == filter_key
        ):
            df = st.session_state.get("sim_df")
        else:
            df = None
            if saved_key is not None and saved_key != filter_key:
                st.session_state.sim_loaded = False
                st.session_state.sim_df = None
            if not st.session_state.get("sim_loaded", False):
                st.info("ℹ️ Настройте фильтры и нажмите «Загрузить» для просмотра данных")

    if df is None and st.session_state.get("sim_loaded") and load_report:
        st.error("❌ Ошибка при загрузке данных. Проверьте подключение к БД.")
    elif df is not None and df.empty and st.session_state.get("sim_loaded", False):
        st.info("📭 Нет данных, соответствующих заданным фильтрам")
    elif df is not None and not df.empty and st.session_state.get("sim_loaded", False):
        st.success(f"✅ Загружено записей: {len(df):,}")

        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("Всего сервисов", len(df))
        with m2:
            st.metric(
                "С ICCID",
                int(df["ICCID"].notna().sum()) if "ICCID" in df.columns else 0,
            )
        with m3:
            st.metric(
                "Уникальных клиентов",
                int(df["CUSTOMER_NAME"].nunique()) if "CUSTOMER_NAME" in df.columns else 0,
            )

        st.markdown("---")
        st.subheader("📋 Данные")
        df_display = _to_display(df)
        st.dataframe(
            df_display,
            use_container_width=True,
            hide_index=True,
            height=450,
        )

        st.markdown("---")
        st.subheader("💾 Экспорт")
        c1, c2 = st.columns(2)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        with c1:
            st.download_button(
                label="📥 Скачать CSV",
                data=export_to_csv(df_display),
                file_name=f"sim_services_{stamp}.csv",
                mime="text/csv",
                use_container_width=True,
                key="sim_download_csv",
            )
        with c2:
            st.download_button(
                label="📥 Скачать Excel",
                data=export_to_excel(df_display),
                file_name=f"sim_services_{stamp}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="sim_download_xlsx",
            )

    st.markdown("---")
    st.caption(
        "💡 TYPE_ID=9001 · атрибуты из DICT "
        "(iccid, imsi, pstn_number_rus, pstn_number, pstn_number_data)"
    )
