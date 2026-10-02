"""
Закладка: паспорта Iridium (BPM Account)
Клиенты с услугами Iridium; фильтры по семейству услуг.
"""
import streamlit as st
from datetime import datetime
from tabs.common import export_to_csv, export_to_excel


_DISPLAY_COLS = [
    "CUSTOMER_NAME",
    "LETTER_FIO",
    "MANAGER_FIO",
    "CODE_1C",
    "CUSTOMER_ID",
    "SUBJECT_TYPE",
    "SERVICE_TYPES",
    "EMAIL",
    "HAS_PASSPORT",
    "PASSPORT_ID",
    "ISSUED_BY",
    "ISSUED_DATE",
    "CRM_CONTACT",
    "CRM_JOB_TITLE",
    "PERSON_FIO",
    "DIRECTOR_FIO",
    "PHONE",
    "CRM_NAME",
]

_SCOPE_OPTIONS = {
    "Все Iridium": "all",
    "Только SBD (9002/9014)": "sbd",
    "Только Open Port (9000, STATUS < 0)": "open_port",
    "Только Voice (9001, STATUS > 0)": "voice",
}


def show_tab(get_connection, get_passport_gaps_report):
    st.header("🪪 Паспорта Iridium")
    st.markdown(
        "Клиенты с услугами Iridium. Паспорт и email — из BPM `Account` "
        "(ODBC `CRMDB`, поле **Web**). "
        "**LETTER_FIO** — ФИО для обращения в письме (клиент-ФЛ или директор/PrimaryContact). "
        "**MANAGER_FIO** — ответственный менеджер СТЭККОМ (`Account.OwnerId` → `Contact` "
        "с email `@steccom.ru`)."
    )

    with st.expander("ℹ️ О отчёте", expanded=False):
        st.markdown(
            """
        **Биллинг (Oracle BM7):** выборка по семейству услуг:

        - **Все Iridium** — 9000 / 9001 / 9002 / 9005 / 9008 / 9013 / 9014;
        - **Только SBD** — 9002, 9014;
        - **Только Open Port** — 9000 и `STATUS < 0`;
        - **Только Voice** — 9001 и `STATUS > 0`.

        **ФИО для письма (`LETTER_FIO`):**
        - **ФЛ:** название контрагента (= ФИО клиента);
        - **ЮЛ:** контакты CRM (`Contact` по `AccountId`) — приоритет «Генеральный директор»,
          затем любой «директор» (более новый по дате создания контакта).

        **Менеджер СТЭККОМ (`MANAGER_FIO`):**
        - `Account.OwnerId` → `Contact.Name`, если `Contact.Email` в домене `@steccom.ru`.
        """
        )

    st.markdown("---")
    scope_label = st.radio(
        "Тип услуги",
        list(_SCOPE_OPTIONS.keys()),
        horizontal=True,
        key="pp_service_scope",
    )
    service_scope = _SCOPE_OPTIONS[scope_label]

    c1, c2 = st.columns(2)
    with c1:
        customer_name_filter = st.text_input("Клиент", key="pp_customer_name")
        code_1c_filter = st.text_input("Код 1С", key="pp_code_1c")
    with c2:
        only_missing = st.checkbox(
            "Только без паспорта",
            value=True,
            key="pp_only_missing",
        )
        only_persons = st.checkbox(
            "Только физлица (ФЛ)",
            value=False,
            key="pp_only_persons",
            help="В BPM SubjectType = физлицо",
        )
        only_with_email = st.checkbox(
            "Только с email (Web)",
            value=False,
            key="pp_only_email",
        )
        # STATUS>0 имеет смысл для «Все» и «SBD»; Open Port/Voice задают STATUS сами
        show_active = service_scope in ("all", "sbd")
        only_active_sim = True
        if show_active:
            only_active_sim = st.checkbox(
                "Только с активной услугой (STATUS > 0)",
                value=True,
                key="pp_active_sim",
                help="Для выбранного семейства услуг",
            )
        exclude_steccom = st.checkbox(
            "Без СТЭККОМ (customer_id=521)",
            value=True,
            key="pp_excl_521",
        )

    col_opts1, col_opts2 = st.columns([3, 1])
    with col_opts1:
        st.markdown("**Настройте фильтры и нажмите «Загрузить»:**")
    with col_opts2:
        load_report = st.button(
            "📊 Загрузить",
            type="primary",
            use_container_width=True,
            key="pp_load",
        )

    filter_key = "|".join(
        [
            service_scope,
            customer_name_filter or "",
            code_1c_filter or "",
            str(bool(only_missing)),
            str(bool(only_persons)),
            str(bool(only_with_email)),
            str(bool(only_active_sim)),
            str(bool(exclude_steccom)),
        ]
    )

    if load_report:
        with st.spinner("Биллинг + CRM Account..."):
            df = get_passport_gaps_report(
                get_connection,
                only_missing_passport=only_missing,
                only_persons=only_persons,
                only_with_email=only_with_email,
                only_active_sim=only_active_sim,
                exclude_steccom=exclude_steccom,
                service_scope=service_scope,
                customer_name_filter=customer_name_filter or None,
                code_1c_filter=code_1c_filter or None,
            )
        st.session_state.pp_filter_key = filter_key
        st.session_state.pp_df = df
        st.session_state.pp_loaded = df is not None
    else:
        saved = st.session_state.get("pp_filter_key")
        if (
            st.session_state.get("pp_loaded", False)
            and saved is not None
            and saved == filter_key
        ):
            df = st.session_state.get("pp_df")
        else:
            df = None
            if saved is not None and saved != filter_key:
                st.session_state.pp_loaded = False
                st.session_state.pp_df = None
            if not st.session_state.get("pp_loaded", False):
                st.info("ℹ️ Нажмите «Загрузить» для выборки клиентов без паспортов")

    if df is None and st.session_state.get("pp_loaded") and load_report:
        st.error("❌ Ошибка загрузки. Проверьте Oracle и ODBC CRMDB.")
    elif df is not None and df.empty and st.session_state.get("pp_loaded", False):
        st.info("📭 Нет строк по текущим фильтрам")
    elif df is not None and not df.empty and st.session_state.get("pp_loaded", False):
        st.success(f"✅ Загружено записей: {len(df):,}")
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.metric("Клиентов", len(df))
        with m2:
            no_pass = int((df["HAS_PASSPORT"] == "N").sum()) if "HAS_PASSPORT" in df.columns else 0
            st.metric("Без паспорта", no_pass)
        with m3:
            with_email = int(df["EMAIL"].notna().sum()) if "EMAIL" in df.columns else 0
            st.metric("С email", with_email)
        with m4:
            no_crm = int((df["SUBJECT_TYPE"] == "нет в CRM").sum()) if "SUBJECT_TYPE" in df.columns else 0
            st.metric("Нет в CRM", no_crm)

        st.markdown("---")
        st.subheader("📋 Данные")
        cols = [c for c in _DISPLAY_COLS if c in df.columns]
        df_display = df[cols].copy() if cols else df.copy()
        st.dataframe(df_display, use_container_width=True, hide_index=True, height=450)

        st.markdown("---")
        st.subheader("💾 Экспорт (для рассылки)")
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        e1, e2 = st.columns(2)
        with e1:
            st.download_button(
                "📥 CSV",
                data=export_to_csv(df_display),
                file_name=f"passport_gaps_{stamp}.csv",
                mime="text/csv",
                use_container_width=True,
                key="pp_csv",
            )
        with e2:
            st.download_button(
                "📥 Excel",
                data=export_to_excel(df_display),
                file_name=f"passport_gaps_{stamp}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="pp_xlsx",
            )

    st.markdown("---")
    st.caption(
        "💡 BM7 Iridium · BPM Account (Code1C) · email = Web · менеджер = Owner @steccom.ru"
    )
