#!/usr/bin/env python3
"""
Streamlit отчет по превышению трафика Iridium M2M
Версия для Oracle Database (backup)
"""

import os
# Исправление проблемы с protobuf - ДОЛЖНО БЫТЬ ПЕРВЫМ, до любых импортов
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

import streamlit as st
import pandas as pd
import cx_Oracle
from datetime import datetime
import io
import warnings
from utils.auth_db import (
    init_db, authenticate_user, create_user, list_users,
    delete_user, is_superuser, update_user_permissions,
    get_user_permissions, AVAILABLE_TABS,
)
# Важно: не делать dict(AVAILABLE_TABS) — это копия. sanitize_allowed_tab_keys и
# get_user_permissions читают тот же объект в auth_db; если дописать bills только
# в копию, ключ из users.db отбрасывается при старом auth_db без bills, и вкладка
# не появляется в интерфейсе.
for _tab_key, _tab_label in (
    ("bills", "📄 Рассылка счетов"),
    ("campaigns", "📧 Кампании"),
):
    if _tab_key not in AVAILABLE_TABS:
        AVAILABLE_TABS[_tab_key] = _tab_label

# Импорт модулей закладок
from tabs.tab_report import show_tab as show_report_tab
from tabs.tab_revenue import show_tab as show_revenue_tab
from tabs.tab_lbs import show_tab as show_lbs_tab
from tabs.tab_analytics import show_tab as show_analytics_tab
from tabs.tab_loader import show_tab as show_loader_tab
from tabs.tab_bills import show_tab as show_bills_tab
from tabs.tab_campaigns import show_tab as show_campaigns_tab

# Подавляем предупреждение pandas о cx_Oracle (работает корректно)
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')

from utils.db_connection import load_config_env, get_db_connection as get_connection
from utils.queries import (
    count_file_records, get_records_in_db, get_main_report,
    get_current_period, get_periods, get_plans,
    get_revenue_periods, get_revenue_report,
    get_lbs_services_report,
    get_analytics_duplicates, get_analytics_invoice_period_report,
    remove_analytics_duplicates
)

# Конфигурация базы данных
load_config_env()
ORACLE_USER = os.getenv('ORACLE_USER')
ORACLE_PASSWORD = os.getenv('ORACLE_PASSWORD')
ORACLE_HOST = os.getenv('ORACLE_HOST')
ORACLE_PORT = int(os.getenv('ORACLE_PORT', '1521'))
ORACLE_SERVICE = os.getenv('ORACLE_SERVICE') or os.getenv('ORACLE_SID')

# Вспомогательные функции для экспорта
from tabs.common import export_to_csv, export_to_excel
















# Инициализация базы данных пользователей
init_db()

def show_login_page():
    """Отображение страницы входа"""
    st.title("🔐 Система отчетов по Iridium M2M")
    st.markdown("---")
    
    st.info("💡 Для создания учетной записи обратитесь к администратору или используйте скрипт `create_user.py`")
    
    st.subheader("Вход")
    with st.form("login_form"):
        login_username = st.text_input("Имя пользователя", key="login_username")
        login_password = st.text_input("Пароль", type="password", key="login_password")
        login_submitted = st.form_submit_button("Войти", use_container_width=True)
        
        if login_submitted:
            if not login_username or not login_password:
                st.error("Заполните все поля")
            else:
                success, username, is_super, allowed_tabs = authenticate_user(login_username, login_password)
                if success:
                    st.session_state.authenticated = True
                    st.session_state.username = username
                    st.session_state.is_superuser = is_super
                    st.session_state.allowed_tabs = allowed_tabs
                    st.success(f"Добро пожаловать, {username}! 👋")
                    st.rerun()
                else:
                    st.error("Неверное имя пользователя или пароль")

def show_user_management():
    """Отображение управления пользователями (только для суперпользователей)"""
    if not st.session_state.get('is_superuser', False):
        return
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("👥 Управление пользователями")
    
    # Создание нового пользователя
    with st.sidebar.expander("➕ Создать пользователя"):
        with st.form("create_user_form"):
            new_username = st.text_input("Имя пользователя")
            new_password = st.text_input("Пароль", type="password")
            new_is_super = st.checkbox("Суперпользователь")
            
            # Выбор вкладок для нового пользователя (только если не суперпользователь)
            if not new_is_super:
                st.markdown("**Доступные вкладки (по умолчанию все включены):**")
                selected_tabs_new = []
                for tab_key, tab_name in AVAILABLE_TABS.items():
                    if st.checkbox(tab_name, value=True, key=f"new_user_tab_{tab_key}"):
                        selected_tabs_new.append(tab_key)
            else:
                selected_tabs_new = None  # Суперпользователь получает все вкладки автоматически
            
            create_submitted = st.form_submit_button("Создать")
            
            if create_submitted:
                success, message = create_user(
                    new_username, 
                    new_password, 
                    is_superuser=new_is_super,
                    allowed_tabs=selected_tabs_new if not new_is_super else None,
                    created_by=st.session_state.username
                )
                if success:
                    st.sidebar.success(message)
                    st.rerun()
                else:
                    st.sidebar.error(message)
    
    # Список пользователей
    with st.sidebar.expander("📋 Список пользователей"):
        users = list_users()
        if users:
            for user in users:
                superuser_mark = " 👑" if user['is_superuser'] else ""
                st.write(f"**{user['username']}**{superuser_mark}")
                if user['last_login']:
                    st.caption(f"Последний вход: {user['last_login'][:10]}")
                
                # Управление правами доступа (только для обычных пользователей)
                if not user['is_superuser']:
                    # Используем кнопку для открытия модального окна управления правами
                    if st.button(f"🔐 Управление правами", key=f"manage_perms_{user['username']}"):
                        st.session_state[f"show_perms_{user['username']}"] = not st.session_state.get(f"show_perms_{user['username']}", False)
                    
                    # Показываем интерфейс управления правами, если кнопка нажата
                    if st.session_state.get(f"show_perms_{user['username']}", False):
                        # Получаем текущие права пользователя
                        success_perm, current_tabs = get_user_permissions(user['username'])
                        if success_perm:
                            # Если у пользователя нет прав, даем все по умолчанию
                            if not current_tabs:
                                current_tabs = list(AVAILABLE_TABS.keys())
                            
                            # Чекбоксы для каждой вкладки (в т.ч. «Счета» и «Кампании»)
                            selected_tabs = []
                            st.markdown("**Доступные вкладки:**")
                            for tab_key, tab_name in AVAILABLE_TABS.items():
                                checked = tab_key in current_tabs
                                if st.checkbox(tab_name, value=checked, key=f"tab_{user['username']}_{tab_key}"):
                                    selected_tabs.append(tab_key)
                            
                            # Кнопка сохранения прав
                            col1, col2 = st.columns(2)
                            with col1:
                                if st.button("💾 Сохранить", key=f"save_perms_{user['username']}"):
                                    success, message = update_user_permissions(user['username'], selected_tabs)
                                    if success:
                                        st.session_state[f"show_perms_{user['username']}"] = False
                                        st.success(message)
                                        st.rerun()
                                    else:
                                        st.error(message)
                            with col2:
                                if st.button("❌ Отмена", key=f"cancel_perms_{user['username']}"):
                                    st.session_state[f"show_perms_{user['username']}"] = False
                                    st.rerun()
                        else:
                            st.error("Не удалось загрузить права пользователя")
                
                # Кнопка удаления (кроме текущего пользователя и суперпользователей)
                if user['username'] != st.session_state.username and not user['is_superuser']:
                    if st.button("🗑️ Удалить", key=f"delete_{user['username']}"):
                        success, message = delete_user(user['username'])
                        if success:
                            st.sidebar.success(message)
                            st.rerun()
                        else:
                            st.sidebar.error(message)
                st.markdown("---")
        else:
            st.write("Пользователи не найдены")

def main():
    """Основная функция приложения"""
    
    # Инициализация session state для авторизации
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    if 'username' not in st.session_state:
        st.session_state.username = None
    if 'is_superuser' not in st.session_state:
        st.session_state.is_superuser = False
    if 'allowed_tabs' not in st.session_state:
        st.session_state.allowed_tabs = []
    
    # Если не авторизован, показываем страницу входа
    if not st.session_state.authenticated:
        st.set_page_config(
            page_title="Iridium M2M - Авторизация",
            page_icon="🔐",
            layout="centered"
        )
        show_login_page()
        return
    
    # Основное приложение для авторизованных пользователей
    st.set_page_config(
        page_title="Iridium M2M KB Assistant",
        page_icon="📊",
        layout="wide"
    )
    
    # Боковая панель с информацией о пользователе
    st.sidebar.header("👤 Пользователь")
    st.sidebar.write(f"**{st.session_state.username}**")
    if st.session_state.is_superuser:
        st.sidebar.write("👑 Суперпользователь")
    
    # Управление пользователями для суперпользователей
    show_user_management()
    
    # Кнопка выхода
    if st.sidebar.button("🚪 Выйти"):
        st.session_state.authenticated = False
        st.session_state.username = None
        st.session_state.is_superuser = False
        for _k in ("main_nav_tab_key", "main_nav_sidebar_radio"):
            st.session_state.pop(_k, None)
        st.rerun()
    
    st.sidebar.markdown("---")
    
    # Проверка загрузки конфигурации
    if not ORACLE_PASSWORD:
        st.error("⚠️ **Конфигурация не загружена!**")
        st.stop()

    # Разрешённые вкладки (до фильтров — нужно для навигации в сайдбаре)
    if st.session_state.get("is_superuser", False):
        allowed_tabs_keys = list(AVAILABLE_TABS.keys())
    else:
        un = st.session_state.get("username")
        ok, tabs = get_user_permissions(un) if un else (False, [])
        allowed_tabs_keys = tabs if ok else []
        st.session_state.allowed_tabs = allowed_tabs_keys

    tab_configs = [
        (key, name)
        for key, name in AVAILABLE_TABS.items()
        if key in allowed_tabs_keys
    ]
    if not tab_configs:
        st.error("❌ У вас нет доступа ни к одной вкладке.")
        st.stop()

    _nav_keys = [k for k, _ in tab_configs]
    _nav_labels = [n for _, n in tab_configs]
    if (
        "main_nav_tab_key" not in st.session_state
        or st.session_state.main_nav_tab_key not in _nav_keys
    ):
        st.session_state.main_nav_tab_key = _nav_keys[0]

    if (
        "main_nav_sidebar_radio" not in st.session_state
        or st.session_state.main_nav_sidebar_radio not in _nav_labels
    ):
        st.session_state.main_nav_sidebar_radio = _nav_labels[
            _nav_keys.index(st.session_state.main_nav_tab_key)
        ]

    st.sidebar.markdown("### 📑 Раздел")
    st.sidebar.caption("Выбор раздела — здесь, без горизонтальной полосы вкладок.")
    _picked = st.sidebar.radio(
        "Раздел приложения",
        options=_nav_labels,
        key="main_nav_sidebar_radio",
    )
    st.session_state.main_nav_tab_key = _nav_keys[_nav_labels.index(_picked)]

    # Заголовок
    st.title("📊 Iridium M2M KB Assistant")
    st.markdown("---")

    tab_key = st.session_state.main_nav_tab_key
    if tab_key == "report":
        show_report_tab(get_connection, get_main_report, get_periods, get_plans)
    elif tab_key == "revenue":
        show_revenue_tab(get_connection, get_revenue_report, get_periods, get_plans)
    elif tab_key == "lbs":
        show_lbs_tab(get_connection, get_lbs_services_report, get_periods)
    elif tab_key == "analytics":
        show_analytics_tab(
            get_connection,
            get_analytics_invoice_period_report,
            get_analytics_duplicates,
            get_periods,
            get_plans,
            remove_analytics_duplicates,
        )
    elif tab_key == "loader":
        show_loader_tab(get_connection, count_file_records, get_records_in_db)
    elif tab_key == "bills":
        show_bills_tab()
    elif tab_key == "campaigns":
        show_campaigns_tab()
    elif tab_key == "assistant":
        try:
            from kb_billing.rag.streamlit_assistant import show_assistant_tab

            show_assistant_tab()
        except Exception as e:
            st.error(f"Ошибка ассистента: {e}")
    elif tab_key == "kb_expansion":
        try:
            from kb_billing.rag.streamlit_kb_expansion import show_kb_expansion_tab

            show_kb_expansion_tab()
        except Exception as e:
            st.error(f"Ошибка расширения KB: {e}")
    elif tab_key == "confluence_librarian":
        try:
            from kb_billing.rag.streamlit_confluence_librarian import (
                show_confluence_librarian_tab,
            )

            show_confluence_librarian_tab()
        except Exception as e:
            st.error(f"Ошибка спутникового библиотекаря: {e}")
    else:
        st.error(f"Неизвестный раздел: {tab_key!r}. Обновите страницу или обратитесь к администратору.")

if __name__ == "__main__":
    main()

