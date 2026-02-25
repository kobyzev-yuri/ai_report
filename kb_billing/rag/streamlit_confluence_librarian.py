#!/usr/bin/env python3
"""
Интерфейс спутникового библиотекаря: интеграция Confluence с единой KB.
Проверка подключения, выбор пространства, синхронизация страниц в confluence_docs, перезагрузка KB.
"""
import os
import sys
from pathlib import Path

# Корень проекта
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
os.environ.setdefault("PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION", "python")

# Подгрузка config.env (CONFLUENCE_URL, CONFLUENCE_TOKEN)
_config_env = project_root / "config.env"
if _config_env.exists():
    with open(_config_env, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip("'\""))

import streamlit as st

from kb_billing.rag.confluence_client import ConfluenceClient
from kb_billing.rag.confluence_kb_generator import ConfluenceKBGenerator


def _get_client(url: str = "", token: str = "") -> ConfluenceClient:
    url = url or os.getenv("CONFLUENCE_URL", "")
    token = token or os.getenv("CONFLUENCE_TOKEN", "")
    return ConfluenceClient(base_url=url or None, token=token or None)


def show_confluence_librarian_tab():
    """Закладка «Спутниковый библиотекарь» — интеграция Confluence с KB."""
    st.header("🛰️ Спутниковый библиотекарь — Confluence и KB")
    st.markdown("""
    **Синхронизация документации и схем из Confluence в единую базу знаний:**
    - 🔗 Проверка подключения к Confluence (docs.steccom.ru)
    - 📂 Список пространств и выбор пространства для выгрузки
    - 📥 Синхронизация страниц в формат KB (сохранение в `confluence_docs/`)
    - 🔄 Обновление векторной KB (перезагрузка в Qdrant)
    """)
    st.markdown("---")

    # Настройки (можно переопределить из config.env)
    confluence_url = st.text_input(
        "URL Confluence",
        value=os.getenv("CONFLUENCE_URL", "https://docs.steccom.ru"),
        key="confluence_url_lib",
        help="Базовый URL Confluence (например https://docs.steccom.ru)",
    )
    confluence_token = st.text_input(
        "Токен (Personal Access Token)",
        value=os.getenv("CONFLUENCE_TOKEN", ""),
        type="password",
        key="confluence_token_lib",
        help="Задаётся в config.env или здесь. Не сохраняйте в коде.",
    )
    if not confluence_token:
        st.caption("💡 Задайте CONFLUENCE_TOKEN в config.env или введите выше.")

    client = _get_client(confluence_url, confluence_token)

    # Проверка подключения
    if st.button("🔌 Проверить подключение к Confluence", type="primary", key="confluence_check_btn"):
        with st.spinner("Проверка..."):
            ok, msg = client.check_connection()
        if ok:
            st.success(msg)
            try:
                spaces = client.get_spaces(limit=20)
                if spaces:
                    st.subheader("Пространства (первые 20)")
                    for s in spaces:
                        st.text(f"  • {s.get('key', '')} — {s.get('name', '')}")
            except Exception as e:
                st.warning(f"Список пространств: {e}")
        else:
            st.error(msg)

    st.markdown("---")
    st.subheader("Синхронизация пространства в KB")

    space_key = st.text_input(
        "Ключ пространства (Space key)",
        value="",
        placeholder="например DEMO или ~username",
        key="confluence_space_key",
    )
    limit_pages = st.number_input(
        "Макс. страниц за один запуск (0 = без ограничения)",
        min_value=0,
        value=50,
        key="confluence_limit",
    )
    limit = None if limit_pages == 0 else int(limit_pages)

    if st.button("📥 Синхронизировать пространство в KB", key="confluence_sync_btn"):
        if not space_key:
            st.error("Введите ключ пространства.")
        else:
            with st.spinner("Синхронизация..."):
                try:
                    gen = ConfluenceKBGenerator(client=client)
                    docs = gen.sync_space(space_key, limit=limit)
                    out_dir = gen.get_synced_docs_path()
                    st.success(f"Сохранено документов: **{len(docs)}** в `{out_dir}`")
                    if docs:
                        with st.expander("Первые заголовки"):
                            for d in docs[:15]:
                                st.text(f"  • {d.get('title', '')}")
                except Exception as e:
                    st.error(str(e))
                    import traceback
                    st.code(traceback.format_exc())

    st.markdown("---")
    st.subheader("Обновление векторной KB (Qdrant)")
    st.markdown("После синхронизации из Confluence нужно перезагрузить KB, чтобы документы попали в поиск.")
    if st.button("🔄 Перезагрузить KB в Qdrant (все источники)", key="confluence_reload_kb_btn"):
        with st.spinner("Загрузка в Qdrant (биллинг + Confluence)..."):
            try:
                from kb_billing.rag.kb_loader import KBLoader
                loader = KBLoader()
                loader.load_all(recreate=False)
                st.success("KB обновлена. Новые документы Confluence добавлены в коллекцию.")
            except Exception as e:
                st.error(str(e))
                import traceback
                st.code(traceback.format_exc())
