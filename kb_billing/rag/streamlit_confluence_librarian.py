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

try:
    from kb_billing.rag.confluence_kb_generator import ConfluenceKBGenerator
    HAS_CONFLUENCE_GENERATOR = True
except ImportError as e:
    HAS_CONFLUENCE_GENERATOR = False
    ConfluenceKBGenerator = None


def _get_client(url: str = "", token: str = "") -> ConfluenceClient:
    url = url or os.getenv("CONFLUENCE_URL", "")
    token = token or os.getenv("CONFLUENCE_TOKEN", "")
    return ConfluenceClient(base_url=url or None, token=token or None)


def show_confluence_librarian_tab():
    """Закладка «Спутниковый библиотекарь» — интеграция Confluence с KB."""
    st.header("🛰️ Спутниковый библиотекарь — Confluence и KB")
    if not HAS_CONFLUENCE_GENERATOR:
        st.error(
            "Модуль `confluence_kb_generator` не найден. Синхронизируйте папку `kb_billing/rag/` на сервер "
            "(в т.ч. файл `confluence_kb_generator.py`) и установите зависимости: `pip install beautifulsoup4 requests`."
        )
        st.info("Проверка подключения к Confluence ниже должна работать.")
    st.markdown("""
    **Синхронизация документации и схем из Confluence в единую базу знаний:**
    - 🔗 Проверка подключения к Confluence (docs.steccom.ru)
    - 📂 Список пространств и выбор пространства для выгрузки
    - 📥 Синхронизация страниц в формат KB (сохранение в `confluence_docs/`)
    - 🔄 Обновление векторной KB (перезагрузка в Qdrant)
    """)
    st.markdown("---")

    # Какие документы Confluence уже в KB (confluence_docs/*.json)
    confluence_docs_dir = project_root / "kb_billing" / "confluence_docs"
    outdated_set = set()
    if confluence_docs_dir.exists():
        outdated_file = confluence_docs_dir / "outdated.txt"
        if outdated_file.exists():
            try:
                with open(outdated_file, "r", encoding="utf-8") as f:
                    outdated_set = {line.strip() for line in f if line.strip()}
            except Exception:
                pass
    if confluence_docs_dir.exists():
        import json as _json
        all_entries = []
        for json_file in sorted(confluence_docs_dir.glob("*.json")):
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    docs = _json.load(f)
            except Exception:
                continue
            if not isinstance(docs, list):
                docs = [docs]
            for d in docs:
                src = d.get("source") or {}
                pid = src.get("page_id", "") or ""
                all_entries.append({
                    "Файл": json_file.name,
                    "Заголовок": d.get("title", "—"),
                    "Ссылка": src.get("url", ""),
                    "page_id": pid,
                    "outdated": pid in outdated_set,
                })
        if all_entries:
            st.subheader("📋 Документы Confluence в KB")
            st.caption(
                "Список документов из `kb_billing/confluence_docs/`. "
                "Можно обновить документ из Confluence или пометить устаревшим (тогда он не попадёт в поиск до «Перезагрузить KB»)."
            )
            total = len(all_entries)
            in_use = sum(1 for e in all_entries if not e["outdated"])
            st.metric("Документов в KB (Confluence)", total)
            st.caption(f"Актуальных (попадут в поиск): {in_use}, устаревших: {total - in_use}")
            with st.expander("Показать список документов: обновить / пометить устаревшим"):
                for i, row in enumerate(all_entries, 1):
                    label = f"**{i}. {row['Заголовок']}**"
                    if row["outdated"]:
                        label += " — ⚠️ устаревший"
                    st.markdown(label)
                    if row["Ссылка"]:
                        st.caption(f"📎 [{row['Ссылка']}]({row['Ссылка']})")
                    else:
                        st.caption(f"Файл: {row['Файл']}, page_id: `{row['page_id']}`")
                    if not row["page_id"]:
                        continue
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        if st.button("🔄 Обновить из Confluence", key=f"upd_{i}_{row['page_id']}"):
                            if HAS_CONFLUENCE_GENERATOR:
                                with st.spinner("Обновление..."):
                                    try:
                                        gen = ConfluenceKBGenerator(client=_get_client())
                                        n = gen.update_docs_by_page_ids([row["page_id"]])
                                        st.success(f"Обновлено документов: {n}. Нажмите «Перезагрузить KB в Qdrant» для применения.")
                                    except Exception as e:
                                        st.error(str(e))
                                st.rerun()
                            else:
                                st.error("Модуль синхронизации недоступен.")
                    with col2:
                        if not row["outdated"] and st.button("📌 Пометить устаревшим", key=f"out_{i}_{row['page_id']}"):
                            try:
                                gen = ConfluenceKBGenerator(client=_get_client()) if HAS_CONFLUENCE_GENERATOR else None
                                if gen:
                                    gen.add_to_outdated(row["page_id"])
                                    st.success("Помечен устаревшим. Перезагрузите KB в Qdrant, чтобы исключить из поиска.")
                                else:
                                    path = confluence_docs_dir / "outdated.txt"
                                    with open(path, "a", encoding="utf-8") as f:
                                        f.write(row["page_id"] + "\n")
                                    st.success("Помечен устаревшим.")
                                st.rerun()
                            except Exception as e:
                                st.error(str(e))
                    with col3:
                        if row["outdated"] and st.button("✅ Вернуть в актуальные", key=f"rev_{i}_{row['page_id']}"):
                            try:
                                if HAS_CONFLUENCE_GENERATOR:
                                    gen = ConfluenceKBGenerator(client=_get_client())
                                    gen.remove_from_outdated(row["page_id"])
                                else:
                                    path = confluence_docs_dir / "outdated.txt"
                                    if path.exists():
                                        lines = [l for l in open(path, encoding="utf-8") if l.strip() != row["page_id"]]
                                        with open(path, "w", encoding="utf-8") as f:
                                            f.writelines(lines)
                                    st.success("Снята пометка. Перезагрузите KB в Qdrant.")
                                st.rerun()
                            except Exception as e:
                                st.error(str(e))
            st.markdown("---")
    else:
        st.caption("Папка `kb_billing/confluence_docs/` пока пуста — после первой синхронизации здесь появится список документов.")
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
    st.caption(
        "Ключ пространства — это не URL. Для пользователя из ссылки вида "
        "«.../username=n.shiriaev» укажите ключ личного пространства: **~n.shiriaev** (тильда + логин)."
    )
    space_key = st.text_input(
        "Ключ пространства (Space key)",
        value="",
        placeholder="например DEMO, SPC или ~n.shiriaev",
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
        if not HAS_CONFLUENCE_GENERATOR:
            st.error("Модуль синхронизации недоступен. Разверните `kb_billing/rag/confluence_kb_generator.py` и установите beautifulsoup4.")
        elif not space_key.strip():
            st.error("Введите ключ пространства.")
        else:
            with st.spinner("Синхронизация..."):
                try:
                    gen = ConfluenceKBGenerator(client=client)
                    docs = gen.sync_space(space_key.strip(), limit=limit)
                    out_dir = gen.get_synced_docs_path()
                    st.success(f"Сохранено документов: **{len(docs)}** в `{out_dir}`")
                    if docs:
                        with st.expander("Первые заголовки"):
                            for d in docs[:15]:
                                st.text(f"  • {d.get('title', '')}")
                except Exception as e:
                    err = str(e)
                    st.error(err)
                    if "404" in err and "~" in (space_key or ""):
                        st.info(
                            "Для личных пространств этот Confluence может не отдавать страницы по spaceKey. "
                            "Используйте блок ниже «Конкретные страницы по URL или ID»: вставьте ссылки на нужные страницы."
                        )
                    import traceback
                    st.code(traceback.format_exc())

    st.markdown("---")
    st.subheader("Конкретные страницы по URL или ID")
    st.caption("Укажите ссылки на страницы Confluence или только их ID (по одному на строку). Пример URL: .../pages/viewpage.action?pageId=123456")
    page_urls_or_ids = st.text_area(
        "URL страниц или Page ID (каждый с новой строки)",
        value="",
        height=120,
        placeholder="https://docs.steccom.ru/pages/viewpage.action?pageId=123456\n123457\n...",
        key="confluence_page_urls",
    )
    if st.button("📥 Синхронизировать выбранные страницы в KB", key="confluence_sync_pages_btn"):
        if not HAS_CONFLUENCE_GENERATOR:
            st.error("Модуль синхронизации недоступен.")
        elif not page_urls_or_ids.strip():
            st.error("Введите хотя бы один URL или ID страницы.")
        else:
            lines = [s.strip() for s in page_urls_or_ids.strip().splitlines() if s.strip()]
            with st.spinner("Синхронизация страниц..."):
                try:
                    gen = ConfluenceKBGenerator(client=client)
                    docs = gen.sync_page_ids(lines, output_suffix="custom_pages")
                    st.success(f"Сохранено документов: **{len(docs)}** в `confluence_custom_pages.json`")
                    if docs:
                        with st.expander("Заголовки"):
                            for d in docs[:20]:
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

    st.markdown("---")
    st.subheader("Проверка релевантности")
    st.markdown(
        "Задайте вопрос по тематике загруженных документов Confluence. Поиск вернёт наиболее похожие фрагменты "
        "и оценку сходства (score). Оцените, насколько найденное релевантно вопросу."
    )
    relevance_question = st.text_area(
        "Вопрос для проверки поиска",
        value="",
        height=80,
        placeholder="Например: что такое Z10MK4? инструкция Kingsat P8, мобильные МЧС...",
        key="relevance_question",
    )
    relevance_limit = st.slider("Сколько документов показать", min_value=1, max_value=15, value=5, key="relevance_limit")
    if st.button("🔍 Найти в документах Confluence", key="relevance_search_btn"):
        if not relevance_question.strip():
            st.warning("Введите вопрос.")
        else:
            with st.spinner("Поиск по векторной KB..."):
                try:
                    from kb_billing.rag.rag_assistant import RAGAssistant
                    assistant = RAGAssistant()
                    docs = assistant.search_semantic(
                        relevance_question.strip(),
                        content_type="confluence_doc",
                        limit=relevance_limit,
                    )
                    if not docs:
                        st.info("По запросу ничего не найдено. Убедитесь, что документы загружены в Qdrant (кнопка «Перезагрузить KB» выше).")
                    else:
                        st.success(f"Найдено документов: **{len(docs)}**")
                        for i, d in enumerate(docs, 1):
                            score = d.get("similarity", 0)
                            title = d.get("title", "—")
                            url = d.get("source_url", "")
                            content = (d.get("content") or "")[:400]
                            with st.expander(f"**{i}. {title}** — сходство: {score:.1%}"):
                                if url:
                                    st.markdown(f"📎 [Открыть в Confluence]({url})")
                                st.caption("Фрагмент текста:")
                                st.text(content + ("..." if len(d.get("content") or "") > 400 else ""))
                                st.caption("Оценка релевантности: смотрите на score (сходство) и по фрагменту решите, отвечает ли документ на вопрос.")
                except Exception as e:
                    st.error(str(e))
                    import traceback
                    st.code(traceback.format_exc())
