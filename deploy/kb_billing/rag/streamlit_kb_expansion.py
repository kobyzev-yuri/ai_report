#!/usr/bin/env python3
"""
Streamlit модуль для интерактивного расширения KB
"""
import os
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

import streamlit as st
import sys
from pathlib import Path
import json
from typing import Any, Dict, List

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from kb_billing.rag.kb_expansion_agent import KBExpansionAgent
from db_connection import get_db_connection as get_connection


@st.cache_resource
def init_expansion_agent():
    """Инициализация агента расширения KB (кэшируется)"""
    try:
        from kb_billing.rag.config_sql4a import SQL4AConfig
        
        qdrant_host = os.getenv("QDRANT_HOST", SQL4AConfig.QDRANT_HOST)
        qdrant_port = int(os.getenv("QDRANT_PORT", SQL4AConfig.QDRANT_PORT))
        
        return KBExpansionAgent(
            qdrant_host=qdrant_host,
            qdrant_port=qdrant_port
        )
    except Exception as e:
        st.error(f"Ошибка инициализации агента расширения KB: {e}")
        import traceback
        st.code(traceback.format_exc())
        return None


def show_kb_expansion_tab():
    """Отображение закладки для расширения KB (KB библиотекарь)"""
    
    st.header("📚 KB Библиотекарь - Расширение базы знаний SQL отчетами")
    st.markdown("""
    **Библиотекарь для систематизации SQL отчетов в KB:**
    - 🔍 Проверяет существующие примеры в KB
    - 📊 Импортирует готовые SQL отчеты
    - 📝 Систематизирует и категоризирует SQL запросы
    - 🎓 Переобучает KB с новыми примерами
    - 📋 Управляет коллекцией SQL отчетов
    """)
    
    st.markdown("---")
    
    # Инициализация агента
    agent = init_expansion_agent()
    if not agent:
        return
    
    # Статистика KB
    examples_count = agent.get_existing_examples_count()
    st.info(f"📊 В базе знаний сейчас: **{examples_count}** примеров")
    
    st.markdown("---")
    
    # Вкладки для разных способов добавления и редактирования KB
    tab_import, tab_generate, tab_docs, tab_code = st.tabs([
        "📥 Импорт SQL отчета",
        "🤖 Генерация из вопроса",
        "📑 Описание таблиц / VIEW",
        "💻 Кодовые примеры (Perl/Python/SQL)"
    ])
    
    # Инициализация session_state
    if "expansion_question" not in st.session_state:
        st.session_state.expansion_question = ""
    if "expansion_analysis" not in st.session_state:
        st.session_state.expansion_analysis = None
    if "collected_examples" not in st.session_state:
        st.session_state.collected_examples = []
    if "current_example" not in st.session_state:
        st.session_state.current_example = None
    
    # ========== ВКЛАДКА ИМПОРТА SQL ==========
    with tab_import:
        st.subheader("📥 Импорт готового SQL отчета в KB")
        st.markdown("""
        **Импортируйте готовые SQL запросы:**
        - Вставьте SQL запрос, который уже работает
        - Добавьте описание/вопрос для этого запроса
        - Систематизируйте по категориям
        """)
        
        with st.form("import_sql_form", clear_on_submit=False):
            imported_question = st.text_area(
                "Вопрос/описание для SQL отчета:",
                height=80,
                placeholder="Например: Список коммерческих клиентов с адресами и банковскими реквизитами за год",
                key="imported_question"
            )
            
            imported_sql = st.text_area(
                "SQL запрос:",
                height=300,
                placeholder="SELECT ...",
                key="imported_sql"
            )
            
            col1, col2 = st.columns(2)
            with col1:
                import_category = st.selectbox(
                    "Категория:",
                    ["Общие", "Клиенты", "Сервисы", "Поиск", "Доходы", "Расходы", "Трафик", "Превышение трафика", 
                     "Себестоимость", "Финансовые алерты", "Аналитика", "Отчеты", "Экспорт", 
                     "Fees", "Планы", "Тарифы", "Валюты", "Справочники", "CRM"],
                    key="import_category"
                )
            with col2:
                import_complexity = st.slider(
                    "Сложность (1-5):",
                    min_value=1,
                    max_value=5,
                    value=2,
                    key="import_complexity"
                )
            
            import_context = st.text_area(
                "Контекст/описание (опционально):",
                height=100,
                placeholder="Дополнительное описание запроса, особенности использования...",
                key="import_context"
            )
            
            import_business_entity = st.text_input(
                "Бизнес-сущность (опционально):",
                value="general",
                key="import_business_entity"
            )
            
            import_button = st.form_submit_button("💾 Импортировать в KB", type="primary", use_container_width=True)
            
            if import_button:
                if not imported_question or not imported_sql:
                    st.error("❌ Заполните вопрос и SQL запрос!")
                else:
                    # Проверяем на дубликаты
                    similar, is_duplicate = agent.check_existing_examples(imported_question)
                    
                    if is_duplicate:
                        st.warning("⚠️ **Похожий пример уже существует в KB!**")
                        st.markdown("**Найденный пример:**")
                        st.code(similar[0].get('question', ''), language=None)
                        st.code(similar[0].get('sql', ''), language="sql")
                        st.info("💡 Если это другой вариант, уточните вопрос или SQL.")
                    else:
                        # Форматируем пример
                        example = agent.format_example_for_kb(
                            question=imported_question,
                            sql=imported_sql,
                            context=import_context if import_context else None,
                            business_entity=import_business_entity if import_business_entity else None,
                            complexity=import_complexity,
                            category=import_category
                        )
                        
                        # Добавляем в файл
                        if agent.add_example_to_file(example):
                            st.success(f"✅ SQL отчет успешно импортирован в KB!")
                            
                            # Добавляем в собранные примеры
                            if "collected_examples" not in st.session_state:
                                st.session_state.collected_examples = []
                            st.session_state.collected_examples.append(example)
                            
                            # Обновляем статистику
                            new_count = agent.get_existing_examples_count()
                            st.info(f"📊 Теперь в KB: **{new_count}** примеров")
                            
                            # Очищаем форму
                            st.rerun()
                        else:
                            st.error("❌ Не удалось импортировать. Возможно, такой пример уже существует.")
        
        # Импорт из результатов выполнения (если есть)
        if "sql_result" in st.session_state or "financial_result" in st.session_state:
            st.markdown("---")
            st.subheader("📋 Импорт из результатов выполнения")
            
            result_keys = []
            if "sql_result" in st.session_state:
                result_keys.append(("sql_result", "Результат SQL запроса"))
            if "financial_result" in st.session_state:
                result_keys.append(("financial_result", "Результат финансового анализа"))
            
            for result_key, label in result_keys:
                result = st.session_state[result_key]
                if "sql" in result:
                    with st.expander(f"📊 {label}"):
                        st.code(result["sql"], language="sql")
                        
                        if st.button(f"📥 Импортировать этот SQL", key=f"import_{result_key}"):
                            # Предзаполняем форму импорта
                            st.session_state.imported_question = f"SQL запрос из {label}"
                            st.session_state.imported_sql = result["sql"]
                            st.rerun()
    
    # ========== ВКЛАДКА ГЕНЕРАЦИИ ИЗ ВОПРОСА ==========
    with tab_generate:
        st.subheader("🤖 Генерация SQL из вопроса")
        st.markdown("""
        **Создайте новый SQL отчет из вопроса:**
        - Введите вопрос на русском языке
        - Ассистент сгенерирует SQL запрос
        - Отредактируйте и сохраните в KB
        """)
        
        # Форма для ввода вопроса
        with st.form("expansion_form", clear_on_submit=False):
            question_input = st.text_area(
                "Введите вопрос клиента:",
                height=100,
                placeholder="Например: Покажи всех клиентов с адресами и банковскими реквизитами за год",
                value=st.session_state.expansion_question,
                key="expansion_question_input"
            )
            
            analyze_button = st.form_submit_button("🔍 Проанализировать вопрос", type="primary", use_container_width=True)
            
            if analyze_button:
                st.session_state.expansion_question = question_input
                st.session_state.expansion_analysis = None
                st.session_state.current_example = None
        
        question = st.session_state.expansion_question
        
        # Анализ вопроса
        if question and (st.session_state.expansion_analysis is None or st.session_state.expansion_question != question):
            with st.spinner("Анализ вопроса и поиск похожих примеров..."):
                api_key = os.getenv("OPENAI_API_KEY")
                api_base = os.getenv("OPENAI_BASE_URL") or os.getenv("OPENAI_API_BASE")
                
                analysis = agent.analyze_question_and_suggest_sql(
                    question=question,
                    api_key=api_key,
                    api_base=api_base
                )
                st.session_state.expansion_analysis = analysis
        
        # Отображение результатов анализа
        if st.session_state.expansion_analysis:
            analysis = st.session_state.expansion_analysis
            
            st.markdown("---")
            st.subheader("📋 Результаты анализа")
            
            # Проверка на дубликаты
            if analysis["is_duplicate"]:
                st.warning("⚠️ **Похожий пример уже существует в KB!**")
                st.markdown("**Найденный пример:**")
                similar = analysis["similar_examples"][0]
                st.code(similar.get('question', ''), language=None)
                st.code(similar.get('sql', ''), language="sql")
                st.info("💡 Этот вопрос уже покрыт существующим примером. Если нужен другой вариант, уточните вопрос.")
            else:
                # Показываем похожие примеры (если есть)
                if analysis["similar_examples"]:
                    st.markdown("**🔍 Похожие примеры в KB:**")
                    for i, similar in enumerate(analysis["similar_examples"][:3], 1):
                        with st.expander(f"Пример {i} (схожесть: {similar.get('score', 0):.2%})"):
                            st.markdown(f"**Вопрос:** {similar.get('question', '')}")
                            st.code(similar.get('sql', ''), language="sql")
            
            # Уточняющие вопросы
            if analysis["clarifications"]:
                st.markdown("**❓ Рекомендуется уточнить:**")
                for clarification in analysis["clarifications"]:
                    st.markdown(f"- {clarification}")
            
            # Предложенный SQL
            if analysis["suggested_sql"]:
                st.markdown("---")
                st.subheader("💡 Предложенный SQL запрос")
                st.code(analysis["suggested_sql"], language="sql")
                
                # Форма для редактирования и сохранения примера
                with st.form("save_example_form"):
                    st.markdown("**📝 Редактирование примера:**")
                    
                    # Редактируемые поля
                    edited_question = st.text_area(
                        "Вопрос (можно отредактировать):",
                        value=question,
                        height=80,
                        key="edited_question"
                    )
                    
                    edited_sql = st.text_area(
                        "SQL запрос (можно отредактировать):",
                        value=analysis["suggested_sql"],
                        height=200,
                        key="edited_sql"
                    )
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        category = st.selectbox(
                            "Категория:",
                            ["Общие", "Клиенты", "Сервисы", "Поиск", "Доходы", "Расходы", "Трафик", "Превышение трафика", 
                             "Себестоимость", "Финансовые алерты", "Аналитика", "Отчеты", "Экспорт", 
                             "Fees", "Планы", "Тарифы", "Валюты", "Справочники", "CRM"],
                            key="example_category"
                        )
                    with col2:
                        complexity = st.slider(
                            "Сложность (1-5):",
                            min_value=1,
                            max_value=5,
                            value=2,
                            key="example_complexity"
                        )
                    
                    context_input = st.text_area(
                        "Контекст/описание (опционально):",
                        value=analysis.get("suggested_context", ""),
                        height=100,
                        key="example_context"
                    )
                    
                    business_entity = st.text_input(
                        "Бизнес-сущность (опционально):",
                        value="general",
                        key="example_business_entity"
                    )
                    
                    save_button = st.form_submit_button("💾 Сохранить пример в KB", type="primary", use_container_width=True)
                    
                    if save_button:
                        # Форматируем пример
                        example = agent.format_example_for_kb(
                            question=edited_question,
                            sql=edited_sql,
                            context=context_input if context_input else None,
                            business_entity=business_entity if business_entity else None,
                            complexity=complexity,
                            category=category
                        )
                        
                        # Добавляем в файл
                        if agent.add_example_to_file(example):
                            st.success(f"✅ Пример сохранен в файл!")
                            st.session_state.collected_examples.append(example)
                            st.session_state.current_example = example
                            
                            # Обновляем статистику
                            new_count = agent.get_existing_examples_count()
                            st.info(f"📊 Теперь в KB: **{new_count}** примеров")
                        else:
                            st.error("❌ Не удалось сохранить пример. Возможно, такой пример уже существует.")
            else:
                st.info("💡 Для генерации SQL запроса установите OPENAI_API_KEY в config.env")

    # ========== ВКЛАДКА: ОПИСАНИЯ ТАБЛИЦ / VIEW ==========
    with tab_docs:
        kb_root = Path(__file__).parent.parent
        tables_dir = kb_root / "tables"
        views_dir = kb_root / "views"

        st.subheader("📑 Описание таблиц")
        if not tables_dir.exists():
            st.warning(f"Директория с таблицами не найдена: {tables_dir}")
        else:
            table_files = sorted(tables_dir.glob("*.json"))
            if not table_files:
                st.info("Файлы описаний таблиц (*.json) не найдены.")
            else:
                table_map: Dict[str, Path] = {f.stem: f for f in table_files}
                selected_table = st.selectbox(
                    "Выберите таблицу для редактирования описания:",
                    options=sorted(table_map.keys()),
                    key="kb_tables_select",
                )

                if selected_table:
                    table_path = table_map[selected_table]
                    with open(table_path, "r", encoding="utf-8") as f:
                        table_data: Dict[str, Any] = json.load(f)

                    st.markdown(f"**Файл:** `{table_path.name}`")

                    desc = st.text_area(
                        "Описание таблицы:",
                        value=table_data.get("description", ""),
                        height=140,
                        key="kb_table_description",
                    )

                    business_rules_text = "\n".join(table_data.get("business_rules", []))
                    business_rules_text = st.text_area(
                        "Business rules (по одному правилу в строке):",
                        value=business_rules_text,
                        height=160,
                        key="kb_table_business_rules",
                    )

                    usage_notes_text = "\n".join(table_data.get("usage_notes", []))
                    usage_notes_text = st.text_area(
                        "Usage notes (по одной заметке в строке):",
                        value=usage_notes_text,
                        height=160,
                        key="kb_table_usage_notes",
                    )

                    with st.expander("🔍 Технические детали (key_columns, relationships и т.п.)"):
                        st.json(
                            {
                                "key_columns": table_data.get("key_columns", {}),
                                "relationships": table_data.get("relationships", []),
                            }
                        )

                    if st.button("💾 Сохранить описание таблицы", type="primary", use_container_width=True, key="kb_save_table_doc"):
                        try:
                            table_data["description"] = desc.strip()
                            table_data["business_rules"] = [
                                line.strip()
                                for line in business_rules_text.splitlines()
                                if line.strip()
                            ]
                            table_data["usage_notes"] = [
                                line.strip()
                                for line in usage_notes_text.splitlines()
                                if line.strip()
                            ]

                            with open(table_path, "w", encoding="utf-8") as f:
                                json.dump(table_data, f, ensure_ascii=False, indent=2)

                            st.success("✅ Описание таблицы сохранено в JSON. KB будет использовать эти изменения при следующем обновлении (init_kb).")
                        except Exception as e:
                            st.error(f"❌ Ошибка при сохранении описания таблицы: {e}")

        st.markdown("---")
        st.subheader("📑 Описание VIEW")

        if not views_dir.exists():
            st.warning(f"Директория с VIEW не найдена: {views_dir}")
        else:
            view_files = sorted(views_dir.glob("*.json"))
            if not view_files:
                st.info("Файлы описаний VIEW (*.json) не найдены.")
            else:
                view_map: Dict[str, Path] = {f.stem: f for f in view_files}
                selected_view = st.selectbox(
                    "Выберите VIEW для редактирования описания:",
                    options=sorted(view_map.keys()),
                    key="kb_views_select",
                )

                if selected_view:
                    view_path = view_map[selected_view]
                    with open(view_path, "r", encoding="utf-8") as f:
                        view_data: Dict[str, Any] = json.load(f)

                    st.markdown(f"**Файл:** `{view_path.name}`")

                    v_desc = st.text_area(
                        "Описание VIEW:",
                        value=view_data.get("description", ""),
                        height=140,
                        key="kb_view_description",
                    )

                    v_usage_notes_text = "\n".join(view_data.get("usage_notes", []))
                    v_usage_notes_text = st.text_area(
                        "Usage notes (по одной заметке в строке):",
                        value=v_usage_notes_text,
                        height=160,
                        key="kb_view_usage_notes",
                    )

                    with st.expander("🔍 Технические детали VIEW (колонки и др.)"):
                        st.json(
                            {
                                "columns": view_data.get("columns", {}),
                                "source_tables": view_data.get("source_tables", []),
                            }
                        )

                    if st.button("💾 Сохранить описание VIEW", type="primary", use_container_width=True, key="kb_save_view_doc"):
                        try:
                            view_data["description"] = v_desc.strip()
                            view_data["usage_notes"] = [
                                line.strip()
                                for line in v_usage_notes_text.splitlines()
                                if line.strip()
                            ]

                            with open(view_path, "w", encoding="utf-8") as f:
                                json.dump(view_data, f, ensure_ascii=False, indent=2)

                            st.success("✅ Описание VIEW сохранено в JSON. KB будет использовать эти изменения при следующем обновлении (init_kb).")
                        except Exception as e:
                            st.error(f"❌ Ошибка при сохранении описания VIEW: {e}")

        st.markdown("---")
        st.subheader("🔎 Сканер схемы Oracle (DDL для ещё не описанных таблиц)")

        # Список уже описанных таблиц в KB
        existing_table_names = set()
        if tables_dir.exists():
            for f in tables_dir.glob("*.json"):
                existing_table_names.add(f.stem.upper())

        conn = None
        try:
            conn = get_connection()
        except Exception as e:
            st.error(f"❌ Ошибка подключения к Oracle: {e}")
            conn = None

        if not conn:
            st.info("Для работы сканера схемы требуется активное подключение к Oracle.")
        else:
            try:
                cur = conn.cursor()
                # Получаем список всех таблиц текущего пользователя
                cur.execute("SELECT table_name FROM user_tables ORDER BY table_name")
                all_tables = [r[0] for r in cur.fetchall()]

                # Фильтруем те, для которых ещё нет JSON-описания
                missing_tables = [t for t in all_tables if t.upper() not in existing_table_names]

                if not missing_tables:
                    st.success("✅ Все таблицы пользователя уже имеют JSON-описания в KB.")
                else:
                    st.markdown(f"Найдено таблиц без описаний в KB: **{len(missing_tables)}**")
                    selected_new_table = st.selectbox(
                        "Выберите таблицу для генерации базового JSON на основе DDL:",
                        options=missing_tables,
                        key="kb_missing_tables_select",
                    )

                    if selected_new_table:
                        st.markdown(f"**Таблица:** `{selected_new_table}`")

                        if st.button(
                            "📥 Загрузить DDL из Oracle и создать JSON в KB",
                            type="primary",
                            use_container_width=True,
                            key="kb_generate_table_json",
                        ):
                            try:
                                # Получаем DDL таблицы
                                cur.execute(
                                    "SELECT DBMS_METADATA.GET_DDL('TABLE', :tname) FROM dual",
                                    {"tname": selected_new_table},
                                )
                                ddl_row = cur.fetchone()
                                table_ddl = ""
                                if ddl_row and ddl_row[0] is not None:
                                    table_ddl = str(ddl_row[0])

                                # Получаем DDL индексов
                                cur_idx = conn.cursor()
                                cur_idx.execute(
                                    "SELECT index_name FROM user_indexes WHERE table_name = :tname",
                                    {"tname": selected_new_table},
                                )
                                indexes_ddl = []
                                for (idx_name,) in cur_idx.fetchall():
                                    try:
                                        cur_idx.execute(
                                            "SELECT DBMS_METADATA.GET_DDL('INDEX', :iname) FROM dual",
                                            {"iname": idx_name},
                                        )
                                        idx_row = cur_idx.fetchone()
                                        if idx_row and idx_row[0] is not None:
                                            indexes_ddl.append(str(idx_row[0]))
                                    except Exception:
                                        # Индекс может быть служебным или недоступным для GET_DDL - пропускаем
                                        continue
                                cur_idx.close()

                                target_json = tables_dir / f"{selected_new_table}.json"
                                if target_json.exists():
                                    st.warning(f"⚠️ Файл `{target_json.name}` уже существует, генерация пропущена.")
                                else:
                                    table_doc = {
                                        "table_name": selected_new_table,
                                        "schema": os.getenv("ORACLE_USER", "billing"),
                                        "description": "",
                                        "database": "Oracle (production)",
                                        "ddl": table_ddl,
                                        "indexes_ddl": indexes_ddl,
                                        "key_columns": {},
                                        "business_rules": [],
                                        "relationships": [],
                                        "usage_notes": [],
                                    }

                                    tables_dir.mkdir(parents=True, exist_ok=True)
                                    with open(target_json, "w", encoding="utf-8") as f:
                                        json.dump(table_doc, f, ensure_ascii=False, indent=2)

                                    st.success(f"✅ Создан файл описания таблицы: `{target_json.name}`")
                                    st.info(
                                        "Теперь вы можете отредактировать описание, business rules и usage notes "
                                        "во вкладке выше, а KB сможет использовать DDL и индексы уже сейчас."
                                    )
                            except Exception as e:
                                st.error(f"❌ Ошибка при загрузке DDL или создании JSON: {e}")
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    # ========== ВКЛАДКА: КОДОВЫЕ ПРИМЕРЫ ==========
    with tab_code:
        kb_root = Path(__file__).parent.parent
        code_dir = kb_root / "training_data"
        code_file = code_dir / "code_examples.json"

        st.subheader("💻 Кодовые примеры (Perl / Python / SQL / Shell)")
        st.markdown(
            """
            Здесь можно сохранять фрагменты кода, которые иллюстрируют текущий функционал:
            - Perl-скрипты загрузки / обработки;
            - Python-утилиты;
            - SQL / PL/SQL процедуры;
            - Shell-скрипты.

            Ассистент будет использовать эти примеры как контекст при генерации кода для поддержки бизнес-процессов.
            """
        )

        code_dir.mkdir(parents=True, exist_ok=True)
        if code_file.exists():
            try:
                with open(code_file, "r", encoding="utf-8") as f:
                    code_examples: List[Dict[str, Any]] = json.load(f)
            except Exception:
                code_examples = []
        else:
            code_examples = []

        if code_examples:
            st.markdown("### 📦 Существующие кодовые примеры")
            for i, ex in enumerate(code_examples[-10:][::-1], 1):
                title = ex.get("title") or ex.get("description") or f"Пример {i}"
                with st.expander(f"{i}. {title} [{ex.get('language', 'unknown')}]"):
                    st.markdown(f"**Язык:** {ex.get('language', '')}")
                    if ex.get("description"):
                        st.markdown(f"**Описание:** {ex['description']}")
                    if ex.get("related_objects"):
                        st.markdown(f"**Связанные объекты:** {', '.join(ex['related_objects'])}")
                    if ex.get("tags"):
                        st.markdown(f"**Теги:** {', '.join(ex['tags'])}")
                    st.code(ex.get("code", ""), language=ex.get("language", "").lower() or None)
        else:
            st.info("Пока нет сохранённых кодовых примеров.")

        st.markdown("---")
        st.subheader("➕ Добавить новый кодовый пример")

        with st.form("kb_add_code_example"):
            language = st.selectbox(
                "Язык",
                options=["Perl", "Python", "SQL", "PL/SQL", "Shell", "Other"],
                key="kb_code_language",
            )
            title = st.text_input(
                "Краткое название примера",
                key="kb_code_title",
                placeholder="Например: Скрипт dedup для ANALYTICS по PERIOD_ID",
            )
            description = st.text_area(
                "Описание (что делает код, с какими таблицами / VIEW работает)",
                height=120,
                key="kb_code_description",
            )
            related_objects_raw = st.text_input(
                "Связанные таблицы / VIEW (через запятую)",
                key="kb_code_related",
                placeholder="Например: ANALYTICS, BM_SERVICE_MONEY",
            )
            tags_raw = st.text_input(
                "Теги (через запятую)",
                key="kb_code_tags",
                placeholder="Например: dedup, analytics, perl",
            )
            code_text = st.text_area(
                "Код",
                height=260,
                key="kb_code_text",
                placeholder="# Вставьте сюда Perl / Python / SQL / Shell код...",
            )

            save_code_btn = st.form_submit_button("💾 Сохранить кодовый пример", type="primary", use_container_width=True)

            if save_code_btn:
                if not code_text.strip() or not title.strip():
                    st.error("❌ Укажите хотя бы название и сам код.")
                else:
                    related_objects = [
                        o.strip()
                        for o in related_objects_raw.split(",")
                        if o.strip()
                    ]
                    tags = [t.strip() for t in tags_raw.split(",") if t.strip()]

                    example = {
                        "language": language,
                        "title": title.strip(),
                        "description": description.strip(),
                        "code": code_text,
                        "related_objects": related_objects,
                        "tags": tags,
                    }

                    code_examples.append(example)
                    try:
                        with open(code_file, "w", encoding="utf-8") as f:
                            json.dump(code_examples, f, ensure_ascii=False, indent=2)
                        st.success("✅ Кодовый пример сохранён в `training_data/code_examples.json`.")
                        st.info("KB сможет использовать эти примеры после следующего обновления (init_kb).")
                        st.experimental_rerun()
                    except Exception as e:
                        st.error(f"❌ Ошибка при сохранении кодового примера: {e}")

    # Собранные примеры и переобучение (видно всегда, вне вкладок)
    st.markdown("---")
    if st.session_state.collected_examples:
        st.markdown("---")
        st.subheader("📦 Собранные примеры для переобучения")
        
        collected_count = len(st.session_state.collected_examples)
        st.info(f"Собрано примеров: **{collected_count}**")
        
        # Показываем список собранных примеров
        for i, example in enumerate(st.session_state.collected_examples, 1):
            with st.expander(f"Пример {i}: {example['question'][:50]}..."):
                st.markdown(f"**Вопрос:** {example['question']}")
                st.code(example['sql'], language="sql")
                st.markdown(f"**Категория:** {example.get('category', 'Общие')}")
                st.markdown(f"**Сложность:** {example.get('complexity', 2)}/5")
        
        # Кнопка переобучения KB
        st.markdown("---")
        st.subheader("🎓 Переобучение KB")
        
        col1, col2 = st.columns([2, 1])
        with col1:
            recreate_collection = st.checkbox(
                "Пересоздать коллекцию (удалит все существующие данные)",
                value=False,
                key="recreate_collection"
            )
        with col2:
            if recreate_collection:
                st.warning("⚠️ Все данные будут удалены!")
        
        if st.button("🔄 Переобучить KB с новыми примерами", type="primary", use_container_width=True):
            with st.spinner("Переобучение KB... Это может занять несколько минут."):
                success, message = agent.retrain_kb_with_new_examples(recreate=recreate_collection)
                
                if success:
                    st.success(message)
                    st.balloons()
                    
                    # Очищаем собранные примеры после успешного переобучения
                    st.session_state.collected_examples = []
                    st.session_state.current_example = None
                    
                    # Обновляем статистику
                    new_count = agent.get_existing_examples_count()
                    st.info(f"📊 KB переобучена. Всего примеров: **{new_count}**")
                else:
                    st.error(message)
    
    else:
        st.info("💡 Введите вопрос и сохраните примеры для переобучения KB")

