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

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from kb_billing.rag.kb_expansion_agent import KBExpansionAgent


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
    
    # Вкладки для разных способов добавления
    tab_import, tab_generate = st.tabs(["📥 Импорт SQL отчета", "🤖 Генерация из вопроса"])
    
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

