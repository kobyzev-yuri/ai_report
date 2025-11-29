#!/usr/bin/env python3
"""
Streamlit модуль для интеграции RAG ассистента
"""
import os
# Исправление проблемы с protobuf - должно быть ДО импорта transformers
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

import streamlit as st
import sys
from pathlib import Path

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from kb_billing.rag.rag_assistant import RAGAssistant
import pandas as pd
import re
import cx_Oracle
import os


@st.cache_resource
def init_assistant():
    """Инициализация RAG ассистента (кэшируется для избежания rerun)"""
    try:
        # Параметры Qdrant из переменных окружения или config.env
        from kb_billing.rag.config_sql4a import SQL4AConfig
        
        qdrant_host = os.getenv("QDRANT_HOST", SQL4AConfig.QDRANT_HOST)
        qdrant_port = int(os.getenv("QDRANT_PORT", SQL4AConfig.QDRANT_PORT))
        
        return RAGAssistant(
            qdrant_host=qdrant_host,
            qdrant_port=qdrant_port
        )
    except Exception as e:
        st.error(f"Ошибка инициализации RAG ассистента: {e}")
        import traceback
        st.code(traceback.format_exc())
        st.info("Убедитесь, что Qdrant запущен: `docker run -p 6333:6333 qdrant/qdrant`")
        return None


def show_assistant_tab():
    """Отображение закладки с ассистентом"""
    
    st.header("🤖 Ассистент для аналитических отчетов")
    st.markdown("""
    Ассистент поможет вам:
    - 📊 Генерировать SQL запросы для аналитических отчетов
    - 🔍 Искать информацию по SBD услугам
    - 📋 Находить примеры запросов по схожим вопросам
    """)
    
    st.markdown("---")
    
    # Инициализация ассистента (кэшируется, не вызывает rerun)
    assistant = init_assistant()
    if not assistant:
        return
    
    # Инициализация session_state
    if "assistant_question" not in st.session_state:
        st.session_state.assistant_question = ""
    if "assistant_category" not in st.session_state:
        st.session_state.assistant_category = "Все категории"
    if "assistant_action" not in st.session_state:
        st.session_state.assistant_action = None  # None, "search", "generate"
    
    # Две колонки: вопрос и результаты
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("💬 Ваш вопрос")
        
        # Используем форму для предотвращения rerun при вводе
        with st.form("assistant_form", clear_on_submit=False):
            # Поле ввода вопроса
            question_input = st.text_area(
                "Введите ваш вопрос на русском языке:",
                height=150,
                placeholder="Например: Покажи превышение трафика за октябрь 2025",
                value=st.session_state.assistant_question,
                key="assistant_question_input"
            )
            
            # Категория для фильтрации (опционально)
            category = st.selectbox(
                "Категория (опционально):",
                ["Все категории", "Превышение трафика", "Сервисы", "Клиенты", 
                 "Себестоимость", "Аналитика", "Отчеты", "Финансовые алерты"],
                index=["Все категории", "Превышение трафика", "Сервисы", "Клиенты", 
                       "Себестоимость", "Аналитика", "Отчеты", "Финансовые алерты"].index(st.session_state.assistant_category),
                key="assistant_category_input"
            )
            
            # Две кнопки в форме
            col_btn1, col_btn2 = st.columns(2)
            with col_btn1:
                search_button = st.form_submit_button("🔍 Найти похожие примеры", type="primary", use_container_width=True)
            with col_btn2:
                generate_button = st.form_submit_button("📊 Сгенерировать SQL", type="secondary", use_container_width=True)
            
            # Обработка нажатия кнопок
            if search_button:
                st.session_state.assistant_action = "search"
                st.session_state.assistant_question = question_input
                st.session_state.assistant_category = category
            elif generate_button:
                st.session_state.assistant_action = "generate"
                st.session_state.assistant_question = question_input
                st.session_state.assistant_category = category
        
        # Используем сохраненное значение вопроса
        question = st.session_state.assistant_question
        category = st.session_state.assistant_category
    
    with col2:
        st.subheader("📋 Результаты")
        
        # Используем action из session_state вместо прямых проверок кнопок
        if st.session_state.assistant_action == "search" and question:
            with st.spinner("Поиск похожих примеров..."):
                # Фильтр по категории
                filter_category = None if category == "Все категории" else category
                
                # Поиск похожих примеров
                examples = assistant.search_similar_examples(
                    question=question,
                    category=filter_category,
                    limit=5
                )
                
                if examples:
                    st.success(f"Найдено {len(examples)} похожих примеров")
                    
                    for i, example in enumerate(examples, 1):
                        result_key = f"example_result_{i}"
                        with st.expander(f"Пример {i} (similarity: {example['similarity']:.3f})", expanded=(i == 1)):
                            st.markdown(f"**Вопрос:** {example['question']}")
                            st.markdown(f"**Категория:** {example.get('category', 'N/A')}")
                            st.markdown(f"**Сложность:** {example.get('complexity', 'N/A')}")
                            
                            # SQL запрос с возможностью копирования
                            st.markdown("**SQL запрос:**")
                            st.code(example['sql'], language="sql")
                            
                            # Кнопка выполнения SQL
                            if st.button(f"▶️ Выполнить SQL {i}", key=f"execute_sql_{i}"):
                                execute_sql_query(example['sql'], result_key=result_key)
                            
                            # Отображение сохраненного результата, если есть
                            if result_key in st.session_state:
                                result = st.session_state[result_key]
                                if "df" in result and result["df"] is not None:
                                    st.markdown("---")
                                    st.markdown(f"**Результаты выполнения (обновлено: {result['timestamp']}):**")
                                    if result["df"].empty:
                                        st.info("ℹ️ Запрос выполнен успешно, но результатов нет")
                                    else:
                                        st.success(f"✅ Найдено записей: {len(result['df'])}")
                                        st.dataframe(result["df"], use_container_width=True, height=400)
                                        
                                        # Кнопка экспорта
                                        csv = result["df"].to_csv(index=False).encode('utf-8')
                                        st.download_button(
                                            label="📥 Скачать CSV",
                                            data=csv,
                                            file_name=f"query_result_{result['timestamp'].strftime('%Y%m%d_%H%M%S')}.csv",
                                            mime="text/csv",
                                            key=f"download_{result_key}_saved"
                                        )
                                elif "error" in result:
                                    st.markdown("---")
                                    st.error(f"❌ Ошибка: {result['error']}")
                                    with st.expander("🔍 Детали ошибки", expanded=False):
                                        st.code(result.get("traceback", ""), language="python")
                else:
                    st.info("Похожие примеры не найдены. Попробуйте переформулировать вопрос.")
        
        elif st.session_state.assistant_action == "generate" and question:
            with st.spinner("Генерация SQL запроса..."):
                # Получение контекста
                context = assistant.get_context_for_sql_generation(question, max_examples=5)
                
                # Попытка генерации SQL через LLM
                api_key = os.getenv("OPENAI_API_KEY")
                # Поддержка обоих вариантов: OPENAI_BASE_URL (как в sql4A) и OPENAI_API_BASE
                api_base = os.getenv("OPENAI_BASE_URL") or os.getenv("OPENAI_API_BASE")
                
                generated_sql = None
                if api_key:
                    try:
                        generated_sql = assistant.generate_sql_with_llm(
                            question=question,
                            context=context,
                            api_key=api_key,
                            api_base=api_base
                        )
                    except Exception as e:
                        st.warning(f"Не удалось сгенерировать SQL через LLM: {e}")
                
                # Если SQL сгенерирован, показываем и выполняем
                if generated_sql:
                    st.success("✅ SQL запрос сгенерирован!")
                    st.markdown("**Сгенерированный SQL:**")
                    st.code(generated_sql, language="sql")
                    
                    # Автоматическое выполнение SQL
                    st.markdown("**Результаты выполнения:**")
                    execute_sql_query(generated_sql)
                    
                    # Кнопка для повторного выполнения
                    if st.button("🔄 Выполнить повторно", key="re_execute_generated"):
                        execute_sql_query(generated_sql)
                else:
                    # Если LLM недоступен, показываем контекст и примеры
                    api_key = os.getenv("OPENAI_API_KEY")
                    if not api_key:
                        st.info("""
                        💡 **Автоматическая генерация SQL через LLM недоступна**
                        
                        Для включения автоматической генерации SQL установите в `config.env`:
                        - `OPENAI_API_KEY=your-api-key`
                        - `OPENAI_API_BASE=https://api.proxyapi.ru/openai/v1` (опционально, для прокси)
                        
                        **Сейчас доступно:** Вы можете использовать похожие примеры ниже и выполнить их кнопкой "▶️ Выполнить".
                        """)
                    
                    # Форматирование контекста
                    formatted_context = assistant.format_context_for_llm(context)
                    
                    # Показываем контекст
                    st.markdown("**Контекст для генерации:**")
                    with st.expander("Показать контекст", expanded=False):
                        st.text(formatted_context)
                    
                    # Если есть похожие примеры, показываем их
                    if context.get("examples"):
                        st.markdown("**Рекомендуемые примеры:**")
                        for i, example in enumerate(context["examples"][:3], 1):
                            result_key_gen = f"gen_example_result_{i}"
                            st.markdown(f"{i}. {example['question']}")
                            st.code(example['sql'], language="sql")
                            
                            # Кнопка выполнения для каждого примера
                            if st.button(f"▶️ Выполнить пример {i}", key=f"execute_gen_example_{i}"):
                                execute_sql_query(example['sql'], result_key=result_key_gen)
                            
                            # Отображение сохраненного результата, если есть
                            if result_key_gen in st.session_state:
                                result = st.session_state[result_key_gen]
                                if "df" in result and result["df"] is not None:
                                    st.markdown("---")
                                    st.markdown(f"**Результаты выполнения:**")
                                    if result["df"].empty:
                                        st.info("ℹ️ Запрос выполнен успешно, но результатов нет")
                                    else:
                                        st.success(f"✅ Найдено записей: {len(result['df'])}")
                                        st.dataframe(result["df"], use_container_width=True, height=400)
                                        
                                        # Кнопка экспорта
                                        csv = result["df"].to_csv(index=False).encode('utf-8')
                                        st.download_button(
                                            label="📥 Скачать CSV",
                                            data=csv,
                                            file_name=f"query_result_{result['timestamp'].strftime('%Y%m%d_%H%M%S')}.csv",
                                            mime="text/csv",
                                            key=f"download_{result_key_gen}_saved"
                                        )
                                elif "error" in result:
                                    st.markdown("---")
                                    st.error(f"❌ Ошибка: {result['error']}")
                                    with st.expander("🔍 Детали ошибки", expanded=False):
                                        st.code(result.get("traceback", ""), language="python")
                    
                    # Информация о таблицах
                    if context.get("tables_info"):
                        st.markdown("**Используемые таблицы:**")
                        for table_name in context["tables_info"].keys():
                            st.markdown(f"- {table_name}")
                    
                    st.info("""
                    💡 **Для автоматической генерации SQL:** 
                    
                    Установите переменную окружения OPENAI_API_KEY в config.env.
                    Для использования прокси (например, proxyapi.ru) установите OPENAI_API_BASE=https://api.proxyapi.ru/openai/v1
                    
                    Вы можете скопировать SQL из похожих примеров выше и выполнить его вручную.
                    """)
        
        else:
            st.info("💡 Введите вопрос и нажмите кнопку **🔍 Найти похожие примеры** или **📊 Сгенерировать SQL**")


def get_connection():
    """Создание подключения к Oracle (из streamlit_report_oracle_backup.py)"""
    try:
        ORACLE_USER = os.getenv('ORACLE_USER')
        ORACLE_PASSWORD = os.getenv('ORACLE_PASSWORD')
        ORACLE_HOST = os.getenv('ORACLE_HOST')
        ORACLE_PORT = int(os.getenv('ORACLE_PORT', '1521'))
        ORACLE_SID = os.getenv('ORACLE_SID')
        ORACLE_SERVICE = os.getenv('ORACLE_SERVICE') or os.getenv('ORACLE_SID')
        
        if not all([ORACLE_USER, ORACLE_PASSWORD, ORACLE_HOST]):
            return None
        
        if ORACLE_SID:
            dsn = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, sid=ORACLE_SID)
        else:
            dsn = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, service_name=ORACLE_SERVICE)
        
        conn = cx_Oracle.connect(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=dsn
        )
        return conn
    except Exception as e:
        st.error(f"Ошибка подключения к Oracle: {e}")
        return None


def execute_sql_query(sql: str, result_key: str = "sql_result"):
    """Выполнение SQL запроса в Oracle и сохранение результата в session_state"""
    try:
        conn = get_connection()
        if not conn:
            st.error("❌ Не удалось подключиться к базе данных. Проверьте настройки Oracle в config.env")
            return
        
        # Выполнение запроса
        with st.spinner("Выполнение SQL запроса..."):
            df = pd.read_sql(sql, conn)
            conn.close()
        
        # Сохранение результата в session_state
        st.session_state[result_key] = {
            "sql": sql,
            "df": df,
            "timestamp": pd.Timestamp.now()
        }
        
        # Отображение результата
        if df.empty:
            st.info("ℹ️ Запрос выполнен успешно, но результатов нет")
        else:
            st.success(f"✅ Запрос выполнен успешно. Найдено записей: {len(df)}")
            st.dataframe(df, use_container_width=True, height=400)
            
            # Кнопка экспорта
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Скачать CSV",
                data=csv,
                file_name=f"query_result_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key=f"download_{result_key}"
            )
    except Exception as e:
        error_msg = str(e)
        st.error(f"❌ Ошибка при выполнении SQL: {error_msg}")
        import traceback
        with st.expander("🔍 Детали ошибки", expanded=False):
            st.code(traceback.format_exc(), language="python")
        
        # Сохранение ошибки в session_state
        st.session_state[result_key] = {
            "sql": sql,
            "error": error_msg,
            "traceback": traceback.format_exc()
        }

