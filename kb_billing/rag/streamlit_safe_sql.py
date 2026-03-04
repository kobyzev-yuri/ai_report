#!/usr/bin/env python3
"""
Streamlit модуль для безопасного выполнения модифицирующих SQL запросов
"""
import os
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

import streamlit as st
import sys
from pathlib import Path
import pandas as pd

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from kb_billing.rag.safe_sql_executor import SafeSQLExecutor
from kb_billing.rag.rag_assistant import RAGAssistant


@st.cache_resource
def init_safe_executor():
    """Инициализация безопасного исполнителя (кэшируется)"""
    try:
        return SafeSQLExecutor()
    except Exception as e:
        st.error(f"Ошибка инициализации безопасного исполнителя: {e}")
        return None


@st.cache_resource
def init_assistant():
    """Инициализация RAG ассистента для генерации SQL"""
    try:
        from kb_billing.rag.config_sql4a import SQL4AConfig
        
        qdrant_host = os.getenv("QDRANT_HOST", SQL4AConfig.QDRANT_HOST)
        qdrant_port = int(os.getenv("QDRANT_PORT", SQL4AConfig.QDRANT_PORT))
        
        return RAGAssistant(
            qdrant_host=qdrant_host,
            qdrant_port=qdrant_port
        )
    except Exception as e:
        st.error(f"Ошибка инициализации RAG ассистента: {e}")
        return None


def show_safe_sql_tab():
    """Отображение закладки для безопасного выполнения SQL"""
    
    st.header("🔒 Безопасное выполнение SQL (INSERT/UPDATE/DELETE)")
    st.markdown("""
    **Безопасная работа с модифицирующими запросами:**
    - ✅ Валидация SQL запросов
    - 👁️ Предварительный просмотр изменений (dry-run)
    - 🔄 Транзакции с возможностью отката
    - 📝 Логирование всех операций
    - ⚠️ Предупреждения о потенциально опасных операциях
    """)
    
    st.markdown("---")
    
    # Инициализация
    executor = init_safe_executor()
    assistant = init_assistant()
    
    if not executor:
        st.error("❌ Не удалось инициализировать безопасный исполнитель")
        return
    
    # Режим работы
    st.subheader("⚙️ Режим работы")
    mode = st.radio(
        "Выберите режим:",
        ["🔍 Только предпросмотр (dry-run)", "✅ Выполнение с подтверждением", "🚀 Прямое выполнение (осторожно!)"],
        key="execution_mode"
    )
    
    dry_run = mode == "🔍 Только предпросмотр (dry-run)"
    require_confirmation = mode == "✅ Выполнение с подтверждением"
    
    st.markdown("---")
    
    # Инициализация session_state
    if "safe_sql_query" not in st.session_state:
        st.session_state.safe_sql_query = ""
    if "safe_sql_result" not in st.session_state:
        st.session_state.safe_sql_result = None
    if "safe_sql_preview" not in st.session_state:
        st.session_state.safe_sql_preview = None
    
    st.subheader("💬 SQL запрос")
    
    # Примеры запросов
    with st.expander("📋 Примеры модифицирующих запросов"):
        st.markdown("""
        **Пример 1: Обновление налога в тарифных элементах**
        ```sql
        UPDATE TARIFF_EL 
        SET MONEY = MONEY * 1.1  -- Увеличение на 10% (20% -> 22%)
        WHERE TARIFF_ID IN (
            SELECT TARIFF_ID FROM TARIFF 
            WHERE TARIFF_TYPE = 'ABONENTKA'
        )
        AND START_DATE >= TO_DATE('2026-01-01', 'YYYY-MM-DD')
        ```
        
        **Пример 2: Создание новых тарифных элементов на следующий год**
        ```sql
        INSERT INTO TARIFF_EL (TARIFF_ID, ZONE_ID, MONEY, START_DATE, TAX_RATE)
        SELECT 
            TARIFF_ID,
            ZONE_ID,
            MONEY * 1.1,  -- Увеличение на 10%
            TO_DATE('2026-01-01', 'YYYY-MM-DD'),
            22  -- Новый налог 22%
        FROM TARIFF_EL
        WHERE START_DATE = TO_DATE('2025-01-01', 'YYYY-MM-DD')
        AND TAX_RATE = 20
        ```
        """)
    
    # Форма для ввода SQL
    with st.form("safe_sql_form", clear_on_submit=False):
        sql_input = st.text_area(
            "Введите SQL запрос (INSERT/UPDATE/DELETE):",
            height=200,
            placeholder="UPDATE TARIFF_EL SET MONEY = MONEY * 1.1 WHERE ...",
            value=st.session_state.safe_sql_query,
            key="safe_sql_input"
        )
        
        col1, col2 = st.columns(2)
        with col1:
            analyze_button = st.form_submit_button("🔍 Проанализировать", type="primary", use_container_width=True)
        with col2:
            if assistant:
                generate_button = st.form_submit_button("🤖 Сгенерировать SQL", use_container_width=True)
            else:
                generate_button = None
        
        if generate_button and assistant:
            # Генерация SQL через ассистента
            question = st.text_input("Опишите, что нужно сделать:", key="safe_sql_question")
            if question:
                with st.spinner("Генерация SQL..."):
                    api_key = os.getenv("OPENAI_API_KEY")
                    api_base = os.getenv("OPENAI_BASE_URL") or os.getenv("OPENAI_API_BASE")
                    
                    if api_key:
                        context = assistant.get_context_for_sql_generation(question, max_examples=5)
                        generated_sql = assistant.generate_sql_with_llm(
                            question=question,
                            context=context,
                            api_key=api_key,
                            api_base=api_base
                        )
                        if generated_sql:
                            st.session_state.safe_sql_query = generated_sql
                            st.rerun()
        
        if analyze_button:
            st.session_state.safe_sql_query = sql_input
            st.session_state.safe_sql_result = None
            st.session_state.safe_sql_preview = None
    
    sql = st.session_state.safe_sql_query
    
    if sql:
        st.markdown("---")
        st.subheader("📋 Анализ запроса")
        
        # Проверка типа запроса
        is_modifying = executor.is_modifying_query(sql)
        
        if is_modifying:
            st.info("⚠️ **Модифицирующий запрос обнаружен**")
            
            # Валидация
            is_valid, error, warnings = executor.validate_sql(sql)
            
            if not is_valid:
                st.error(f"❌ **Ошибка валидации:** {error}")
            else:
                st.success("✅ **SQL запрос валиден**")
                
                if warnings:
                    st.warning("⚠️ **Предупреждения:**")
                    for warning in warnings:
                        st.warning(warning)
                
                # Предварительный просмотр
                st.markdown("---")
                st.subheader("👁️ Предварительный просмотр изменений")
                
                with st.spinner("Анализ предстоящих изменений..."):
                    preview = executor.preview_changes(sql)
                
                if preview.get("error"):
                    st.error(f"❌ Ошибка предпросмотра: {preview['error']}")
                else:
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Тип операции", preview.get("query_type", "UNKNOWN"))
                    with col2:
                        rows = preview.get("estimated_rows")
                        if rows:
                            if isinstance(rows, int):
                                st.metric("Затронуто строк", rows)
                            else:
                                st.info(f"Строк: {rows}")
                    
                    if preview.get("affected_tables"):
                        st.info(f"📊 **Затронутые таблицы:** {', '.join(preview['affected_tables'])}")
                    
                    if preview.get("preview_data"):
                        st.markdown("**Данные, которые будут изменены (первые 10 строк):**")
                        if isinstance(preview["preview_data"], list):
                            # Пытаемся преобразовать в DataFrame
                            try:
                                df = pd.DataFrame(preview["preview_data"])
                                st.dataframe(df, use_container_width=True)
                            except:
                                st.code(str(preview["preview_data"][:5]))
                        else:
                            st.info(preview["preview_data"])
                    
                    st.session_state.safe_sql_preview = preview
                    
                    # Кнопка выполнения
                    st.markdown("---")
                    st.subheader("▶️ Выполнение")
                    
                    if dry_run:
                        st.info("🔍 **Режим dry-run:** Запрос не будет выполнен, только предпросмотр")
                    elif require_confirmation:
                        # Двухэтапное подтверждение
                        confirm_key = f"confirm_execute_{hash(sql)}"
                        
                        if confirm_key not in st.session_state:
                            st.warning("⚠️ **Требуется подтверждение для выполнения модифицирующего запроса**")
                            
                            col1, col2 = st.columns(2)
                            with col1:
                                if st.button("✅ Подтвердить и выполнить", key="confirm_exec", type="primary", use_container_width=True):
                                    st.session_state[confirm_key] = True
                                    st.rerun()
                            with col2:
                                if st.button("❌ Отменить", key="cancel_exec", use_container_width=True):
                                    st.session_state.safe_sql_query = ""
                                    st.rerun()
                        else:
                            # Выполняем запрос
                            with st.spinner("Выполнение SQL запроса..."):
                                result = executor.execute_in_transaction(sql, confirm_commit=False)
                            
                            if result.get("success"):
                                st.success(f"✅ **Запрос выполнен успешно!**")
                                st.metric("Затронуто строк", result.get("rows_affected", 0))
                                
                                if result.get("committed"):
                                    st.success("✅ **Изменения зафиксированы в базе данных**")
                                else:
                                    st.warning("⚠️ **Изменения НЕ зафиксированы. Используйте кнопку 'Зафиксировать' для коммита.**")
                                
                                st.session_state.safe_sql_result = result
                            else:
                                st.error(f"❌ **Ошибка выполнения:** {result.get('error')}")
                                if result.get("rolled_back"):
                                    st.info("✅ **Транзакция откачена**")
                    else:
                        # Прямое выполнение (с предупреждением)
                        st.warning("🚨 **ВНИМАНИЕ:** Прямое выполнение без подтверждения!")
                        
                        if st.button("🚀 Выполнить запрос", type="primary", use_container_width=True):
                            with st.spinner("Выполнение SQL запроса..."):
                                result = executor.execute_in_transaction(sql, confirm_commit=False)
                            
                            if result.get("success"):
                                st.success(f"✅ **Запрос выполнен успешно!**")
                                st.metric("Затронуто строк", result.get("rows_affected", 0))
                                st.session_state.safe_sql_result = result
                            else:
                                st.error(f"❌ **Ошибка выполнения:** {result.get('error')}")
        else:
            st.info("ℹ️ Это SELECT запрос. Для выполнения используйте вкладку '🤖 Биллинг ассистент'")
    
    # История выполнения
    if st.session_state.safe_sql_result:
        st.markdown("---")
        st.subheader("📝 Результат выполнения")
        result = st.session_state.safe_sql_result
        
        if result.get("success"):
            st.json({
                "rows_affected": result.get("rows_affected", 0),
                "committed": result.get("committed", False),
                "rolled_back": result.get("rolled_back", False),
                "execution_time": result.get("execution_time")
            })
            
            if not result.get("committed") and not result.get("rolled_back"):
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("✅ Зафиксировать изменения", type="primary", use_container_width=True):
                        commit_result = executor.commit_transaction()
                        if commit_result.get("success"):
                            st.success("✅ Изменения зафиксированы")
                            st.rerun()
                with col2:
                    if st.button("🔄 Откатить изменения", use_container_width=True):
                        rollback_result = executor.rollback_transaction()
                        if rollback_result.get("success"):
                            st.info("✅ Изменения откачены")
                            st.rerun()






