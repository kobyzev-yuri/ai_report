#!/usr/bin/env python3
"""
Streamlit модуль для интеграции RAG ассистента
"""
import os
# Исправление проблемы с protobuf - должно быть ДО импорта transformers
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

import time
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
    """)
    
    st.markdown("---")
    
    # Инициализация ассистента (кэшируется, не вызывает rerun)
    assistant = init_assistant()
    if not assistant:
        return
    
    # Инициализация session_state
    if "assistant_question" not in st.session_state:
        st.session_state.assistant_question = ""
    if "assistant_action" not in st.session_state:
        st.session_state.assistant_action = None  # None, "generate"
    if "last_generated_question" not in st.session_state:
        st.session_state.last_generated_question = ""  # Последний вопрос, для которого был сгенерирован SQL
    if "last_generated_sql" not in st.session_state:
        st.session_state.last_generated_sql = None  # Последний сгенерированный SQL
    
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
        
        # Кнопка генерации SQL
        generate_button = st.form_submit_button("📊 Сгенерировать SQL", type="primary", use_container_width=True)
        
        # Обработка нажатия кнопки
        if generate_button:
            st.session_state.assistant_action = "generate"
            st.session_state.assistant_question = question_input
            # Очищаем предыдущие результаты при новой генерации
            st.session_state.last_generated_question = ""
            st.session_state.last_generated_sql = None
    
    # Используем сохраненное значение вопроса
    question = st.session_state.assistant_question
    
    st.markdown("---")
    
    # Генерация SQL
    if st.session_state.assistant_action == "generate" and question:
        # Проверяем, изменился ли вопрос - если да, генерируем новый SQL
        question_changed = (st.session_state.last_generated_question != question)
        
        # Если вопрос не изменился и SQL уже был сгенерирован, показываем его
        if not question_changed and st.session_state.last_generated_sql:
            generated_sql = st.session_state.last_generated_sql
            context = None  # Контекст не нужен, если SQL уже есть
        else:
            # Генерируем новый SQL только если вопрос изменился
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
                        # Сохраняем сгенерированный SQL и вопрос
                        if generated_sql:
                            st.session_state.last_generated_sql = generated_sql
                            st.session_state.last_generated_question = question
                    except Exception as e:
                        st.warning(f"Не удалось сгенерировать SQL через LLM: {e}")
        
        # Если SQL сгенерирован, показываем и выполняем
        if generated_sql:
            st.success("✅ SQL запрос сгенерирован!")
            st.markdown("**Сгенерированный SQL:**")
            st.code(generated_sql, language="sql")
            
            # Кнопки для анализа и выполнения
            col_exec, col_stats = st.columns([2, 1])
            with col_exec:
                execute_btn = st.button("▶️ Выполнить запрос", key="execute_generated", type="primary", use_container_width=True)
            with col_stats:
                stats_btn = st.button("📈 Со статистикой", key="execute_with_stats_generated", use_container_width=True)
            
            # Обработка кнопок
            if execute_btn:
                execute_sql_query(generated_sql, result_key="sql_result", check_plan=True)
            elif stats_btn:
                st.info("💡 **Примечание:** Эта функция показывает фактический план выполнения запроса. Она НЕ собирает статистику таблиц для оптимизатора Oracle. Для улучшения плана выполнения используйте кнопку '📊 Собрать статистику' после выполнения запроса.")
                
                with st.spinner("Выполнение запроса со сбором статистики выполнения..."):
                    df, exec_time, stats_text = execute_sql_with_stats(generated_sql, result_key="generated_with_stats")
                
                if df is not None:
                    if exec_time:
                        st.metric("⏱️ Время выполнения", f"{exec_time:.2f} сек")
                    if stats_text:
                        st.markdown("**Фактический план выполнения (Actual Execution Plan):**")
                        st.code(stats_text, language="text")
                        st.info("💡 Этот план показывает, как запрос был выполнен. Для улучшения плана на будущее используйте кнопку '📊 Собрать статистику' ниже.")
                        
                        # Извлекаем таблицы из SQL для предложения сбора статистики
                        tables = extract_tables_from_sql(generated_sql)
                        if tables:
                            st.markdown("**📊 Собрать статистику для улучшения плана:**")
                            for table in tables[:5]:  # Показываем максимум 5 таблиц
                                if st.button(f"📊 Собрать статистику для {table}", key=f"gather_stats_{table}_generated"):
                                    with st.spinner(f"Сбор статистики для таблицы {table}... Это может занять несколько минут для больших таблиц."):
                                        success, message = gather_table_stats(table)
                                        if success:
                                            st.success(message)
                                        else:
                                            st.error(message)
                    
                    # Сохраняем результат для отображения ниже
                    st.session_state["sql_result"] = {
                        "sql": generated_sql,
                        "df": df,
                        "timestamp": pd.Timestamp.now()
                    }
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
            if context:
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
        st.info("💡 Введите вопрос и нажмите кнопку **📊 Сгенерировать SQL**")
    
    # Единое место для отображения результатов снизу
    st.markdown("---")
    st.subheader("📋 Результаты выполнения")
    
    # Проверяем все возможные ключи результатов
    result_keys_to_check = [
        "sql_result",
        "generated_with_stats",
        "gen_example_result_1", "gen_example_result_2", "gen_example_result_3"
    ]
    
    displayed_result = False
    for result_key in result_keys_to_check:
        if result_key in st.session_state:
            result = st.session_state[result_key]
            if "df" in result and result["df"] is not None:
                displayed_result = True
                if result["df"].empty:
                    st.info("ℹ️ Запрос выполнен успешно, но результатов нет")
                else:
                    st.success(f"✅ Запрос выполнен успешно. Найдено записей: {len(result['df'])}")
                    st.dataframe(result["df"], use_container_width=True, height=400)
                    
                    # Кнопка экспорта
                    csv = result["df"].to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Скачать CSV",
                        data=csv,
                        file_name=f"query_result_{result['timestamp'].strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        key=f"download_{result_key}_final"
                    )
                break  # Показываем только один результат
            elif "error" in result:
                displayed_result = True
                st.error(f"❌ Ошибка: {result['error']}")
                with st.expander("🔍 Детали ошибки", expanded=False):
                    st.code(result.get("traceback", ""), language="python")
                break
    
    if not displayed_result:
        st.info("💡 Результаты выполнения запросов будут отображаться здесь")


def show_financial_analysis_tab():
    """Отображение закладки с финансовым анализом"""
    
    st.header("📊 Финансовый анализ и исследование прибыльности")
    st.markdown("""
    **Финансовый анализ для выявления проблем с прибыльностью:**
    - 🔍 Выявление убыточных клиентов и услуг (расходы > доходы)
    - 📈 Анализ динамики прибыльности по периодам
    - 📉 Выявление тенденций к ухудшению прибыльности
    - 💡 Анализ структуры затрат и доходов
    - ⚠️ Выявление клиентов с низкой маржой
    """)
    
    st.markdown("---")
    
    # Инициализация ассистента (кэшируется, не вызывает rerun)
    assistant = init_assistant()
    if not assistant:
        return
    
    # Инициализация session_state для финансового анализа
    if "financial_question" not in st.session_state:
        st.session_state.financial_question = ""
    if "financial_action" not in st.session_state:
        st.session_state.financial_action = None
    if "last_financial_question" not in st.session_state:
        st.session_state.last_financial_question = ""  # Последний вопрос, для которого был сгенерирован SQL
    if "last_financial_sql" not in st.session_state:
        st.session_state.last_financial_sql = None  # Последний сгенерированный SQL
    
    st.subheader("💬 Ваш вопрос для финансового анализа")
    
    # Используем форму для предотвращения rerun при вводе
    with st.form("financial_form", clear_on_submit=False):
        # Поле ввода вопроса
        question_input = st.text_area(
            "Введите вопрос для финансового анализа:",
            height=150,
            placeholder="Например: Найди убыточных клиентов за октябрь\nИли: Покажи динамику прибыльности клиентов по периодам\nИли: Найди клиентов с ухудшением прибыльности",
            value=st.session_state.financial_question,
            key="financial_question_input"
        )
        
        # Кнопка генерации SQL
        generate_button = st.form_submit_button("📊 Сгенерировать SQL для анализа", type="primary", use_container_width=True)
        
        # Обработка нажатия кнопки
        if generate_button:
            st.session_state.financial_action = "generate"
            st.session_state.financial_question = question_input
            # Очищаем предыдущие результаты при новой генерации
            st.session_state.last_financial_question = ""
            st.session_state.last_financial_sql = None
    
    st.markdown("---")
    st.subheader("📋 Результаты финансового анализа")
    
    question = st.session_state.financial_question
    
    if st.session_state.financial_action == "generate" and question:
        # Инициализируем переменные
        generated_sql = None
        context = None
        
        # Проверяем, изменился ли вопрос - если да, генерируем новый SQL
        question_changed = (st.session_state.last_financial_question != question)
        
        # Если вопрос не изменился и SQL уже был сгенерирован, показываем его
        if not question_changed and st.session_state.last_financial_sql:
            generated_sql = st.session_state.last_financial_sql
        else:
            # Генерируем новый SQL только если вопрос изменился
            with st.spinner("Генерация SQL запроса для финансового анализа..."):
                # Получение контекста
                context = assistant.get_context_for_sql_generation(question, max_examples=5)
                
                # Попытка генерации SQL через LLM
                api_key = os.getenv("OPENAI_API_KEY")
                api_base = os.getenv("OPENAI_BASE_URL") or os.getenv("OPENAI_API_BASE")
                
                if api_key:
                    try:
                        generated_sql = assistant.generate_sql_with_llm(
                            question=question,
                            context=context,
                            api_key=api_key,
                            api_base=api_base
                        )
                        # Сохраняем сгенерированный SQL и вопрос
                        if generated_sql:
                            st.session_state.last_financial_sql = generated_sql
                            st.session_state.last_financial_question = question
                    except Exception as e:
                        st.warning(f"Не удалось сгенерировать SQL через LLM: {e}")
        
        # Если SQL сгенерирован, показываем и выполняем
        if generated_sql:
            st.success("✅ SQL запрос сгенерирован!")
            st.markdown("**Сгенерированный SQL:**")
            st.code(generated_sql, language="sql")
            
            # Кнопки для анализа и выполнения
            col_exec, col_stats = st.columns([2, 1])
            with col_exec:
                execute_btn = st.button("▶️ Выполнить запрос", key="execute_financial", type="primary", use_container_width=True)
            with col_stats:
                stats_btn = st.button("📈 Со статистикой", key="execute_with_stats_financial", use_container_width=True)
            
            # Обработка кнопок
            if execute_btn:
                execute_sql_query(generated_sql, result_key="financial_result", check_plan=True)
            elif stats_btn:
                # Информация о фактическом плане выполнения
                with st.expander("ℹ️ О фактическом плане выполнения", expanded=False):
                    st.markdown("""
                    **Фактический план выполнения (Actual Execution Plan)** показывает:
                    - Реальный план, который использовал Oracle при выполнении запроса
                    - Фактическое количество обработанных строк (A-Rows)
                    - Фактическое время выполнения операций
                    - Реальное использование буферов (Buffers)
                    
                    **Важно:** Этот план показывает, как запрос был выполнен, но не улучшает план для следующих выполнений.
                    Для улучшения плана нужно обновить статистику таблиц через `DBMS_STATS.GATHER_TABLE_STATS`.
                    """)
                
                st.info("💡 **Примечание:** Эта функция показывает фактический план выполнения запроса. Она НЕ собирает статистику таблиц для оптимизатора Oracle. Для улучшения плана выполнения используйте кнопку '📊 Собрать статистику' после выполнения запроса.")
                
                with st.spinner("Выполнение запроса со сбором статистики выполнения..."):
                    df, exec_time, stats_text = execute_sql_with_stats(generated_sql, result_key="financial_with_stats")
                
                if df is not None:
                    if exec_time:
                        st.metric("⏱️ Время выполнения", f"{exec_time:.2f} сек")
                    if stats_text:
                        st.markdown("**Фактический план выполнения (Actual Execution Plan):**")
                        st.code(stats_text, language="text")
                        st.info("💡 Этот план показывает, как запрос был выполнен. Для улучшения плана на будущее используйте кнопку '📊 Собрать статистику' ниже.")
                        
                        # Предложение собрать статистику для улучшения плана
                        tables = extract_tables_from_sql(generated_sql)
                        if tables:
                            st.markdown("---")
                            
                            # Кнопка для сбора статистики (только для основных таблиц)
                            main_tables = ['STECCOM_EXPENSES', 'SPNET_TRAFFIC', 'BM_CURRENCY_RATE', 
                                         'V_CONSOLIDATED_REPORT_WITH_BILLING', 'V_REVENUE_FROM_INVOICES', 'BM_INVOICE_ITEM', 'BM_PERIOD']
                            tables_to_gather = [t for t in tables if t in main_tables]
                            
                            if tables_to_gather:
                                # Проверяем актуальность статистики
                                stats_status = check_table_stats_freshness(tables_to_gather, max_days=30)
                                
                                # Показываем статус статистики
                                st.info("💡 **Статус статистики таблиц:**")
                                for table in tables_to_gather:
                                    if table in stats_status:
                                        is_fresh, days_ago, message = stats_status[table]
                                        if is_fresh:
                                            st.success(f"{table}: {message}")
                                        else:
                                            st.warning(f"{table}: {message}")
                                
                                # Показываем кнопку сбора статистики только если есть устаревшие таблицы
                                needs_refresh = any(not stats_status.get(t, (True, 0, ""))[0] for t in tables_to_gather if t in stats_status)
                                
                                if needs_refresh:
                                    st.markdown("**📊 Обновить статистику для улучшения плана:**")
                                    if st.button("📊 Собрать статистику для улучшения плана", key="gather_stats_financial"):
                                        st.warning("⚠️ **Внимание:** Сбор статистики может занять несколько минут для больших таблиц. Пожалуйста, дождитесь завершения.")
                                        for table in tables_to_gather:
                                            # Собираем статистику только для устаревших таблиц
                                            if table in stats_status and not stats_status[table][0]:
                                                with st.spinner(f"Сбор статистики для {table}... Это может занять несколько минут для больших таблиц."):
                                                    success, message = gather_table_stats(table)
                                                    if success:
                                                        st.success(message)
                                                    else:
                                                        st.warning(message)
                                else:
                                    st.success("✅ Статистика для всех таблиц актуальна. Дополнительный сбор статистики не требуется.")
                    
                    # Сохраняем результат для отображения ниже
                    st.session_state["financial_result"] = {
                        "sql": generated_sql,
                        "df": df,
                        "timestamp": pd.Timestamp.now()
                    }
        else:
            # Если LLM недоступен, показываем контекст и примеры
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                st.info("""
                💡 **Автоматическая генерация SQL через LLM недоступна**
                
                Для включения автоматической генерации SQL установите в `config.env`:
                - `OPENAI_API_KEY=your-api-key`
                - `OPENAI_API_BASE=https://api.proxyapi.ru/openai/v1` (опционально, для прокси)
                """)
            
            # Показываем примеры финансового анализа
            st.markdown("**Примеры запросов для финансового анализа:**")
            examples = [
                "Найди убыточных клиентов за октябрь",
                "Покажи динамику прибыльности клиентов по периодам",
                "Найди клиентов с ухудшением прибыльности",
                "Покажи клиентов с низкой маржой за октябрь",
                "Покажи структуру затрат и доходов по клиенту за октябрь"
            ]
            for i, example in enumerate(examples, 1):
                st.markdown(f"{i}. {example}")
            
            # Если есть похожие примеры, показываем их
            if context and context.get("examples"):
                st.markdown("**Рекомендуемые примеры:**")
                for i, example in enumerate(context["examples"][:3], 1):
                    st.markdown(f"{i}. {example['question']}")
                    st.code(example['sql'], language="sql")
                    
                    # Кнопка выполнения для каждого примера
                    if st.button(f"▶️ Выполнить пример {i}", key=f"execute_financial_example_{i}"):
                        execute_sql_query(example['sql'], result_key="financial_result")
    else:
        st.info("💡 Введите вопрос для финансового анализа и нажмите кнопку **📊 Сгенерировать SQL для анализа**")
    
    # Единое место для отображения результатов снизу
    st.markdown("---")
    st.subheader("📋 Результаты выполнения")
    
    # Проверяем результат финансового анализа
    if "financial_result" in st.session_state:
        result = st.session_state["financial_result"]
        if "df" in result and result["df"] is not None:
            df = result["df"]
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
                    file_name=f"financial_result_{result['timestamp'].strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    key=f"download_financial_result_final"
                )
                
                # Финансовый анализ результатов (только метрики, без дублирования таблиц)
                st.markdown("---")
                st.subheader("💡 Финансовый анализ результатов")
                
                # Проверяем наличие финансовых полей
                profit_cols = [col for col in df.columns if any(keyword in col.lower() for keyword in ['прибыль', 'profit', 'убыток', 'loss', 'маржа', 'margin'])]
                cost_cols = [col for col in df.columns if any(keyword in col.lower() for keyword in ['расход', 'expense', 'cost', 'затрат'])]
                revenue_cols = [col for col in df.columns if any(keyword in col.lower() for keyword in ['доход', 'revenue', 'выручк'])]
                
                if profit_cols or (cost_cols and revenue_cols):
                    # Показываем только метрики и статистику, без дублирования таблиц
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Анализ убыточных позиций (только количество, без таблицы)
                        if profit_cols:
                            profit_col = profit_cols[0]
                            negative_profit_count = len(df[df[profit_col] < 0]) if profit_col in df.columns else 0
                            if negative_profit_count > 0:
                                st.warning(f"⚠️ **Убыточных позиций: {negative_profit_count}**")
                            else:
                                st.success("✅ Убыточных позиций не обнаружено")
                        
                        # Анализ низкой маржи (только количество, без таблицы)
                        margin_cols = [col for col in df.columns if 'маржа' in col.lower() or 'margin' in col.lower()]
                        if margin_cols:
                            margin_col = margin_cols[0]
                            low_margin_count = len(df[df[margin_col] < 10]) if margin_col in df.columns else 0
                            if low_margin_count > 0:
                                st.info(f"ℹ️ **Позиций с низкой маржой (<10%): {low_margin_count}**")
                            else:
                                st.success("✅ Позиций с низкой маржой не обнаружено")
                    
                    with col2:
                        # Статистика по прибыли
                        if profit_cols:
                            profit_col = profit_cols[0]
                            if profit_col in df.columns:
                                total_profit = df[profit_col].sum()
                                avg_profit = df[profit_col].mean()
                                st.metric("Общая прибыль", f"{total_profit:,.2f} RUB")
                                st.metric("Средняя прибыль", f"{avg_profit:,.2f} RUB")
                    
                    # Пояснение результатов
                    st.markdown("---")
                    with st.expander("📖 Пояснение расчетов и показателей", expanded=True):
                        st.markdown("""
                        **Как считаются показатели:**
                        
                        1. **Прибыль (PROFIT_RUB)** = Доходы (REVENUE_RUB) - Расходы (EXPENSES_RUB)
                           - Если значение **отрицательное** → это **убыток** (расходы превышают доходы)
                           - Если значение **положительное** → это **прибыль**
                        
                        2. **Расходы (EXPENSES_RUB)** включают:
                           - Превышение трафика (CALCULATED_OVERAGE)
                           - Стоимость трафика из SPNet (SPNET_TOTAL_AMOUNT)
                           - Все сборы и комиссии (FEES_TOTAL)
                           - Конвертация из USD в RUB через курс из счетов-фактур
                        
                        3. **Доходы (REVENUE_RUB)** - сумма из счетов-фактур в рублях:
                           - SBD трафик превышения
                           - SBD абонплата
                           - SUSPEND абонплата
                           - Мониторинг и другие услуги
                        
                        4. **Маржа (MARGIN_PCT)** = (Прибыль / Доходы) × 100%
                           - Показывает процент прибыли от дохода
                           - Если прибыль отрицательная → маржа тоже отрицательная
                           - Маржа < 10% считается низкой
                        
                        5. **Себестоимость (COST_PCT)** = (Расходы / Доходы) × 100%
                           - Показывает процент расходов от дохода
                           - Если себестоимость > 100% → убыток
                        
                        **Пример:**
                        - Доходы: 100,000 RUB
                        - Расходы: 120,000 RUB
                        - **Прибыль: -20,000 RUB** (убыток 20,000 руб)
                        - **Маржа: -20%** (убыток составляет 20% от дохода)
                        - **Себестоимость: 120%** (расходы превышают доходы на 20%)
                        """)
        elif "error" in result:
            st.error(f"❌ Ошибка: {result['error']}")
            with st.expander("🔍 Детали ошибки", expanded=False):
                st.code(result.get("traceback", ""), language="python")
    else:
        st.info("💡 Результаты выполнения запросов будут отображаться здесь")


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


def explain_plan(sql: str, return_analysis: bool = False):
    """Выполнение EXPLAIN PLAN для SQL запроса и возврат плана выполнения
    
    Args:
        sql: SQL запрос для анализа
        return_analysis: Если True, возвращает также стоимость и предупреждения
    
    Returns:
        Если return_analysis=False: (plan_text, error) - для обратной совместимости
        Если return_analysis=True: (cost, plan_text, warnings, error) - расширенный анализ
    """
    try:
        conn = get_connection()
        if not conn:
            return None, None, [], "❌ Не удалось подключиться к базе данных"
        
        cursor = conn.cursor()
        
        # Очистка SQL запроса: удаляем точку с запятой в конце и лишние пробелы
        sql_clean = sql.strip().rstrip(';').strip()
        
        warnings = []
        
        # Выполнение EXPLAIN PLAN
        try:
            # Используем короткий STATEMENT_ID для избежания ошибок с длинными именами
            import uuid
            statement_id = uuid.uuid4().hex[:8].upper()  # Короткий ID (8 символов)
            
            # Сначала очищаем предыдущий план с нашим statement_id (если есть)
            try:
                cursor.execute(f"DELETE FROM PLAN_TABLE WHERE STATEMENT_ID = '{statement_id}'")
            except:
                pass  # Может не быть прав или таблицы
            
            # Выполняем EXPLAIN PLAN с указанием STATEMENT_ID
            explain_sql = f"EXPLAIN PLAN SET STATEMENT_ID = '{statement_id}' FOR {sql_clean}"
            cursor.execute(explain_sql)
            
            # Получаем план с форматом ALL для анализа стоимости
            plan_text = None
            plan_data = None
            
            for format_type in ['ALL', 'TYPICAL', 'BASIC', 'SERIAL']:
                try:
                    plan_query = f"""
                        SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY('PLAN_TABLE', '{statement_id}', '{format_type}'))
                    """
                    cursor.execute(plan_query)
                    plan_rows = cursor.fetchall()
                    if plan_rows:
                        plan_text = "\n".join([row[0] for row in plan_rows])
                        plan_data = plan_rows
                        break
                except Exception as format_error:
                    continue
            
            # Если не получилось с statement_id, пробуем без него
            if not plan_text:
                try:
                    cursor.execute("""
                        SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY('PLAN_TABLE', NULL, 'ALL'))
                    """)
                    plan_rows = cursor.fetchall()
                    if plan_rows:
                        plan_text = "\n".join([row[0] for row in plan_rows])
                        plan_data = plan_rows
                except:
                    pass
            
            # Извлекаем стоимость из плана
            cost = None
            if plan_data:
                # Ищем строку с Cost в плане (обычно в первой строке или в строке с "Plan hash value")
                for row in plan_data:
                    row_text = row[0] if isinstance(row, tuple) else str(row)
                    # Ищем паттерн "Cost (%d)" или "Cost=(%d)"
                    import re
                    cost_match = re.search(r'Cost\s*[=:]\s*(\d+)', row_text, re.IGNORECASE)
                    if cost_match:
                        cost = int(cost_match.group(1))
                        break
                    
                    # Альтернативный паттерн: "cost (%d)"
                    cost_match = re.search(r'cost\s*\((\d+)\)', row_text, re.IGNORECASE)
                    if cost_match:
                        cost = int(cost_match.group(1))
                        break
                
                # Если не нашли в тексте, пробуем получить из PLAN_TABLE напрямую
                if cost is None:
                    try:
                        cursor.execute(f"""
                            SELECT COST FROM PLAN_TABLE 
                            WHERE STATEMENT_ID = '{statement_id}' 
                            AND COST IS NOT NULL 
                            ORDER BY COST DESC 
                            FETCH FIRST 1 ROW ONLY
                        """)
                        cost_row = cursor.fetchone()
                        if cost_row and cost_row[0]:
                            cost = int(cost_row[0])
                    except:
                        pass
            
            # Анализ плана на потенциальные проблемы
            if plan_text:
                plan_lower = plan_text.lower()
                
                # Проверка на TABLE ACCESS FULL (полное сканирование таблиц)
                full_scan_count = plan_lower.count('table access full')
                if full_scan_count > 0:
                    warnings.append(f"⚠️ Обнаружено {full_scan_count} полных сканирований таблиц (TABLE ACCESS FULL) - запрос может быть медленным")
                
                # Проверка на CARTESIAN JOIN (декартово произведение)
                if 'cartesian' in plan_lower:
                    warnings.append("🚨 ОБНАРУЖЕНО ДЕКАРТОВО ПРОИЗВЕДЕНИЕ (CARTESIAN JOIN) - запрос может выполняться очень долго!")
                
                # Проверка на высокую стоимость
                if cost:
                    if cost > 1000000:
                        warnings.append(f"🚨 ОЧЕНЬ ВЫСОКАЯ СТОИМОСТЬ ({cost:,}) - запрос может выполняться несколько часов или дней!")
                    elif cost > 100000:
                        warnings.append(f"⚠️ Высокая стоимость ({cost:,}) - запрос может выполняться долго (минуты или часы)")
                    elif cost > 10000:
                        warnings.append(f"ℹ️ Средняя стоимость ({cost:,}) - запрос может выполняться несколько секунд или минут")
            
            # Очищаем план после получения
            try:
                cursor.execute(f"DELETE FROM PLAN_TABLE WHERE STATEMENT_ID = '{statement_id}'")
            except:
                pass
            
            cursor.close()
            conn.close()
            
            # Возвращаем результат в зависимости от режима
            if return_analysis:
                return cost, plan_text, warnings, None
            else:
                # Обратная совместимость: возвращаем (plan_text, error)
                if plan_text:
                    # Добавляем предупреждения к плану, если есть
                    if warnings:
                        plan_text_with_warnings = "\n".join(warnings) + "\n\n" + plan_text
                    else:
                        plan_text_with_warnings = plan_text
                    return plan_text_with_warnings, None
                else:
                    return None, "Не удалось получить план выполнения. Возможно, запрос слишком сложный для EXPLAIN PLAN."
            
        except Exception as e:
            error_msg = str(e)
            error_code = None
            
            # Извлекаем код ошибки Oracle
            if "ORA-" in error_msg:
                import re
                match = re.search(r'ORA-(\d+)', error_msg)
                if match:
                    error_code = match.group(1)
            
            # Если ошибка связана с длинными именами объектов (ORA-12899) или другими проблемами PLAN_TABLE
            if error_code == "12899" or "value too large" in error_msg.lower() or "object_name" in error_msg.lower():
                cursor.close()
                conn.close()
                error_message = (
                    f"⚠️ EXPLAIN PLAN не может обработать этот запрос из-за длинных имен объектов в PLAN_TABLE.\n\n"
                    f"**Рекомендация:** Используйте кнопку **📈 Со статистикой** для выполнения запроса с анализом производительности.\n"
                    f"Этот метод использует DBMS_XPLAN.DISPLAY_CURSOR и работает для любых запросов, включая сложные CTE.\n\n"
                    f"**Альтернатива:** Упростите запрос или выполните его части отдельно для анализа."
                )
                if return_analysis:
                    return None, None, [], error_message
                else:
                    return None, error_message
            
            # Если ошибка связана с синтаксисом или идентификаторами
            if "invalid identifier" in error_msg.lower() or "ora-00904" in error_msg.lower():
                try:
                    # Пробуем обернуть запрос в подзапрос
                    wrapped_sql = f"SELECT * FROM ({sql_clean})"
                    explain_sql = f"EXPLAIN PLAN FOR {wrapped_sql}"
                    cursor.execute(explain_sql)
                    
                    cursor.execute("""
                        SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY('PLAN_TABLE', NULL, 'SERIAL'))
                    """)
                    plan_rows = cursor.fetchall()
                    if plan_rows:
                        plan_text = "\n".join([row[0] for row in plan_rows])
                        cursor.close()
                        conn.close()
                        if return_analysis:
                            return None, plan_text, [], None
                        else:
                            return plan_text, None
                except:
                    pass
            
            cursor.close()
            conn.close()
            error_message = (
                f"Ошибка при выполнении EXPLAIN PLAN: {error_msg}\n\n"
                f"💡 **Совет:** Используйте кнопку **📈 Со статистикой** для анализа производительности запроса.\n"
                f"Этот метод работает для любых запросов, включая сложные CTE и подзапросы."
            )
            if return_analysis:
                return None, None, [], error_message
            else:
                return None, error_message
            
    except Exception as e:
        error_message = f"Ошибка подключения: {str(e)}"
        if return_analysis:
            return None, None, [], error_message
        else:
            return None, error_message


def get_table_stats_date(table_name: str, schema: str = None):
    """Получение даты последнего сбора статистики для таблицы"""
    try:
        conn = get_connection()
        if not conn:
            return None
        
        cursor = conn.cursor()
        
        # Определяем схему (по умолчанию используем текущего пользователя)
        if not schema:
            schema = os.getenv('ORACLE_USER', 'BILLING7')
        
        try:
            # Получаем дату последнего сбора статистики
            cursor.execute(f"""
                SELECT LAST_ANALYZED 
                FROM ALL_TAB_STATISTICS 
                WHERE OWNER = UPPER('{schema}') 
                  AND TABLE_NAME = UPPER('{table_name}')
                  AND PARTITION_NAME IS NULL
                ORDER BY LAST_ANALYZED DESC
                FETCH FIRST 1 ROW ONLY
            """)
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            if result and result[0]:
                return result[0]
            return None
        except Exception as e:
            cursor.close()
            conn.close()
            return None
            
    except Exception as e:
        return None


def gather_table_stats(table_name: str, schema: str = None):
    """Сбор статистики для таблицы через DBMS_STATS"""
    try:
        conn = get_connection()
        if not conn:
            return False, "❌ Не удалось подключиться к базе данных"
        
        cursor = conn.cursor()
        
        # Определяем схему (по умолчанию используем текущего пользователя)
        if not schema:
            schema = os.getenv('ORACLE_USER', 'BILLING7')
        
        # Проверяем дату последнего сбора статистики
        last_analyzed = get_table_stats_date(table_name, schema)
        stats_info = ""
        if last_analyzed:
            from datetime import datetime
            if isinstance(last_analyzed, datetime):
                days_ago = (datetime.now() - last_analyzed).days
                stats_info = f" (последний сбор: {days_ago} дн. назад)"
        
        try:
            # Собираем статистику для таблицы
            cursor.execute(f"""
                BEGIN
                    DBMS_STATS.GATHER_TABLE_STATS(
                        ownname => '{schema}',
                        tabname => '{table_name}',
                        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
                        method_opt => 'FOR ALL COLUMNS SIZE AUTO',
                        cascade => TRUE
                    );
                END;
            """)
            conn.commit()
            
            cursor.close()
            conn.close()
            
            return True, f"✅ Статистика для таблицы {schema}.{table_name} успешно собрана{stats_info}"
        except Exception as e:
            cursor.close()
            conn.close()
            return False, f"❌ Ошибка при сборе статистики: {str(e)}"
            
    except Exception as e:
        return False, f"❌ Ошибка подключения: {str(e)}"


def check_table_stats_freshness(tables: list, schema: str = None, max_days: int = 30):
    """Проверка актуальности статистики для списка таблиц
    
    Args:
        tables: список имен таблиц
        schema: схема (по умолчанию из ORACLE_USER)
        max_days: максимальное количество дней с последнего сбора статистики
        
    Returns:
        dict: {table_name: (is_fresh, days_ago, message)}
    """
    if not schema:
        schema = os.getenv('ORACLE_USER', 'BILLING7')
    
    stats_status = {}
    for table in tables:
        last_analyzed = get_table_stats_date(table, schema)
        if last_analyzed:
            from datetime import datetime
            if isinstance(last_analyzed, datetime):
                days_ago = (datetime.now() - last_analyzed).days
                is_fresh = days_ago <= max_days
                if is_fresh:
                    message = f"✅ Статистика актуальна ({days_ago} дн. назад)"
                else:
                    message = f"⚠️ Статистика устарела ({days_ago} дн. назад, рекомендуется обновить)"
                stats_status[table] = (is_fresh, days_ago, message)
            else:
                stats_status[table] = (True, 0, "✅ Статистика собрана")
        else:
            stats_status[table] = (False, None, "❌ Статистика не найдена")
    
    return stats_status


def extract_tables_from_sql(sql: str):
    """Извлечение имен таблиц из SQL запроса (упрощенная версия)"""
    import re
    
    # Удаляем комментарии
    sql_clean = re.sub(r'--.*?$', '', sql, flags=re.MULTILINE)
    sql_clean = re.sub(r'/\*.*?\*/', '', sql_clean, flags=re.DOTALL)
    
    # Ищем FROM и JOIN
    tables = set()
    
    # Паттерны для поиска таблиц
    patterns = [
        r'FROM\s+([A-Z_][A-Z0-9_]*)',  # FROM TABLE_NAME
        r'JOIN\s+([A-Z_][A-Z0-9_]*)',  # JOIN TABLE_NAME
        r'INTO\s+([A-Z_][A-Z0-9_]*)',  # INSERT INTO TABLE_NAME
        r'UPDATE\s+([A-Z_][A-Z0-9_]*)',  # UPDATE TABLE_NAME
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, sql_clean, re.IGNORECASE)
        tables.update(matches)
    
    # Фильтруем только реальные таблицы (исключаем ключевые слова)
    keywords = {'SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'UNION', 'INTERSECT', 'EXCEPT'}
    tables = {t.upper() for t in tables if t.upper() not in keywords}
    
    return list(tables)


def execute_sql_with_stats(sql: str, result_key: str = "sql_result"):
    """Выполнение SQL запроса с сбором статистики выполнения"""
    try:
        conn = get_connection()
        if not conn:
            st.error("❌ Не удалось подключиться к базе данных. Проверьте настройки Oracle в config.env")
            return
        
        cursor = conn.cursor()
        
        # Очистка SQL запроса: удаляем точку с запятой в конце и лишние пробелы
        sql_clean = sql.strip().rstrip(';').strip()
        
        # Включаем сбор статистики
        try:
            cursor.execute("ALTER SESSION SET STATISTICS_LEVEL = ALL")
            cursor.execute("ALTER SESSION SET TIMED_STATISTICS = TRUE")
        except:
            pass  # Может не хватить прав
        
        # Выполнение запроса с измерением времени
        start_time = time.time()
        
        df = pd.read_sql(sql_clean, conn)
        
        execution_time = time.time() - start_time
        
        # Получаем статистику выполнения
        stats_sql = """
            SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR(NULL, NULL, 'ALLSTATS LAST'))
        """
        stats_text = None
        try:
            cursor.execute(stats_sql)
            stats_rows = cursor.fetchall()
            if stats_rows:
                stats_text = "\n".join([row[0] for row in stats_rows])
        except:
            pass  # Может не быть статистики
        
        cursor.close()
        conn.close()
        
        # Сохранение результата в session_state
        st.session_state[result_key] = {
            "sql": sql_clean,
            "df": df,
            "timestamp": pd.Timestamp.now(),
            "execution_time": execution_time,
            "stats": stats_text
        }
        
        return df, execution_time, stats_text
        
    except Exception as e:
        error_msg = str(e)
        import traceback
        traceback_str = traceback.format_exc()
        
        # Сохранение ошибки в session_state (без отображения)
        st.session_state[result_key] = {
            "sql": sql_clean if 'sql_clean' in locals() else sql.strip().rstrip(';').strip(),
            "error": error_msg,
            "traceback": traceback_str
        }
        return None, None, None


def execute_sql_query(sql: str, result_key: str = "sql_result", check_plan: bool = True):
    """Выполнение SQL запроса в Oracle и сохранение результата в session_state (без отображения)
    
    Args:
        sql: SQL запрос для выполнения
        result_key: Ключ для сохранения результата в session_state
        check_plan: Проверять ли план выполнения перед выполнением запроса
    """
    try:
        conn = get_connection()
        if not conn:
            st.session_state[result_key] = {
                "sql": sql.strip().rstrip(';').strip(),
                "error": "❌ Не удалось подключиться к базе данных. Проверьте настройки Oracle в config.env",
                "traceback": ""
            }
            return
        
        # Очистка SQL запроса: удаляем точку с запятой в конце и лишние пробелы
        sql_clean = sql.strip().rstrip(';').strip()
        
        # Проверка плана выполнения перед выполнением запроса
        if check_plan:
            cost, plan_text, warnings, plan_error = explain_plan(sql_clean, return_analysis=True)
            
            # Если есть предупреждения о высокой стоимости, показываем их
            if warnings:
                # Проверяем критичность предупреждений
                critical_warnings = [w for w in warnings if '🚨' in w or 'ОЧЕНЬ ВЫСОКАЯ' in w]
                high_warnings = [w for w in warnings if '⚠️' in w and '🚨' not in w]
                
                if critical_warnings:
                    # Критические предупреждения - требуем подтверждения
                    st.error("🚨 **КРИТИЧЕСКОЕ ПРЕДУПРЕЖДЕНИЕ:**")
                    for warning in critical_warnings:
                        st.error(warning)
                    if high_warnings:
                        for warning in high_warnings:
                            st.warning(warning)
                    
                    st.markdown("---")
                    st.warning("**Запрос может выполняться очень долго (часы или дни) и блокировать систему!**")
                    
                    # Проверяем, было ли уже подтверждение для этого запроса
                    confirm_key = f"confirm_execute_{result_key}"
                    if confirm_key not in st.session_state:
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button("✅ Продолжить выполнение", key=f"confirm_{result_key}", type="primary"):
                                st.session_state[confirm_key] = True
                                st.rerun()
                        with col2:
                            if st.button("❌ Отменить выполнение", key=f"cancel_{result_key}"):
                                st.session_state[result_key] = {
                                    "sql": sql_clean,
                                    "error": "Выполнение отменено пользователем из-за высокой стоимости запроса",
                                    "traceback": "",
                                    "plan_cost": cost,
                                    "warnings": warnings
                                }
                                conn.close()
                                return
                        conn.close()
                        return  # Ждем подтверждения
                    else:
                        # Подтверждение получено, продолжаем
                        st.info("✅ Выполнение подтверждено пользователем")
                        st.markdown("---")
                elif high_warnings:
                    # Высокие предупреждения - показываем, но не блокируем
                    for warning in high_warnings:
                        st.warning(warning)
            
            # Сохраняем информацию о плане в session_state
            plan_info = {
                "cost": cost,
                "warnings": warnings,
                "plan_text": plan_text
            }
        else:
            plan_info = None
        
        # Выполнение запроса
        with st.spinner("Выполнение SQL запроса..."):
            df = pd.read_sql(sql_clean, conn)
            conn.close()
        
        # Сохранение результата в session_state (без отображения)
        result_data = {
            "sql": sql_clean,
            "df": df,
            "timestamp": pd.Timestamp.now()
        }
        if plan_info:
            result_data["plan_info"] = plan_info
        
        st.session_state[result_key] = result_data
        
    except Exception as e:
        error_msg = str(e)
        import traceback
        traceback_str = traceback.format_exc()
        
        # Сохранение ошибки в session_state (без отображения)
        error_data = {
            "sql": sql_clean if 'sql_clean' in locals() else sql.strip().rstrip(';').strip(),
            "error": error_msg,
            "traceback": traceback_str
        }
        if 'plan_info' in locals():
            error_data["plan_info"] = plan_info
        
        st.session_state[result_key] = error_data

