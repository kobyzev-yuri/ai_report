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
import io

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
                            # Очищаем ошибку, если SQL успешно сгенерирован
                            if "sql_generation_error" in st.session_state:
                                del st.session_state["sql_generation_error"]
                    except Exception as e:
                        error_msg = str(e)
                        st.session_state["sql_generation_error"] = error_msg
                        st.session_state.last_generated_sql = None
                        st.session_state.last_generated_question = None
        
        # Если SQL сгенерирован, показываем и автоматически выполняем
        if generated_sql:
            st.success("✅ SQL запрос сгенерирован!")
            st.markdown("**Сгенерированный SQL:**")
            st.code(generated_sql, language="sql")
            
            # Автоматически выполняем запрос
            with st.spinner("Выполнение запроса..."):
                execute_sql_query(generated_sql, result_key="sql_result", check_plan=True)
            
            # Кнопка для выполнения со статистикой (опционально)
            if st.button("📈 Выполнить со статистикой", key="execute_with_stats_generated", use_container_width=True):
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
            # Если SQL не сгенерирован, показываем ошибку и возможность ввода SQL вручную
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                st.error("""
                ❌ **Автоматическая генерация SQL через LLM недоступна**
                
                Для включения автоматической генерации SQL установите в `config.env`:
                - `OPENAI_API_KEY=your-api-key`
                - `OPENAI_API_BASE=https://api.proxyapi.ru/openai/v1` (опционально, для прокси)
                """)
            else:
                error_msg = st.session_state.get("sql_generation_error", "Не удалось сгенерировать SQL запрос")
                st.error(f"❌ {error_msg}")
            
            # Предлагаем ввести SQL вручную
            st.markdown("---")
            st.markdown("**💡 Альтернатива: введите SQL запрос вручную**")
            manual_sql = st.text_area(
                "Введите SQL запрос:",
                height=150,
                key="manual_sql_input",
                help="Вы можете ввести SQL запрос вручную, если автоматическая генерация не работает"
            )
            
            if st.button("▶️ Выполнить SQL вручную", key="execute_manual_sql", type="primary", use_container_width=True):
                if manual_sql.strip():
                    with st.spinner("Выполнение SQL запроса..."):
                        execute_sql_query(manual_sql.strip(), result_key="sql_result", check_plan=True)
                    st.rerun()
                else:
                    st.warning("⚠️ Введите SQL запрос перед выполнением")
            
            # Показываем пример SQL для запроса пользователя (если возможно определить тип запроса)
            user_question = question if 'question' in locals() else st.session_state.get("assistant_question", "")
            if user_question:
                question_lower = user_question.lower()
                if any(word in question_lower for word in ["клиент", "код 1с", "финансов", "реквизит", "счет", "фактур"]):
                    with st.expander("💡 Пример SQL запроса для получения клиентов с кодом 1С и финансовыми реквизитами"):
                        example_sql = """
-- Получение клиентов с кодом 1С и финансовыми реквизитами из последнего счета-фактуры
WITH last_invoices AS (
    SELECT 
        inv.CUSTOMER_ID,
        MAX(inv.MOMENT) AS LAST_INVOICE_DATE
    FROM BM_INVOICE inv
    WHERE inv.NOT_EXPORT = 0  -- Только экспортируемые счета
    GROUP BY inv.CUSTOMER_ID
)
SELECT DISTINCT
    c.CUSTOMER_ID,
    oi.EXT_ID AS CODE_1C,
    -- Название клиента (организация или ФИО)
    COALESCE(
        MAX(CASE WHEN cd.MNEMONIC = 'description' AND cc.CONTACT_DICT_ID = 23 THEN cc.VALUE END),
        TRIM(
            NVL(MAX(CASE WHEN cd.MNEMONIC = 'last_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' ||
            NVL(MAX(CASE WHEN cd.MNEMONIC = 'first_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' ||
            NVL(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '')
        )
    ) AS CUSTOMER_NAME,
    -- Финансовые реквизиты из последнего счета-фактуры
    MAX(inv.BUYER_NAME) AS BUYER_NAME,
    MAX(inv.BUYER_INN) AS BUYER_INN,
    MAX(inv.BUYER_KPP) AS BUYER_KPP,
    MAX(inv.BUYER_ADDRESS) AS BUYER_ADDRESS,
    MAX(inv.MOMENT) AS LAST_INVOICE_DATE
FROM last_invoices li
JOIN BM_INVOICE inv 
    ON inv.CUSTOMER_ID = li.CUSTOMER_ID 
    AND inv.MOMENT = li.LAST_INVOICE_DATE
    AND inv.NOT_EXPORT = 0
JOIN CUSTOMERS c ON c.CUSTOMER_ID = inv.CUSTOMER_ID
LEFT JOIN OUTER_IDS oi 
    ON oi.ID = c.CUSTOMER_ID 
    AND UPPER(TRIM(oi.TBL)) = 'CUSTOMERS'
LEFT JOIN BM_CUSTOMER_CONTACT cc 
    ON cc.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN BM_CONTACT_DICT cd 
    ON cd.CONTACT_DICT_ID = cc.CONTACT_DICT_ID
WHERE oi.EXT_ID IS NOT NULL  -- Только клиенты с кодом 1С
GROUP BY c.CUSTOMER_ID, oi.EXT_ID
ORDER BY CUSTOMER_NAME
                        """
                        st.code(example_sql.strip(), language="sql")
                        if st.button("📋 Скопировать пример", key="copy_example_sql"):
                            st.session_state["manual_sql_input"] = example_sql.strip()
                            st.rerun()
    
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
                    # Маскирование чувствительных данных перед выводом
                    df_display = mask_sensitive_data(result["df"], result.get("sql", ""))
                    
                    st.success(f"✅ Запрос выполнен успешно. Найдено записей: {len(result['df'])}")
                    st.dataframe(df_display, use_container_width=True, height=400)
                    
                    # Кнопки экспорта (также с маскированными данными)
                    col1, col2 = st.columns(2)
                    with col1:
                        csv = df_display.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 Скачать CSV",
                            data=csv,
                            file_name=f"query_result_{result['timestamp'].strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv",
                            key=f"download_{result_key}_csv"
                        )
                    with col2:
                        excel_output = io.BytesIO()
                        with pd.ExcelWriter(excel_output, engine='openpyxl') as writer:
                            df_display.to_excel(writer, index=False, sheet_name='Query Result')
                        excel_data = excel_output.getvalue()
                        st.download_button(
                            label="📊 Скачать Excel",
                            data=excel_data,
                            file_name=f"query_result_{result['timestamp'].strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"download_{result_key}_excel"
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


# УДАЛЕНО: show_financial_analysis_tab - дублировала функциональность show_assistant_tab


def mask_sensitive_data(df: pd.DataFrame, sql: str = None) -> pd.DataFrame:
    """
    Маскирование чувствительных данных в DataFrame:
    - Колонки login и password (из BM_STAFF)
    - Данные из SERVICES где TYPE_ID = 30 (доступ к веб серверу личного кабинета)
    
    Args:
        df: DataFrame с данными
        sql: SQL запрос (опционально, для анализа)
    
    Returns:
        DataFrame с замаскированными данными
    """
    if df is None or df.empty:
        return df
    
    df_masked = df.copy()
    masked_columns = []
    masked_rows = 0
    
    # Маскирование колонок login и password (case-insensitive)
    sensitive_column_names = ['login', 'password']
    for col in df_masked.columns:
        col_lower = col.lower()
        if any(sensitive_name in col_lower for sensitive_name in sensitive_column_names):
            df_masked[col] = '***СКРЫТО***'
            masked_columns.append(col)
    
    # Маскирование данных из SERVICES где TYPE_ID = 30
    # Проверяем наличие колонки TYPE_ID
    type_id_col = None
    for col in df_masked.columns:
        if col.upper() == 'TYPE_ID':
            type_id_col = col
            break
    
    if type_id_col is not None:
        # Находим строки с TYPE_ID = 30
        mask_type_30 = df_masked[type_id_col] == 30
        if mask_type_30.any():
            # Маскируем все колонки для этих строк
            for col in df_masked.columns:
                if col != type_id_col:  # TYPE_ID оставляем видимым для понимания
                    df_masked.loc[mask_type_30, col] = '***СКРЫТО (TYPE_ID=30)***'
            masked_rows = mask_type_30.sum()
    
    # Показываем предупреждение, если были замаскированы данные
    if masked_columns or masked_rows > 0:
        warning_parts = []
        if masked_columns:
            warning_parts.append(f"колонки: {', '.join(masked_columns)}")
        if masked_rows > 0:
            warning_parts.append(f"{masked_rows} строк(и) с TYPE_ID=30")
        
        if warning_parts:
            st.warning(f"🔒 Чувствительные данные замаскированы: {', '.join(warning_parts)}")
    
    return df_masked


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
                # Примечание: стоимость из EXPLAIN PLAN не всегда коррелирует с реальным временем выполнения
                # При наличии индексов запросы с высокой стоимостью могут выполняться быстро
                if cost:
                    if cost > 10000000:  # > 10M - действительно критично
                        warnings.append(f"🚨 ОЧЕНЬ ВЫСОКАЯ СТОИМОСТЬ ({cost:,}) - запрос может выполняться несколько часов или дней!")
                    elif cost > 1000000:  # > 1M - высокая стоимость, но может быть быстрым с индексами
                        warnings.append(f"⚠️ Высокая стоимость ({cost:,}) - запрос может выполняться долго. Рекомендуется проверить наличие индексов в плане выполнения")
                    elif cost > 500000:  # > 500K - средняя-высокая стоимость
                        warnings.append(f"ℹ️ Повышенная стоимость ({cost:,}) - запрос может выполняться несколько секунд или минут. При наличии индексов обычно выполняется быстро")
            
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

