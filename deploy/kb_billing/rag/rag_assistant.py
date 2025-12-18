#!/usr/bin/env python3
"""
RAG ассистент для генерации SQL запросов и поиска информации по KB_billing
"""
import os
# Исправление проблемы с protobuf - должно быть ДО импорта transformers
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

from typing import List, Dict, Any, Optional
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue
from sentence_transformers import SentenceTransformer
import logging

# Импорт конфигурации sql4A
try:
    from kb_billing.rag.config_sql4a import SQL4AConfig
except ImportError:
    # Fallback если модуль не найден
    class SQL4AConfig:
        EMBEDDING_MODEL = "intfloat/multilingual-e5-base"
        EMBEDDING_DIMENSION = 768
        QDRANT_HOST = "localhost"
        QDRANT_PORT = 6333
        QDRANT_COLLECTION = "kb_billing"
        DEFAULT_SEARCH_LIMIT = 5
        SIMILARITY_THRESHOLD = 0.7

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RAGAssistant:
    """RAG ассистент для работы с KB_billing (совместим с sql4A)"""
    
    def __init__(
        self,
        qdrant_host: Optional[str] = None,
        qdrant_port: Optional[int] = None,
        collection_name: Optional[str] = None,
        embedding_model: Optional[str] = None
    ):
        """
        Инициализация RAG ассистента (использует настройки sql4A)
        
        Args:
            qdrant_host: Хост Qdrant сервера (по умолчанию из SQL4AConfig)
            qdrant_port: Порт Qdrant сервера (по умолчанию из SQL4AConfig)
            collection_name: Имя коллекции в Qdrant (по умолчанию из SQL4AConfig)
            embedding_model: Модель для генерации эмбеддингов (по умолчанию из SQL4AConfig)
        """
        # Используем настройки из sql4A конфигурации
        self.qdrant_host = qdrant_host or SQL4AConfig.QDRANT_HOST
        self.qdrant_port = qdrant_port or SQL4AConfig.QDRANT_PORT
        self.client = QdrantClient(
            host=self.qdrant_host,
            port=self.qdrant_port
        )
        self.collection_name = collection_name or SQL4AConfig.QDRANT_COLLECTION
        self.embedding_model = embedding_model or SQL4AConfig.EMBEDDING_MODEL
        self.model = SentenceTransformer(self.embedding_model)
        
        logger.info(f"Инициализация RAGAssistant:")
        logger.info(f"  - Qdrant: {self.qdrant_host}:{self.qdrant_port}")
        logger.info(f"  - Коллекция: {self.collection_name}")
        logger.info(f"  - Модель эмбеддингов: {self.embedding_model}")
        
    def search_similar_examples(
        self,
        question: str,
        category: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Поиск похожих Q/A примеров
        
        Args:
            question: Вопрос финансиста
            category: Категория для фильтрации (опционально)
            limit: Количество результатов (по умолчанию из SQL4AConfig)
        
        Returns:
            Список похожих примеров с SQL запросами
        """
        # Используем настройки из sql4A
        limit = limit or SQL4AConfig.DEFAULT_SEARCH_LIMIT
        
        # Генерация эмбеддинга для вопроса (как в sql4A - с нормализацией)
        query_embedding = self.model.encode(
            question, 
            normalize_embeddings=SQL4AConfig.NORMALIZE_EMBEDDINGS
        ).tolist()
        
        # Построение фильтра
        filter_conditions = [
            FieldCondition(key="type", match=MatchValue(value="qa_example"))
        ]
        
        if category:
            filter_conditions.append(
                FieldCondition(key="category", match=MatchValue(value=category))
            )
        
        query_filter = Filter(must=filter_conditions) if filter_conditions else None
        
        # Поиск в Qdrant
        try:
            results = self.client.search(
                collection_name=self.collection_name,
                query_vector=query_embedding,
                query_filter=query_filter,
                limit=limit
            )
            
            # Форматирование результатов
            examples = []
            for result in results:
                examples.append({
                    "question": result.payload.get("question", ""),
                    "sql": result.payload.get("sql", ""),
                    "category": result.payload.get("category", ""),
                    "complexity": result.payload.get("complexity", 1),
                    "similarity": result.score,
                    "context": result.payload.get("context", "")
                })
            
            return examples
        except Exception as e:
            logger.error(f"Ошибка при поиске примеров: {e}")
            return []
    
    def search_table_info(
        self,
        table_name: str,
        info_type: str = "documentation"
    ) -> Optional[Dict[str, Any]]:
        """
        Поиск информации о таблице
        
        Args:
            table_name: Имя таблицы
            info_type: Тип информации ('ddl', 'documentation', 'both')
        
        Returns:
            Информация о таблице
        """
        try:
            # Поиск DDL
            ddl_result = None
            if info_type in ["ddl", "both"]:
                ddl_filter = Filter(
                    must=[
                        FieldCondition(key="type", match=MatchValue(value="ddl")),
                        FieldCondition(key="table_name", match=MatchValue(value=table_name))
                    ]
                )
                ddl_results = self.client.scroll(
                    collection_name=self.collection_name,
                    scroll_filter=ddl_filter,
                    limit=1
                )
                if ddl_results[0]:
                    ddl_result = ddl_results[0][0].payload if ddl_results[0] else None
            
            # Поиск документации
            doc_result = None
            if info_type in ["documentation", "both"]:
                doc_filter = Filter(
                    must=[
                        FieldCondition(key="type", match=MatchValue(value="documentation")),
                        FieldCondition(key="table_name", match=MatchValue(value=table_name))
                    ]
                )
                doc_results = self.client.scroll(
                    collection_name=self.collection_name,
                    scroll_filter=doc_filter,
                    limit=1
                )
                if doc_results[0]:
                    doc_result = doc_results[0][0].payload if doc_results[0] else None
            
            result = {}
            if ddl_result:
                result["ddl"] = ddl_result.get("content", "")
            if doc_result:
                result["documentation"] = doc_result.get("content", "")
                result["key_columns"] = doc_result.get("key_columns", {})
                result["business_rules"] = doc_result.get("business_rules", [])
                result["relationships"] = doc_result.get("relationships", [])
            
            return result if result else None
        except Exception as e:
            logger.error(f"Ошибка при поиске информации о таблице: {e}")
            return None
    
    def search_semantic(
        self,
        query: str,
        content_type: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Семантический поиск по KB
        
        Args:
            query: Поисковый запрос
            content_type: Тип контента для фильтрации ('qa_example', 'documentation', 'ddl', 'view')
            limit: Количество результатов (по умолчанию из SQL4AConfig)
        
        Returns:
            Список найденных документов
        """
        # Используем настройки из sql4A
        limit = limit or SQL4AConfig.DEFAULT_SEARCH_LIMIT
        
        # Генерация эмбеддинга (как в sql4A - с нормализацией)
        query_embedding = self.model.encode(
            query, 
            normalize_embeddings=SQL4AConfig.NORMALIZE_EMBEDDINGS
        ).tolist()
        
        # Построение фильтра
        filter_conditions = []
        if content_type:
            filter_conditions.append(
                FieldCondition(key="type", match=MatchValue(value=content_type))
            )
        
        query_filter = Filter(must=filter_conditions) if filter_conditions else None
        
        # Поиск
        try:
            results = self.client.search(
                collection_name=self.collection_name,
                query_vector=query_embedding,
                query_filter=query_filter,
                limit=limit
            )
            
            # Форматирование результатов
            documents = []
            for result in results:
                doc = {
                    "type": result.payload.get("type", ""),
                    "content": result.payload.get("content", ""),
                    "similarity": result.score
                }
                
                # Добавляем специфичные поля в зависимости от типа
                if doc["type"] == "qa_example":
                    doc["question"] = result.payload.get("question", "")
                    doc["sql"] = result.payload.get("sql", "")
                    doc["category"] = result.payload.get("category", "")
                elif doc["type"] == "documentation":
                    doc["table_name"] = result.payload.get("table_name", "")
                    doc["description"] = result.payload.get("description", "")
                elif doc["type"] == "ddl":
                    doc["table_name"] = result.payload.get("table_name", "")
                
                documents.append(doc)
            
            return documents
        except Exception as e:
            logger.error(f"Ошибка при семантическом поиске: {e}")
            return []
    
    def get_context_for_sql_generation(
        self,
        question: str,
        max_examples: int = 5,
        include_tables: bool = True
    ) -> Dict[str, Any]:
        """
        Получение контекста для генерации SQL
        
        Args:
            question: Вопрос финансиста
            max_examples: Максимальное количество примеров
            include_tables: Включать ли информацию о таблицах
        
        Returns:
            Контекст для генерации SQL
        """
        # Поиск похожих примеров
        examples = self.search_similar_examples(question, limit=max_examples)
        
        # Извлечение имен таблиц из примеров
        table_names = set()
        for example in examples:
            # Простое извлечение из SQL
            sql = example.get("sql", "")
            import re
            matches = re.findall(r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
            table_names.update(matches)
            matches = re.findall(r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.IGNORECASE)
            table_names.update(matches)
        
        # Получение информации о таблицах
        tables_info = {}
        if include_tables and table_names:
            for table_name in table_names:
                info = self.search_table_info(table_name, info_type="both")
                if info:
                    tables_info[table_name] = info
        
        return {
            "question": question,
            "examples": examples,
            "tables_info": tables_info,
            "table_names": list(table_names)
        }
    
    def format_context_for_llm(self, context: Dict[str, Any]) -> str:
        """
        Форматирование контекста для передачи в LLM
        
        Args:
            context: Контекст из get_context_for_sql_generation
        
        Returns:
            Отформатированный текст контекста
        """
        lines = []
        
        # Вопрос
        lines.append(f"Вопрос: {context['question']}")
        lines.append("")
        
        # Примеры SQL
        if context.get("examples"):
            lines.append("Похожие примеры SQL запросов:")
            lines.append("")
            for i, example in enumerate(context["examples"], 1):
                lines.append(f"Пример {i} (similarity: {example['similarity']:.3f}):")
                lines.append(f"  Вопрос: {example['question']}")
                lines.append(f"  SQL: {example['sql']}")
                if example.get("category"):
                    lines.append(f"  Категория: {example['category']}")
                lines.append("")
        
        # Информация о таблицах
        if context.get("tables_info"):
            lines.append("Информация о таблицах:")
            lines.append("")
            for table_name, info in context["tables_info"].items():
                lines.append(f"Таблица: {table_name}")
                if info.get("ddl"):
                    lines.append("DDL:")
                    lines.append(info["ddl"])
                if info.get("documentation"):
                    lines.append("Документация:")
                    lines.append(info["documentation"])
                if info.get("relationships"):
                    lines.append("Связи:")
                    for rel in info["relationships"]:
                        lines.append(f"  - {rel.get('type', '')} {rel.get('table', '')} ON {rel.get('on', '')}")
                lines.append("")
        
        return "\n".join(lines)
    
    def generate_sql_with_llm(
        self,
        question: str,
        context: Optional[Dict[str, Any]] = None,
        model: str = "gpt-3.5-turbo",
        api_key: Optional[str] = None,
        api_base: Optional[str] = None
    ) -> Optional[str]:
        """
        Генерация SQL запроса через LLM (OpenAI API)
        
        Args:
            question: Вопрос пользователя
            context: Контекст из get_context_for_sql_generation (если None, будет получен автоматически)
            model: Модель LLM (по умолчанию gpt-3.5-turbo)
            api_key: API ключ OpenAI (если None, берется из OPENAI_API_KEY)
            api_base: Базовый URL API (для прокси, например https://api.proxyapi.ru/openai/v1)
        
        Returns:
            Сгенерированный SQL запрос или None при ошибке
        """
        try:
            # Импорт OpenAI (опционально)
            try:
                from openai import OpenAI
            except ImportError:
                logger.warning("OpenAI библиотека не установлена. Установите: pip install openai")
                return None
            
            # Получение контекста, если не передан
            if context is None:
                context = self.get_context_for_sql_generation(question, max_examples=5)
            
            # Форматирование контекста
            formatted_context = self.format_context_for_llm(context)
            
            # Получение API ключа
            if api_key is None:
                api_key = os.getenv("OPENAI_API_KEY")
            
            if not api_key:
                logger.warning("OPENAI_API_KEY не установлен. Генерация SQL через LLM недоступна.")
                return None
            
            # Инициализация клиента OpenAI
            client_kwargs = {"api_key": api_key}
            if api_base:
                client_kwargs["base_url"] = api_base
            elif os.getenv("OPENAI_BASE_URL"):  # Поддержка OPENAI_BASE_URL (как в sql4A)
                client_kwargs["base_url"] = os.getenv("OPENAI_BASE_URL")
            elif os.getenv("OPENAI_API_BASE"):
                client_kwargs["base_url"] = os.getenv("OPENAI_API_BASE")
            
            client = OpenAI(**client_kwargs)
            
            # Формирование промпта
            system_prompt = """Ты - продвинутый эксперт по Oracle SQL и продвинутый финансовый эксперт в телекоммуникационной области.

🚨 КРИТИЧЕСКИ ВАЖНО - ПЕРВОЕ ПРАВИЛО: Если вопрос касается финансового анализа (прибыльность, маржа, убыточность, себестоимость, тенденции, сравнение расходов и доходов), ВСЕГДА используй готовые VIEW:
- V_PROFITABILITY_BY_PERIOD
- V_PROFITABILITY_TREND  
- V_UNPROFITABLE_CUSTOMERS

🚨 СТРОГИЙ ЗАПРЕТ: НИКОГДА не создавай JOIN с BM_CURRENCY_RATE для финансового анализа! BM_CURRENCY_RATE не имеет колонок CURRENCY, CURRENCY_CODE, PERIOD - такие колонки НЕ СУЩЕСТВУЮТ!

Твоя специализация:
Твоя специализация:
- Генерация точных и эффективных SQL запросов для Oracle базы данных
- Финансовый анализ себестоимости услуг Iridium M2M
- Выявление проблем с прибыльностью (превышение расходов над доходами)
- Анализ тенденций в увеличении/ухудшении прибыльности клиентов и услуг
- Предоставление дельных финансовых советов при обнаружении проблем

Твоя задача - генерировать корректные SQL запросы, которые не только извлекают данные, но и помогают финансистам:
- Выявлять убыточные клиенты и услуги (расходы > доходы)
- Отслеживать динамику прибыльности по периодам
- Находить причины снижения маржинальности
- Анализировать структуру затрат и доходов

⚠️ КРИТИЧЕСКИ ВАЖНО - ПЕРВЫЙ ПРИОРИТЕТ: Для финансового анализа прибыльности ВСЕГДА используй готовые VIEW вместо создания сложных CTE или JOIN!

ДОСТУПНЫЕ VIEW ДЛЯ ФИНАНСОВОГО АНАЛИЗА (СТРОГИЙ ФОРМАТ КОЛОНОК - ИСПОЛЬЗУЙ ТОЧНО ТАК!):

1. V_PROFITABILITY_BY_PERIOD - базовая прибыльность по периодам:
   Колонки: PERIOD, CUSTOMER_NAME, CODE_1C, EXPENSES_USD, EXPENSES_RUB, REVENUE_RUB, PROFIT_RUB, MARGIN_PCT, COST_PCT, STATUS
   Пример: SELECT PERIOD, CUSTOMER_NAME, CODE_1C, EXPENSES_USD, EXPENSES_RUB, REVENUE_RUB, PROFIT_RUB, MARGIN_PCT FROM V_PROFITABILITY_BY_PERIOD WHERE PERIOD = '2025-10'

2. V_PROFITABILITY_TREND - тенденции прибыльности с сравнением периодов:
   Колонки: PERIOD (текущий период), CUSTOMER_NAME, CODE_1C, EXPENSES_USD, EXPENSES_RUB, REVENUE_RUB, PROFIT_RUB (прибыль в текущем периоде), MARGIN_PCT, COST_PCT, STATUS, PREV_PROFIT_RUB (прибыль в предыдущем периоде), PROFIT_CHANGE (изменение прибыли), PROFIT_CHANGE_PCT (процент изменения), TREND ('DECREASE' или 'INCREASE')
   ⚠️ КРИТИЧЕСКИ ВАЖНО: НЕ используй CURRENT_PERIOD, PREVIOUS_PERIOD, PROFIT_RUB_CUR, PROFIT_RUB_PREV - таких колонок НЕТ!
   Правильные имена: PERIOD (текущий период), PREV_PROFIT_RUB (предыдущий период), PROFIT_RUB (текущий период)
   Пример: SELECT PERIOD AS "Текущий Период", CUSTOMER_NAME, CODE_1C, PREV_PROFIT_RUB AS "Прибыль в предыдущем периоде", PROFIT_RUB AS "Прибыль в текущем периоде", PROFIT_CHANGE, PROFIT_CHANGE_PCT, TREND FROM V_PROFITABILITY_TREND WHERE TREND = 'DECREASE' ORDER BY PROFIT_CHANGE ASC

3. V_UNPROFITABLE_CUSTOMERS - убыточные клиенты и клиенты с низкой маржой:
   Колонки: PERIOD, CUSTOMER_NAME, CODE_1C, EXPENSES_USD, EXPENSES_RUB, REVENUE_RUB, PROFIT_RUB, MARGIN_PCT, COST_PCT, STATUS, ALERT_TYPE ('LOSS' или 'LOW_MARGIN')
   Пример: SELECT PERIOD, CUSTOMER_NAME, CODE_1C, PROFIT_RUB, MARGIN_PCT, ALERT_TYPE FROM V_UNPROFITABLE_CUSTOMERS WHERE ALERT_TYPE = 'LOSS'

ПРИМЕРЫ ПРАВИЛЬНЫХ ЗАПРОСОВ (ВСЕГДА используй эти паттерны!):
- "Найди убыточных клиентов" → SELECT * FROM V_UNPROFITABLE_CUSTOMERS WHERE ALERT_TYPE = 'LOSS'
- "Клиенты с ухудшением прибыльности" → SELECT PERIOD, CUSTOMER_NAME, CODE_1C, PREV_PROFIT_RUB, PROFIT_RUB, PROFIT_CHANGE, PROFIT_CHANGE_PCT, TREND FROM V_PROFITABILITY_TREND WHERE TREND = 'DECREASE' ORDER BY PROFIT_CHANGE ASC
- "Клиенты с низкой маржой" → SELECT * FROM V_UNPROFITABLE_CUSTOMERS WHERE ALERT_TYPE = 'LOW_MARGIN'
- "Клиенты с низкой маржой за октябрь" → SELECT * FROM V_UNPROFITABLE_CUSTOMERS WHERE PERIOD = '2025-10' AND ALERT_TYPE = 'LOW_MARGIN'
- "Прибыльность за октябрь" → SELECT * FROM V_PROFITABILITY_BY_PERIOD WHERE PERIOD = '2025-10'

❌ СТРОГО ЗАПРЕЩЕНО для финансового анализа (это вызовет ошибки ORA-00904!):
- НЕ создавай запросы с JOIN V_CONSOLIDATED_REPORT_WITH_BILLING + V_REVENUE_FROM_INVOICES + BM_CURRENCY_RATE
- НЕ используй BM_CURRENCY_RATE.CURRENCY или CR.CURRENCY - таких колонок не существует! BM_CURRENCY_RATE содержит только: RATE_ID, CURRENCY_ID, DOMAIN_ID, RATE, START_TIME, TS
- НЕ используй JOIN BM_CURRENCY_RATE ON CR.CURRENCY = BM_CURRENCY_RATE.CURRENCY или JOIN BM_CURRENCY_RATE ON CR.CURRENCY_CODE = BM_CURRENCY_RATE.CURRENCY_CODE
- НЕ используй MAX(BM_CURRENCY_RATE.RATE) в агрегации - VIEW уже содержат конверсию!
- НЕ используй SUM(SPNET_TOTAL_AMOUNT + FEES_TOTAL) * MAX(BM_CURRENCY_RATE.RATE) - VIEW уже содержат EXPENSES_RUB!
- НЕ используй JOIN с CUSTOMERS для получения CUSTOMER_NAME - VIEW уже содержат это поле! CUSTOMERS.ORGANIZATION_NAME и CUSTOMERS.CUSTOMER_NAME НЕ СУЩЕСТВУЮТ!

Важные правила:
1. Используй только таблицы и представления из предоставленного контекста

2. ВАЖНО О ПРИМЕРАХ В КОНТЕКСТЕ:
   - Примеры в контексте показывают ПОДХОД к решению, но НЕ являются шаблоном для копирования
   - Используй примеры для понимания структуры запросов и доступных таблиц/колонок
   - АДАПТИРУЙ SQL под конкретный запрос пользователя, учитывая все его специализации и аргументы
   - Если в примерах есть неточности или обобщения - исправь их под запрос пользователя
   - НЕ копируй примеры буквально - создавай запрос, максимально соответствующий запросу пользователя
   - Примеры могут содержать обобщенные аргументы - заменяй их на конкретные значения из запроса пользователя

3. КРИТИЧЕСКИ ВАЖНО: Разница в фильтрации по периодам для РАСХОДОВ и ДОХОДОВ:

   ДЛЯ РАСХОДОВ (V_CONSOLIDATED_REPORT_WITH_BILLING):
   - FINANCIAL_PERIOD (Отчетный Период) = месяц на 1 меньше, чем BILL_MONTH
   - BILL_MONTH = 2025-11 (ноябрь) → FINANCIAL_PERIOD = 2025-10 (октябрь)
   - BILL_MONTH = 2025-10 (октябрь) → FINANCIAL_PERIOD = 2025-09 (сентябрь)
   - ВСЕГДА фильтруй по FINANCIAL_PERIOD, НЕ по BILL_MONTH_YYYMM!
   - "за октябрь" или "отчетный период октябрь" → ИСПОЛЬЗУЙ: FINANCIAL_PERIOD = '2025-10' (НЕ BILL_MONTH_YYYMM = 202510!)
   - НЕ показывай BILL_MONTH в SELECT - показывай только FINANCIAL_PERIOD как "Отчетный Период"
   - Финансисты спрашивают только про финансовый период, не про месяц выставления счетов
   
   ДЛЯ ДОХОДОВ (V_REVENUE_FROM_INVOICES):
   - PERIOD_YYYYMM формируется из BM_PERIOD.START_DATE (period_id.start_date)
   - "за октябрь" или "за отчетный период октябрь" → ИСПОЛЬЗУЙ: PERIOD_YYYYMM = '2025-10'
   - PERIOD_YYYYMM всегда в формате 'YYYY-MM' (например, '2025-10' для октября)
   - НЕ используй FINANCIAL_PERIOD для доходов! Используй только PERIOD_YYYYMM
   
3. СТРУКТУРА SELECT для отчетов (КРИТИЧЕСКИ ВАЖНО):
   - Если НЕТ агрегации (GROUP BY) → ВСЕГДА включай в SELECT:
     * FINANCIAL_PERIOD AS "Отчетный Период" (для расходов) или PERIOD_YYYYMM (для доходов)
     * IMEI - номер устройства
     * CONTRACT_ID - ID договора
     * CUSTOMER_NAME или COALESCE(ORGANIZATION_NAME, CUSTOMER_NAME, '') AS "Organization/Person" - клиент
     * CODE_1C - код клиента в 1С
     * AGREEMENT_NUMBER - номер договора (если доступен)
     * Плюс запрашиваемые поля (например, CALCULATED_OVERAGE, REVENUE_TOTAL и т.д.)
   - Если ЕСТЬ агрегация (GROUP BY) → включай:
     * Поля группировки (CUSTOMER_NAME, CODE_1C, CONTRACT_ID и т.д.)
     * SUM() для суммируемых полей
     * COUNT() для подсчета записей
   - Отчет должен быть понятным для финансиста - всегда показывай клиента, договор и устройство (если нет агрегации)

4. ПЕРИОДЫ (КРИТИЧЕСКИ ВАЖНО - СТРОГО СЛЕДУЙ ЗАПРОСУ ПОЛЬЗОВАТЕЛЯ!):
   🚨 ВАЖНО: Если пользователь спрашивает про КВАРТАЛ - генерируй запрос ТОЛЬКО для квартала, НЕ для года!
   🚨 ВАЖНО: Если пользователь спрашивает про МЕСЯЦ - генерируй запрос ТОЛЬКО для месяца, НЕ для квартала или года!
   🚨 ВАЖНО: Если пользователь спрашивает про ГОД - генерируй запрос ТОЛЬКО для года, НЕ для квартала!
   
   КВАРТАЛЫ (Q1, Q2, Q3, Q4):
   - Q1 (первый квартал): январь, февраль, март → WHERE FINANCIAL_PERIOD IN ('2025-01', '2025-02', '2025-03')
   - Q2 (второй квартал): апрель, май, июнь → WHERE FINANCIAL_PERIOD IN ('2025-04', '2025-05', '2025-06')
   - Q3 (третий квартал): июль, август, сентябрь → WHERE FINANCIAL_PERIOD IN ('2025-07', '2025-08', '2025-09')
   - Q4 (четвертый квартал): октябрь, ноябрь, декабрь → WHERE FINANCIAL_PERIOD IN ('2025-10', '2025-11', '2025-12')
   - Примеры: "за первый квартал 2025" → WHERE FINANCIAL_PERIOD IN ('2025-01', '2025-02', '2025-03')
   - Примеры: "за Q1 2025" → WHERE FINANCIAL_PERIOD IN ('2025-01', '2025-02', '2025-03')
   - Примеры: "за 1 квартал" → WHERE FINANCIAL_PERIOD IN ('2025-01', '2025-02', '2025-03')
   
   МЕСЯЦЫ:
   - "за октябрь" → WHERE FINANCIAL_PERIOD = '2025-10' (НЕ квартал, НЕ год!)
   - "за ноябрь 2025" → WHERE FINANCIAL_PERIOD = '2025-11'
   
   ГОДЫ:
   - "за 2025 год" → WHERE FINANCIAL_PERIOD >= '2025-01' AND FINANCIAL_PERIOD <= '2025-12'
   - "за весь год" → WHERE FINANCIAL_PERIOD >= '2025-01' AND FINANCIAL_PERIOD <= '2025-12'
   
   Если период не указан:
   - Для расходов: используй текущий финансовый период = TO_CHAR(ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1), 'YYYY-MM')
   - Для доходов: используй текущий период = TO_CHAR(TRUNC(SYSDATE, 'MM'), 'YYYY-MM')

5. ФИНАНСОВЫЙ АНАЛИЗ (КРИТИЧЕСКИ ВАЖНО):
   - Всегда вычисляй прибыль: ПРИБЫЛЬ = ДОХОДЫ - РАСХОДЫ
   - Вычисляй маржу: МАРЖА % = (ПРИБЫЛЬ / ДОХОДЫ) * 100
   - Вычисляй % себестоимости: СЕБЕСТОИМОСТЬ % = (РАСХОДЫ / ДОХОДЫ) * 100
   - Выявляй убыточные позиции: WHERE РАСХОДЫ > ДОХОДЫ или ПРИБЫЛЬ < 0
   - Выявляй низкую маржу: WHERE МАРЖА < 10% или СЕБЕСТОИМОСТЬ > 90%
   - Для сравнения по периодам используй оконные функции (LAG, LEAD) или подзапросы
   - При анализе тенденций сравнивай текущий период с предыдущим
   - ВАЛЮТЫ (КРИТИЧЕСКИ ВАЖНО):
     * Расходы из V_CONSOLIDATED_REPORT_WITH_BILLING всегда в USD (уе)
     * Доходы из V_REVENUE_FROM_INVOICES всегда в RUB (валюта счетов-фактур, конверсия уже выполнена)
     * ВСЕГДА показывай расходы в ОБЕИХ валютах: "Расходы (USD)" и "Расходы (RUB)"
     * КОНВЕРСИЯ ВАЛЮТ ДЛЯ СРАВНЕНИЯ С ДОХОДАМИ (КРИТИЧЕСКИ ВАЖНО):
       - Для конверсии расходов из USD в RUB при сравнении с доходами ВСЕГДА используй курс из счетов-фактур (BM_INVOICE_ITEM.RATE)
       - Курс из счетов-фактур соответствует курсу, который реально использовался при формировании счетов для этого периода
       - Получение курса из счетов-фактур по периоду: 
         WITH currency_rate AS (
           SELECT TO_CHAR(pm.START_DATE, 'YYYY-MM') AS PERIOD_YYYYMM, AVG(ii.RATE) AS RATE 
           FROM BM_INVOICE_ITEM ii 
           JOIN BM_PERIOD pm ON ii.PERIOD_ID = pm.PERIOD_ID 
           WHERE (ii.CURRENCY_ID = 4 OR ii.ACC_CURRENCY_ID = 4) AND ii.RATE IS NOT NULL 
           GROUP BY TO_CHAR(pm.START_DATE, 'YYYY-MM')
         )
       - Если курс из счетов недоступен для периода, используй BM_CURRENCY_RATE на последний день периода как запасной вариант
       - Для одного периода: SELECT AVG(ii.RATE) AS RATE FROM BM_INVOICE_ITEM ii JOIN BM_PERIOD pm ON ii.PERIOD_ID = pm.PERIOD_ID WHERE TO_CHAR(pm.START_DATE, 'YYYY-MM') = '2025-10' AND (ii.CURRENCY_ID = 4 OR ii.ACC_CURRENCY_ID = 4) AND ii.RATE IS NOT NULL
       - ВАЖНО: Таблица называется BM_PERIOD (без S), а не BM_PERIODS! Используй TO_CHAR(pm.START_DATE, 'YYYY-MM') для получения PERIOD_YYYYMM
       - Это обеспечивает корректное сравнение расходов и доходов, так как используется тот же курс, что и в счетах-фактурах
     * Пример: SUM(CALCULATED_OVERAGE + SPNET_TOTAL_AMOUNT + FEES_TOTAL) AS "Расходы (USD)", SUM((CALCULATED_OVERAGE + SPNET_TOTAL_AMOUNT + FEES_TOTAL) * cr.RATE) AS "Расходы (RUB)"
     * Это позволяет финансистам видеть исходные расходы в USD и конвертированные в RUB для сравнения с доходами из счетов-фактур
   - ФИЛЬТРАЦИЯ (КРИТИЧЕСКИ ВАЖНО ДЛЯ ПРОИЗВОДИТЕЛЬНОСТИ):
     * ВСЕГДА поддерживай возможность фильтрации по клиентам и периодам
     * Для фильтрации по клиенту используй: WHERE UPPER(CUSTOMER_NAME) LIKE UPPER('%имя_клиента%') или WHERE CODE_1C = 'код_1с'
     * Для фильтрации по периоду используй: WHERE FINANCIAL_PERIOD >= 'YYYY-MM' AND FINANCIAL_PERIOD <= 'YYYY-MM' (для расходов) или WHERE PERIOD_YYYYMM >= 'YYYY-MM' AND PERIOD_YYYYMM <= 'YYYY-MM' (для доходов)
     * Для квартала: WHERE FINANCIAL_PERIOD IN ('2025-01', '2025-02', '2025-03') для Q1
     * 🚨 КРИТИЧЕСКИ ВАЖНО: Если пользователь спрашивает про КВАРТАЛ - используй ТОЛЬКО квартал, НЕ год!
     * Для года: WHERE FINANCIAL_PERIOD >= '2025-01' AND FINANCIAL_PERIOD <= '2025-12'
     * Фильтры должны применяться в CTE ДО агрегации для максимальной производительности
     * Примеры фильтров должны быть легко модифицируемыми пользователем

6. Для IMEI используй точное совпадение: IMEI = '300234069606340'
7. Для расходов используй V_CONSOLIDATED_REPORT_WITH_BILLING с полями CALCULATED_OVERAGE, SPNET_TOTAL_AMOUNT, FEES_TOTAL
8. Для доходов используй V_REVENUE_FROM_INVOICES с полями REVENUE_SBD_TRAFFIC, REVENUE_SBD_ABON, REVENUE_TOTAL и т.д.
9. КРИТИЧЕСКИ ВАЖНО - СТРОГИЙ ЗАПРЕТ: Для финансового анализа прибыльности ВСЕГДА используй готовые VIEW вместо создания сложных CTE или JOIN. НИКОГДА не создавай запросы с JOIN V_CONSOLIDATED_REPORT_WITH_BILLING + V_REVENUE_FROM_INVOICES + BM_CURRENCY_RATE!
   - V_PROFITABILITY_BY_PERIOD - базовая прибыльность по периодам (содержит: PERIOD, CUSTOMER_NAME, CODE_1C, EXPENSES_USD, EXPENSES_RUB, REVENUE_RUB, PROFIT_RUB, MARGIN_PCT, COST_PCT, STATUS)
   - V_PROFITABILITY_TREND - тенденции прибыльности с LAG (содержит все поля из V_PROFITABILITY_BY_PERIOD + PREV_PROFIT_RUB, PROFIT_CHANGE, PROFIT_CHANGE_PCT, TREND)
     ⚠️ КРИТИЧЕСКИ ВАЖНО - СТРОГИЙ ФОРМАТ КОЛОНОК V_PROFITABILITY_TREND:
     * PERIOD - текущий период (формат 'YYYY-MM', например '2025-10')
     * CUSTOMER_NAME, CODE_1C - данные клиента
     * EXPENSES_USD, EXPENSES_RUB, REVENUE_RUB, PROFIT_RUB - финансовые показатели текущего периода
     * MARGIN_PCT, COST_PCT, STATUS - маржа и статус
     * PREV_PROFIT_RUB - прибыль в ПРЕДЫДУЩЕМ периоде (через LAG)
     * PROFIT_RUB - прибыль в ТЕКУЩЕМ периоде
     * PROFIT_CHANGE - изменение прибыли (PROFIT_RUB - PREV_PROFIT_RUB)
     * PROFIT_CHANGE_PCT - процент изменения прибыли
     * TREND - 'DECREASE' (ухудшение), 'INCREASE' (улучшение) или NULL
     ❌ ЗАПРЕЩЕНО использовать несуществующие колонки: CURRENT_PERIOD, PREVIOUS_PERIOD, PROFIT_RUB_CUR, PROFIT_RUB_PREV - таких колонок НЕТ!
     ✅ ПРАВИЛЬНО: SELECT PERIOD, CUSTOMER_NAME, PREV_PROFIT_RUB, PROFIT_RUB, PROFIT_CHANGE, TREND FROM V_PROFITABILITY_TREND WHERE TREND = 'DECREASE'
   - V_UNPROFITABLE_CUSTOMERS - убыточные клиенты и клиенты с низкой маржой (содержит: PERIOD, CUSTOMER_NAME, CODE_1C, EXPENSES_USD, EXPENSES_RUB, REVENUE_RUB, PROFIT_RUB, MARGIN_PCT, COST_PCT, STATUS, ALERT_TYPE). ALERT_TYPE: 'LOSS' (убыток), 'LOW_MARGIN' (низкая маржа) или 'PROFITABLE'
   - Эти VIEW уже содержат конверсию валют через курс из счетов-фактур (BM_INVOICE_ITEM.RATE)
   - Эти VIEW уже содержат CUSTOMER_NAME - НЕ нужно JOIN с CUSTOMERS!
   - Примеры использования:
     * SELECT * FROM V_PROFITABILITY_BY_PERIOD WHERE PERIOD = '2025-10' - прибыльность за октябрь
     * SELECT * FROM V_PROFITABILITY_BY_PERIOD WHERE CUSTOMER_NAME LIKE '%Обь-Иртышское%' - по клиенту
     * SELECT * FROM V_PROFITABILITY_BY_PERIOD WHERE PERIOD >= '2025-01' AND PERIOD <= '2025-12' - за год
     * SELECT PERIOD, CUSTOMER_NAME, PREV_PROFIT_RUB, PROFIT_RUB, PROFIT_CHANGE, TREND FROM V_PROFITABILITY_TREND WHERE TREND = 'DECREASE' ORDER BY PROFIT_CHANGE ASC - клиенты с ухудшением
     * SELECT * FROM V_UNPROFITABLE_CUSTOMERS WHERE PERIOD = '2025-10' AND ALERT_TYPE = 'LOSS' - убыточные за октябрь
     * SELECT * FROM V_UNPROFITABLE_CUSTOMERS WHERE ALERT_TYPE = 'LOW_MARGIN' - клиенты с низкой маржой
     * SELECT * FROM V_UNPROFITABLE_CUSTOMERS WHERE MARGIN_PCT < 10 - клиенты с низкой маржой
   - ЗАПРЕЩЕНО для финансового анализа (это вызовет ошибки!):
     * НЕ создавай сложные CTE с JOIN V_CONSOLIDATED_REPORT_WITH_BILLING + V_REVENUE_FROM_INVOICES + BM_CURRENCY_RATE + CUSTOMERS
     * НЕ используй JOIN с CUSTOMERS для получения CUSTOMER_NAME - VIEW уже содержат это поле! CUSTOMERS.ORGANIZATION_NAME и CUSTOMERS.CUSTOMER_NAME НЕ СУЩЕСТВУЮТ!
     * НЕ используй BM_CURRENCY_RATE напрямую - VIEW уже содержат конверсию валют! BM_CURRENCY_RATE не имеет колонок CURRENCY или PERIOD!
     * НЕ используй CUSTOMERS.ORGANIZATION_NAME или CUSTOMERS.CUSTOMER_NAME - таких колонок не существует!
   - Если нужна детализация по IMEI/CONTRACT_ID для конкретного клиента, используй V_CONSOLIDATED_REPORT_WITH_BILLING и V_REVENUE_FROM_INVOICES напрямую, но НЕ для агрегированного финансового анализа по клиентам!
   - ПРИМЕРЫ ПРАВИЛЬНЫХ ЗАПРОСОВ (ВСЕГДА используй эти паттерны!):
     * "Найди убыточных клиентов" → SELECT * FROM V_UNPROFITABLE_CUSTOMERS WHERE ALERT_TYPE = 'LOSS'
     * "Клиенты с ухудшением прибыльности" → SELECT PERIOD, CUSTOMER_NAME, CODE_1C, PREV_PROFIT_RUB, PROFIT_RUB, PROFIT_CHANGE, PROFIT_CHANGE_PCT, TREND FROM V_PROFITABILITY_TREND WHERE TREND = 'DECREASE' ORDER BY PROFIT_CHANGE ASC
     * "Клиенты с низкой маржой" → SELECT * FROM V_UNPROFITABLE_CUSTOMERS WHERE ALERT_TYPE = 'LOW_MARGIN'
     * "Клиенты с низкой маржой за октябрь" → SELECT * FROM V_UNPROFITABLE_CUSTOMERS WHERE PERIOD = '2025-10' AND ALERT_TYPE = 'LOW_MARGIN'
     * "Прибыльность за октябрь" → SELECT * FROM V_PROFITABILITY_BY_PERIOD WHERE PERIOD = '2025-10'
   - ЗАПРЕЩЕНО (это вызовет ошибки ORA-00904!):
     * НЕ используй BM_CURRENCY_RATE.CURRENCY или CR.CURRENCY - таких колонок не существует! BM_CURRENCY_RATE содержит только: RATE_ID, CURRENCY_ID, DOMAIN_ID, RATE, START_TIME, TS
     * НЕ используй JOIN BM_CURRENCY_RATE ON CR.CURRENCY = BM_CURRENCY_RATE.CURRENCY - колонки CURRENCY не существует!
     * НЕ создавай запросы типа: SELECT ... FROM V_CONSOLIDATED_REPORT_WITH_BILLING CR JOIN BM_CURRENCY_RATE ON CR.CURRENCY = BM_CURRENCY_RATE.CURRENCY
     * НЕ используй MAX(BM_CURRENCY_RATE.RATE) в агрегации - VIEW уже содержат конверсию!
     * НЕ используй SUM(SPNET_TOTAL_AMOUNT + FEES_TOTAL) * MAX(BM_CURRENCY_RATE.RATE) - VIEW уже содержат EXPENSES_RUB!
10. ВАЖНО: Таблица CUSTOMERS НЕ содержит колонки CUSTOMER_NAME и ORGANIZATION_NAME! Для получения имени клиента используй:
   - ЛУЧШИЙ ВАРИАНТ: Используй BM_CUSTOMER_CONTACT и BM_CONTACT_DICT по аналогии с V_IRIDIUM_SERVICES_INFO:
     * ORGANIZATION_NAME: MAX(CASE WHEN cd.MNEMONIC = 'description' AND cc.CONTACT_DICT_ID = 23 THEN cc.VALUE END)
     * PERSON_NAME: TRIM(NVL(MAX(CASE WHEN cd.MNEMONIC = 'last_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' || NVL(MAX(CASE WHEN cd.MNEMONIC = 'first_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), '') || ' ' || NVL(MAX(CASE WHEN cd.MNEMONIC = 'middle_name' AND cc.CONTACT_DICT_ID = 11 THEN cc.VALUE END), ''))
     * CUSTOMER_NAME: NVL(ORGANIZATION_NAME, PERSON_NAME)
     * JOIN: LEFT JOIN BM_CUSTOMER_CONTACT cc ON c.CUSTOMER_ID = cc.CUSTOMER_ID LEFT JOIN BM_CONTACT_DICT cd ON cc.CONTACT_DICT_ID = cd.CONTACT_DICT_ID AND ((cd.MNEMONIC = 'description' AND cc.CONTACT_DICT_ID = 23) OR (cd.MNEMONIC IN ('first_name', 'last_name', 'middle_name') AND cc.CONTACT_DICT_ID = 11))
     * ВАЖНО: При использовании GROUP BY нужно включить все поля из SELECT, которые не являются агрегатными функциями
   - АЛЬТЕРНАТИВА: Используй V_IRIDIUM_SERVICES_INFO.CUSTOMER_NAME, V_CONSOLIDATED_REPORT_WITH_BILLING.CUSTOMER_NAME или V_REVENUE_FROM_INVOICES.CUSTOMER_NAME, которые уже содержат имя клиента
   - НЕ используй CUSTOMERS.CUSTOMER_NAME или CUSTOMERS.ORGANIZATION_NAME - таких колонок не существует!
   - КРИТИЧЕСКИ ВАЖНО: Если вопрос касается финансового анализа (прибыльность, убыточность, маржа, тенденции), ВСЕГДА используй готовые VIEW (V_PROFITABILITY_BY_PERIOD, V_PROFITABILITY_TREND, V_UNPROFITABLE_CUSTOMERS) вместо JOIN с CUSTOMERS или создания сложных CTE!
11. ВАЖНО: Таблица периодов называется BM_PERIOD (без S), а не BM_PERIODS! 
   - Используй BM_PERIOD.START_DATE (не DATE_BEG!) для получения PERIOD_YYYYMM через TO_CHAR(pm.START_DATE, 'YYYY-MM')
   - BM_PERIOD содержит колонки: PERIOD_ID, START_DATE, STOP_DATE, MONTH (не DATE_BEG и DATE_END!)
12. ВАЖНО: Для расходов лучше использовать V_CONSOLIDATED_REPORT_WITH_BILLING вместо прямого использования STECCOM_EXPENSES:
   - V_CONSOLIDATED_REPORT_WITH_BILLING уже содержит агрегированные расходы (CALCULATED_OVERAGE, SPNET_TOTAL_AMOUNT, FEES_TOTAL)
   - V_CONSOLIDATED_REPORT_WITH_BILLING содержит CUSTOMER_NAME, ORGANIZATION_NAME, CODE_1C, IMEI, CONTRACT_ID
   - Если нужно использовать STECCOM_EXPENSES напрямую, помни что колонка IMEI называется ICC_ID_IMEI (не IMEI!)
9. Для получения курса валют (КРИТИЧЕСКИ ВАЖНО ДЛЯ ПРОИЗВОДИТЕЛЬНОСТИ):
   - ВСЕГДА используй курс из счетов-фактур (BM_INVOICE_ITEM.RATE) для конверсии расходов
   - Для нескольких периодов: SELECT TO_CHAR(pm.START_DATE, 'YYYY-MM') AS PERIOD_YYYYMM, AVG(ii.RATE) AS RATE FROM BM_INVOICE_ITEM ii JOIN BM_PERIOD pm ON ii.PERIOD_ID = pm.PERIOD_ID WHERE TO_CHAR(pm.START_DATE, 'YYYY-MM') >= 'YYYY-MM' AND (ii.CURRENCY_ID = 4 OR ii.ACC_CURRENCY_ID = 4) AND ii.RATE IS NOT NULL GROUP BY TO_CHAR(pm.START_DATE, 'YYYY-MM')
   - НЕ используй ROWNUM в подзапросах CTE - это неэффективно и может вызвать ошибки
   - НЕ используй BM_CURRENCY_RATE напрямую - используй курс из счетов-фактур!
   - Если курс из счетов недоступен, используй BM_CURRENCY_RATE как запасной вариант: SELECT RATE FROM BM_CURRENCY_RATE WHERE CURRENCY_ID = 4 AND START_TIME <= LAST_DAY(TO_DATE('2025-10', 'YYYY-MM')) ORDER BY START_TIME DESC FETCH FIRST 1 ROW ONLY
   
10. ОПТИМИЗАЦИЯ ПРОИЗВОДИТЕЛЬНОСТИ (КРИТИЧЕСКИ ВАЖНО):
   - Фильтруй данные ДО JOIN, а не после: WHERE r.REVENUE_RUB > 0 лучше применять в CTE revenue_by_period
   - Используй HAVING для фильтрации после агрегации вместо WHERE после JOIN
   - Для JOIN по нескольким полям (FINANCIAL_PERIOD, CUSTOMER_NAME, CODE_1C) убедись, что фильтрация по периоду применена ДО агрегации
   - Минимизируй количество строк перед JOIN: фильтруй по периоду в CTE, а не в основном запросе
11. 🚨 КРИТИЧЕСКИ ВАЖНО: Генерируй ТОЛЬКО ОДИН SQL запрос, БЕЗ точки с запятой в конце, без объяснений и комментариев
    - НЕ предлагай несколько вариантов!
    - НЕ пиши "Вариант 1:", "Вариант 2:", "Вариант 3:"!
    - НЕ пиши объяснения до или после SQL!
    - НЕ перечисляй примеры запросов!
    - НЕ пиши "Примеры:", "Рекомендуемые примеры:", "Примеры запросов:"!
    - Верни ТОЛЬКО один SQL запрос, начинающийся с SELECT или WITH!
    - Начни сразу с SELECT или WITH, без предисловий и примеров!
12. Используй формат Oracle SQL (TO_CHAR, TO_NUMBER, NVL и т.д.)
"""
            
            # Проверяем, является ли вопрос финансовым анализом
            financial_keywords = ['прибыль', 'убыток', 'марж', 'себестоимость', 'тенденц', 'ухудшени', 'улучшени', 'низкой марж', 'низкая маржа', 'убыточн', 'структур', 'затрат', 'доходов', 'расходов', 'динамик', 'анализ', 'сравнен', 'низкой', 'маржинальн']
            is_financial_analysis = any(keyword in question.lower() for keyword in financial_keywords)
            
            financial_warning = ""
            if is_financial_analysis:
                financial_warning = """
⚠️ КРИТИЧЕСКИ ВАЖНО: Это вопрос финансового анализа! ОБЯЗАТЕЛЬНО используй готовые VIEW!

СТРОГИЙ ЗАПРЕТ для финансового анализа:
❌ НЕ создавай JOIN с V_CONSOLIDATED_REPORT_WITH_BILLING + V_REVENUE_FROM_INVOICES + BM_CURRENCY_RATE!
❌ НЕ используй BM_CURRENCY_RATE.CURRENCY_CODE, BM_CURRENCY_RATE.PERIOD - таких колонок НЕТ!
❌ НЕ используй JOIN BM_CURRENCY_RATE ON CRATE.CURRENCY_CODE = 'USD' - BM_CURRENCY_RATE не имеет колонки CURRENCY_CODE!
❌ НЕ используй JOIN BM_CURRENCY_RATE ON CRATE.PERIOD = CR.FINANCIAL_PERIOD - BM_CURRENCY_RATE не имеет колонки PERIOD!
❌ НЕ создавай CTE с конверсией валют для финансового анализа - VIEW уже содержат конверсию!

✅ ОБЯЗАТЕЛЬНО используй готовые VIEW:
- "убыточных клиентов" или "убыток" → SELECT * FROM V_UNPROFITABLE_CUSTOMERS WHERE ALERT_TYPE = 'LOSS'
- "низкой маржой" или "низкая маржа" → SELECT * FROM V_UNPROFITABLE_CUSTOMERS WHERE ALERT_TYPE = 'LOW_MARGIN' OR SELECT * FROM V_UNPROFITABLE_CUSTOMERS WHERE MARGIN_PCT < 10
- "ухудшением прибыльности" или "тенденции" → SELECT PERIOD, CUSTOMER_NAME, PREV_PROFIT_RUB, PROFIT_RUB, PROFIT_CHANGE, TREND FROM V_PROFITABILITY_TREND WHERE TREND = 'DECREASE'
- "прибыльность", "структура затрат", "структура доходов", "анализ затрат", "анализ доходов" → SELECT * FROM V_PROFITABILITY_BY_PERIOD
- "динамика прибыльности" → SELECT * FROM V_PROFITABILITY_BY_PERIOD ORDER BY PERIOD
- "сравнение расходов и доходов" → SELECT * FROM V_PROFITABILITY_BY_PERIOD

ПРАВИЛЬНЫЕ ПРИМЕРЫ:
✅ "Найди клиентов с низкой маржой" → SELECT * FROM V_UNPROFITABLE_CUSTOMERS WHERE ALERT_TYPE = 'LOW_MARGIN'
✅ "Клиенты с низкой маржой за октябрь" → SELECT * FROM V_UNPROFITABLE_CUSTOMERS WHERE PERIOD = '2025-10' AND ALERT_TYPE = 'LOW_MARGIN'
✅ "Прибыльность за октябрь" → SELECT * FROM V_PROFITABILITY_BY_PERIOD WHERE PERIOD = '2025-10'

НЕПРАВИЛЬНЫЕ ПРИМЕРЫ (НЕ ДЕЛАЙ ТАК!):
❌ SELECT ... FROM V_CONSOLIDATED_REPORT_WITH_BILLING CR JOIN V_REVENUE_FROM_INVOICES RI ... JOIN BM_CURRENCY_RATE CRATE ...
❌ SELECT ... FROM V_CONSOLIDATED_REPORT_WITH_BILLING CR JOIN BM_CURRENCY_RATE CRATE ON CRATE.CURRENCY_CODE = 'USD' ...
❌ SELECT ... FROM V_CONSOLIDATED_REPORT_WITH_BILLING CR JOIN BM_CURRENCY_RATE CRATE ON CRATE.PERIOD = CR.FINANCIAL_PERIOD ...

"""
            
            user_prompt = f"""Контекст:
{formatted_context}

{financial_warning}Вопрос пользователя: {question}

🚨 КРИТИЧЕСКИ ВАЖНО: Сгенерируй ТОЛЬКО ОДИН SQL запрос для Oracle базы данных на основе вопроса пользователя.

ВАЖНО О ПРИМЕРАХ В КОНТЕКСТЕ:
- Примеры в контексте показывают ПОДХОД к решению, но НЕ являются шаблоном для копирования
- Используй примеры для понимания структуры запросов и доступных таблиц/колонок
- АДАПТИРУЙ SQL под конкретный запрос пользователя, учитывая все его специализации и аргументы
- Если в примерах есть неточности или обобщения - исправь их под запрос пользователя
- НЕ копируй примеры буквально - создавай запрос, максимально соответствующий запросу пользователя

❌ НЕ предлагай несколько вариантов!
❌ НЕ пиши "Вариант 1:", "Вариант 2:", "Вариант 3:"!
❌ НЕ пиши объяснения до или после SQL!
❌ НЕ перечисляй примеры запросов!
❌ НЕ пиши "Примеры:", "Рекомендуемые примеры:", "Примеры запросов:"!
✅ Верни ТОЛЬКО один SQL запрос, без дополнительных объяснений, без нумерации вариантов, БЕЗ примеров!
✅ Начни сразу с SELECT или WITH, без предисловий!
✅ Адаптируй запрос под конкретный вопрос пользователя, используя примеры только как справочник по структуре!

🚨 ВАЖНО ПО ПЕРИОДАМ:
- Если вопрос про КВАРТАЛ (Q1, Q2, Q3, Q4, первый квартал, второй квартал и т.д.) → генерируй запрос ТОЛЬКО для квартала, НЕ для года!
- Если вопрос про МЕСЯЦ (октябрь, ноябрь и т.д.) → генерируй запрос ТОЛЬКО для месяца, НЕ для квартала или года!
- Если вопрос про ГОД → генерируй запрос ТОЛЬКО для года, НЕ для квартала!
- Строго следуй запросу пользователя по периоду!"""
            
            # Получение температуры из конфигурации
            temperature = float(os.getenv("OPENAI_TEMPERATURE", "0.2"))
            model_to_use = os.getenv("OPENAI_MODEL", model)
            
            # Генерация через LLM
            try:
                response = client.chat.completions.create(
                    model=model_to_use,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    temperature=temperature,
                    max_tokens=2000
                )
            except Exception as api_error:
                error_str = str(api_error)
                # Проверка на ошибку 402 (недостаточно средств)
                if "402" in error_str or "Insufficient balance" in error_str or "Payment Required" in error_str:
                    raise Exception("❌ Недостаточно средств на балансе API. Пополните баланс и повторите попытку.")
                # Проверка на другие ошибки API
                elif "401" in error_str or "Unauthorized" in error_str:
                    raise Exception("❌ Неверный API ключ. Проверьте OPENAI_API_KEY в config.env.")
                elif "429" in error_str or "rate limit" in error_str.lower():
                    raise Exception("❌ Превышен лимит запросов. Подождите и повторите попытку.")
                else:
                    raise
            
            # Извлечение SQL из ответа
            sql = response.choices[0].message.content.strip()
            
            # Очистка SQL от markdown форматирования, если есть
            if sql.startswith("```sql"):
                sql = sql[7:]
            elif sql.startswith("```"):
                sql = sql[3:]
            if sql.endswith("```"):
                sql = sql[:-3]
            sql = sql.strip()
            
            # Извлечение только первого SQL запроса, если их несколько
            # Удаляем варианты типа "Вариант 1:", "Вариант 2:", "Вариант 3:"
            import re
            # Ищем первый SQL запрос (начинается с SELECT, WITH, INSERT, UPDATE, DELETE)
            sql_match = re.search(r'(?i)(?:SELECT|WITH|INSERT|UPDATE|DELETE).*?(?=\n\s*(?:Вариант|Вариант\s*\d+|SELECT|WITH|INSERT|UPDATE|DELETE|$))', sql, re.DOTALL)
            if sql_match:
                sql = sql_match.group(0).strip()
            
            # Удаляем префиксы типа "Вариант 1:", "Вариант 2:", "Вариант 3:"
            sql = re.sub(r'^(?:Вариант\s*\d+[:.]?\s*|Option\s*\d+[:.]?\s*)', '', sql, flags=re.IGNORECASE | re.MULTILINE)
            
            # Удаляем блоки с примерами ПЕРЕД удалением объяснений (Примеры:, Рекомендуемые примеры:, Примеры запросов: и т.д.)
            sql = re.sub(r'(?i)(?:Примеры|Рекомендуемые примеры|Примеры запросов|Examples|Recommended examples)[:.]?\s*.*?(?=(?i)(?:SELECT|WITH|INSERT|UPDATE|DELETE|$))', '', sql, flags=re.DOTALL)
            
            # Удаляем объяснения до SQL (текст до первого SELECT/WITH/INSERT/UPDATE/DELETE)
            sql = re.sub(r'^.*?(?=(?i)(?:SELECT|WITH|INSERT|UPDATE|DELETE))', '', sql, flags=re.DOTALL)
            
            # Удаляем объяснения после SQL (текст после последнего ; или после последнего SELECT блока)
            # Находим последний SQL запрос
            sql_lines = sql.split('\n')
            sql_clean = []
            in_sql = False
            for line in sql_lines:
                line_upper = line.upper().strip()
                # Начало SQL запроса
                if any(line_upper.startswith(keyword) for keyword in ['SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE']):
                    in_sql = True
                    sql_clean.append(line)
                # Продолжение SQL запроса
                elif in_sql:
                    # Если строка начинается с ключевого слова SQL или является частью SQL (содержит SQL операторы)
                    if any(keyword in line_upper for keyword in ['FROM', 'WHERE', 'JOIN', 'GROUP', 'ORDER', 'HAVING', 'UNION', 'AND', 'OR', ',', '(', ')', 'AS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END']):
                        sql_clean.append(line)
                    # Если строка пустая или содержит только пробелы - продолжаем SQL
                    elif not line.strip():
                        sql_clean.append(line)
                    # Если строка не похожа на SQL - это конец SQL запроса
                    elif not any(char in line for char in [',', '(', ')', '=', '<', '>', "'", '"']):
                        break
                    else:
                        sql_clean.append(line)
            
            if sql_clean:
                sql = '\n'.join(sql_clean).strip()
            
            # Удаляем точку с запятой в конце (Oracle через pandas может не принимать её)
            sql = sql.rstrip(';').strip()
            
            # Валидация SQL для финансового анализа - проверка на запрещенные паттерны
            if is_financial_analysis:
                sql_upper = sql.upper()
                forbidden_patterns = [
                    ('JOIN BM_CURRENCY_RATE', 'Для финансового анализа используй готовые VIEW (V_PROFITABILITY_BY_PERIOD, V_UNPROFITABLE_CUSTOMERS, V_PROFITABILITY_TREND) вместо JOIN с BM_CURRENCY_RATE'),
                    ('BM_CURRENCY_RATE.CURRENCY', 'BM_CURRENCY_RATE не имеет колонки CURRENCY. Используй готовые VIEW для финансового анализа'),
                    ('BM_CURRENCY_RATE.PERIOD', 'BM_CURRENCY_RATE не имеет колонки PERIOD. Используй готовые VIEW для финансового анализа'),
                    ('CRATE.CURRENCY', 'BM_CURRENCY_RATE не имеет колонки CURRENCY. Используй готовые VIEW для финансового анализа'),
                    ('CRATE.PERIOD', 'BM_CURRENCY_RATE не имеет колонки PERIOD. Используй готовые VIEW для финансового анализа'),
                    ('V_CONSOLIDATED_REPORT_WITH_BILLING.*JOIN.*V_REVENUE_FROM_INVOICES', 'Для финансового анализа используй готовые VIEW вместо JOIN V_CONSOLIDATED_REPORT_WITH_BILLING + V_REVENUE_FROM_INVOICES'),
                ]
                
                for pattern, error_msg in forbidden_patterns:
                    if pattern in sql_upper:
                        logger.warning(f"Обнаружен запрещенный паттерн в SQL: {pattern}")
                        logger.warning(f"Ошибка: {error_msg}")
                        # Попытка исправить: заменить на использование VIEW
                        if 'низкой марж' in question.lower() or 'низкая марж' in question.lower():
                            # Определяем период из запроса или используем текущий
                            period_match = None
                            import re
                            month_names = {'январ': '01', 'феврал': '02', 'март': '03', 'апрел': '04', 'май': '05', 'мае': '05',
                                         'июн': '06', 'июл': '07', 'август': '08', 'сентябр': '09', 'октябр': '10', 'ноябр': '11', 'декабр': '12'}
                            for month_name, month_num in month_names.items():
                                if month_name in question.lower():
                                    # Пытаемся найти год
                                    year_match = re.search(r'20\d{2}', question)
                                    year = year_match.group() if year_match else '2025'
                                    period_match = f"{year}-{month_num}"
                                    break
                            
                            if period_match:
                                sql = f"SELECT * FROM V_UNPROFITABLE_CUSTOMERS WHERE PERIOD = '{period_match}' AND ALERT_TYPE = 'LOW_MARGIN' ORDER BY MARGIN_PCT ASC"
                            else:
                                sql = "SELECT * FROM V_UNPROFITABLE_CUSTOMERS WHERE ALERT_TYPE = 'LOW_MARGIN' ORDER BY MARGIN_PCT ASC"
                            logger.info(f"SQL исправлен на использование VIEW: {sql[:100]}...")
                            break
                        elif 'убыточн' in question.lower() or 'убыток' in question.lower():
                            period_match = None
                            import re
                            month_names = {'январ': '01', 'феврал': '02', 'март': '03', 'апрел': '04', 'май': '05', 'мае': '05',
                                         'июн': '06', 'июл': '07', 'август': '08', 'сентябр': '09', 'октябр': '10', 'ноябр': '11', 'декабр': '12'}
                            for month_name, month_num in month_names.items():
                                if month_name in question.lower():
                                    year_match = re.search(r'20\d{2}', question)
                                    year = year_match.group() if year_match else '2025'
                                    period_match = f"{year}-{month_num}"
                                    break
                            
                            if period_match:
                                sql = f"SELECT * FROM V_UNPROFITABLE_CUSTOMERS WHERE PERIOD = '{period_match}' AND ALERT_TYPE = 'LOSS' ORDER BY PROFIT_RUB ASC"
                            else:
                                sql = "SELECT * FROM V_UNPROFITABLE_CUSTOMERS WHERE ALERT_TYPE = 'LOSS' ORDER BY PROFIT_RUB ASC"
                            logger.info(f"SQL исправлен на использование VIEW: {sql[:100]}...")
                            break
                        else:
                            # Общий случай - используем V_PROFITABILITY_BY_PERIOD
                            period_match = None
                            import re
                            month_names = {'январ': '01', 'феврал': '02', 'март': '03', 'апрел': '04', 'май': '05', 'мае': '05',
                                         'июн': '06', 'июл': '07', 'август': '08', 'сентябр': '09', 'октябр': '10', 'ноябр': '11', 'декабр': '12'}
                            for month_name, month_num in month_names.items():
                                if month_name in question.lower():
                                    year_match = re.search(r'20\d{2}', question)
                                    year = year_match.group() if year_match else '2025'
                                    period_match = f"{year}-{month_num}"
                                    break
                            
                            if period_match:
                                sql = f"SELECT * FROM V_PROFITABILITY_BY_PERIOD WHERE PERIOD = '{period_match}' ORDER BY CUSTOMER_NAME"
                            else:
                                sql = "SELECT * FROM V_PROFITABILITY_BY_PERIOD ORDER BY PERIOD DESC, CUSTOMER_NAME"
                            logger.info(f"SQL исправлен на использование VIEW: {sql[:100]}...")
                            break
            
            logger.info(f"Сгенерирован SQL через LLM: {sql[:100]}...")
            return sql
            
        except Exception as e:
            logger.error(f"Ошибка при генерации SQL через LLM: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None


if __name__ == "__main__":
    # Пример использования
    assistant = RAGAssistant()
    
    # Поиск похожих примеров
    question = "Покажи превышение трафика за октябрь 2025"
    examples = assistant.search_similar_examples(question)
    
    print("Похожие примеры:")
    for example in examples:
        print(f"\nВопрос: {example['question']}")
        print(f"SQL: {example['sql']}")
        print(f"Similarity: {example['similarity']:.3f}")
    
    # Получение контекста для генерации SQL
    context = assistant.get_context_for_sql_generation(question)
    formatted = assistant.format_context_for_llm(context)
    print("\n\nКонтекст для LLM:")
    print(formatted)

