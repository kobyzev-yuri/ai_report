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
            system_prompt = """Ты эксперт по Oracle SQL и схеме billing для Iridium M2M сервисов.
Твоя задача - генерировать корректные SQL запросы на основе вопросов пользователя и примеров.

Важные правила:
1. Используй только таблицы и представления из предоставленного контекста

2. КРИТИЧЕСКИ ВАЖНО: Разница в фильтрации по периодам для РАСХОДОВ и ДОХОДОВ:

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
   
3. Для IMEI используй точное совпадение: IMEI = '300234069606340'
4. Для расходов используй V_CONSOLIDATED_REPORT_WITH_BILLING с полями CALCULATED_OVERAGE, SPNET_TOTAL_AMOUNT, FEES_TOTAL
5. Для доходов используй V_REVENUE_FROM_INVOICES с полями REVENUE_SBD_TRAFFIC, REVENUE_SBD_ABON, REVENUE_TOTAL и т.д.
6. Генерируй только SQL запрос, без объяснений и комментариев
7. Используй формат Oracle SQL (TO_CHAR, TO_NUMBER, NVL и т.д.)
"""
            
            user_prompt = f"""Контекст:
{formatted_context}

Вопрос пользователя: {question}

Сгенерируй SQL запрос для Oracle базы данных на основе вопроса и примеров из контекста.
Верни только SQL запрос, без дополнительных объяснений."""
            
            # Получение температуры из конфигурации
            temperature = float(os.getenv("OPENAI_TEMPERATURE", "0.2"))
            model_to_use = os.getenv("OPENAI_MODEL", model)
            
            # Генерация через LLM
            response = client.chat.completions.create(
                model=model_to_use,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=temperature,
                max_tokens=2000
            )
            
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

