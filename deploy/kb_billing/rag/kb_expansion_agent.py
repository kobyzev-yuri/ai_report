#!/usr/bin/env python3
"""
Интерактивный агент для расширения базы знаний (KB)
Уточняет запросы клиента и собирает новые Q/A примеры для обучения KB
"""
import os
# Исправление проблемы с protobuf
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

from typing import List, Dict, Any, Optional, Tuple
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer
import json
from pathlib import Path
import logging

try:
    from kb_billing.rag.config_sql4a import SQL4AConfig
    from kb_billing.rag.rag_assistant import RAGAssistant
    from kb_billing.rag.kb_loader import KBLoader
except ImportError:
    class SQL4AConfig:
        EMBEDDING_MODEL = "intfloat/multilingual-e5-base"
        QDRANT_HOST = "localhost"
        QDRANT_PORT = 6333
        QDRANT_COLLECTION = "kb_billing"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KBExpansionAgent:
    """Интерактивный агент для расширения базы знаний через диалог"""
    
    def __init__(
        self,
        qdrant_host: Optional[str] = None,
        qdrant_port: Optional[int] = None,
        collection_name: Optional[str] = None,
        embedding_model: Optional[str] = None
    ):
        """
        Инициализация агента расширения KB
        
        Args:
            qdrant_host: Хост Qdrant сервера
            qdrant_port: Порт Qdrant сервера
            collection_name: Имя коллекции в Qdrant
            embedding_model: Модель для генерации эмбеддингов
        """
        self.qdrant_host = qdrant_host or SQL4AConfig.QDRANT_HOST
        self.qdrant_port = qdrant_port or SQL4AConfig.QDRANT_PORT
        self.collection_name = collection_name or SQL4AConfig.QDRANT_COLLECTION
        self.embedding_model = embedding_model or SQL4AConfig.EMBEDDING_MODEL
        
        # Инициализация RAG ассистента для поиска существующих примеров
        self.rag_assistant = RAGAssistant(
            qdrant_host=self.qdrant_host,
            qdrant_port=self.qdrant_port,
            collection_name=self.collection_name,
            embedding_model=self.embedding_model
        )
        
        # Инициализация KB загрузчика для добавления новых примеров
        self.kb_loader = KBLoader(
            qdrant_host=self.qdrant_host,
            qdrant_port=self.qdrant_port,
            collection_name=self.collection_name,
            embedding_model=self.embedding_model
        )
        
        # Путь к файлу с примерами
        self.kb_dir = Path(__file__).parent.parent
        self.examples_file = self.kb_dir / "training_data" / "sql_examples.json"
        
        logger.info("Инициализация KBExpansionAgent")
    
    def check_existing_examples(self, question: str, similarity_threshold: float = 0.85) -> Tuple[List[Dict], bool]:
        """
        Проверка существующих примеров в KB
        
        Args:
            question: Вопрос клиента
            similarity_threshold: Порог схожести для определения дубликатов
        
        Returns:
            (похожие_примеры, есть_дубликаты)
        """
        similar = self.rag_assistant.search_similar_examples(question, limit=5)
        
        # Проверяем схожесть через эмбеддинги
        if similar:
            # Берем самый похожий пример
            top_similarity = similar[0].get('score', 0)
            is_duplicate = top_similarity >= similarity_threshold
            return similar, is_duplicate
        
        return [], False
    
    def generate_clarification_questions(self, question: str, context: Dict[str, Any]) -> List[str]:
        """
        Генерация уточняющих вопросов для клиента
        
        Args:
            question: Исходный вопрос
            context: Контекст из существующих примеров
        
        Returns:
            Список уточняющих вопросов
        """
        clarifications = []
        
        # Проверяем, что нужно уточнить
        question_lower = question.lower()
        
        # Уточнения по периоду
        if not any(word in question_lower for word in ['период', 'месяц', 'год', 'октябрь', 'ноябрь', '2025', '2024']):
            clarifications.append("За какой период нужны данные? (например, октябрь 2025, с начала года)")
        
        # Уточнения по клиенту
        if not any(word in question_lower for word in ['клиент', 'customer', 'организация', 'имя']):
            clarifications.append("Нужны данные по конкретному клиенту или по всем клиентам?")
        
        # Уточнения по типу данных
        if not any(word in question_lower for word in ['доход', 'расход', 'трафик', 'превышение', 'прибыль', 'маржа']):
            clarifications.append("Какие данные вас интересуют? (доходы, расходы, трафик, прибыльность)")
        
        # Уточнения по агрегации
        if any(word in question_lower for word in ['суммарно', 'итого', 'всего']) and not any(word in question_lower for word in ['по', 'групп']):
            clarifications.append("Нужна ли группировка? (по клиентам, по периодам, по договорам)")
        
        return clarifications
    
    def format_example_for_kb(
        self,
        question: str,
        sql: str,
        context: Optional[str] = None,
        business_entity: Optional[str] = None,
        complexity: Optional[int] = None,
        category: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Форматирование примера для добавления в KB
        
        Args:
            question: Вопрос клиента
            sql: SQL запрос
            context: Контекст/описание
            business_entity: Бизнес-сущность
            complexity: Сложность (1-5)
            category: Категория
        
        Returns:
            Словарь с примером в формате KB
        """
        example = {
            "question": question.strip(),
            "sql": sql.strip(),
            "context": context or f"SQL запрос для: {question}",
            "business_entity": business_entity or "general",
            "complexity": complexity or 2,
            "category": category or "Общие"
        }
        return example
    
    def add_example_to_file(self, example: Dict[str, Any]) -> bool:
        """
        Добавление примера в файл sql_examples.json
        
        Args:
            example: Пример для добавления
        
        Returns:
            True если успешно добавлено
        """
        try:
            # Читаем существующие примеры
            if self.examples_file.exists():
                with open(self.examples_file, 'r', encoding='utf-8') as f:
                    examples = json.load(f)
            else:
                examples = []
            
            # Проверяем на дубликаты (по вопросу)
            question_lower = example['question'].lower().strip()
            for existing in examples:
                if existing.get('question', '').lower().strip() == question_lower:
                    logger.warning(f"Пример с таким вопросом уже существует: {example['question']}")
                    return False
            
            # Добавляем новый пример
            examples.append(example)
            
            # Сохраняем обратно
            with open(self.examples_file, 'w', encoding='utf-8') as f:
                json.dump(examples, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Пример добавлен в файл: {example['question']}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при добавлении примера в файл: {e}")
            return False
    
    def retrain_kb_with_new_examples(self, recreate: bool = False) -> Tuple[bool, str]:
        """
        Переобучение KB с новыми примерами
        
        Args:
            recreate: Пересоздать коллекцию (удалить старую). 
                     Если False, новые примеры добавляются к существующим.
        
        Returns:
            (успех, сообщение)
        """
        try:
            # Загружаем все данные (включая новые примеры)
            # load_all создает коллекцию если нужно и загружает все компоненты KB
            self.kb_loader.load_all(recreate=recreate)
            
            if recreate:
                message = "✅ KB успешно переобучена (коллекция пересоздана)"
            else:
                message = "✅ KB успешно обновлена с новыми примерами"
            logger.info(message)
            return True, message
            
        except Exception as e:
            error_msg = f"❌ Ошибка при переобучении KB: {str(e)}"
            logger.error(error_msg)
            import traceback
            logger.error(traceback.format_exc())
            return False, error_msg
    
    def get_existing_examples_count(self) -> int:
        """Получение количества существующих примеров"""
        try:
            if self.examples_file.exists():
                with open(self.examples_file, 'r', encoding='utf-8') as f:
                    examples = json.load(f)
                return len(examples)
            return 0
        except:
            return 0
    
    def analyze_question_and_suggest_sql(self, question: str, api_key: Optional[str] = None, api_base: Optional[str] = None) -> Dict[str, Any]:
        """
        Анализ вопроса и предложение SQL запроса
        
        Args:
            question: Вопрос клиента
            api_key: API ключ для LLM
            api_base: Базовый URL для LLM
        
        Returns:
            Словарь с анализом и предложениями
        """
        result = {
            "question": question,
            "similar_examples": [],
            "is_duplicate": False,
            "clarifications": [],
            "suggested_sql": None,
            "suggested_context": None
        }
        
        # Проверяем существующие примеры
        similar, is_duplicate = self.check_existing_examples(question)
        result["similar_examples"] = similar
        result["is_duplicate"] = is_duplicate
        
        if is_duplicate:
            result["suggested_sql"] = similar[0].get('sql')
            result["suggested_context"] = similar[0].get('context')
            return result
        
        # Получаем контекст для генерации SQL
        context = self.rag_assistant.get_context_for_sql_generation(question, max_examples=5)
        
        # Генерируем уточняющие вопросы
        result["clarifications"] = self.generate_clarification_questions(question, context)
        
        # Генерируем SQL если есть API ключ
        if api_key:
            try:
                sql = self.rag_assistant.generate_sql_with_llm(
                    question=question,
                    context=context,
                    api_key=api_key,
                    api_base=api_base
                )
                result["suggested_sql"] = sql
                result["suggested_context"] = self.rag_assistant.format_context_for_llm(context)
            except Exception as e:
                logger.warning(f"Не удалось сгенерировать SQL: {e}")
        
        return result

