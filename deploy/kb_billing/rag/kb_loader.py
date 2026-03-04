#!/usr/bin/env python3
"""
Модуль для загрузки KB_billing в Qdrant векторную БД
"""
import os
# Исправление проблемы с protobuf - должно быть ДО импорта transformers
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KBLoader:
    """Класс для загрузки KB_billing в Qdrant (совместим с sql4A)"""
    
    def __init__(
        self,
        qdrant_host: Optional[str] = None,
        qdrant_port: Optional[int] = None,
        collection_name: Optional[str] = None,
        embedding_model: Optional[str] = None
    ):
        """
        Инициализация загрузчика KB (использует настройки sql4A)
        
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
        self.vector_size = self.model.get_sentence_embedding_dimension()
        
        logger.info(f"Инициализация KBLoader:")
        logger.info(f"  - Qdrant: {self.qdrant_host}:{self.qdrant_port}")
        logger.info(f"  - Коллекция: {self.collection_name}")
        logger.info(f"  - Модель эмбеддингов: {self.embedding_model}")
        logger.info(f"  - Размерность векторов: {self.vector_size}")
        
        # Путь к директории KB
        self.kb_dir = Path(__file__).parent.parent
        
    def create_collection(self, recreate: bool = False):
        """Создание коллекции в Qdrant"""
        try:
            # Проверяем существование коллекции
            collections = self.client.get_collections().collections
            collection_exists = any(c.name == self.collection_name for c in collections)
            
            if collection_exists:
                if recreate:
                    logger.info(f"Удаление существующей коллекции {self.collection_name}")
                    self.client.delete_collection(self.collection_name)
                else:
                    logger.info(f"Коллекция {self.collection_name} уже существует")
                    return
            
            logger.info(f"Создание коллекции {self.collection_name} с размерностью {self.vector_size}")
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(
                    size=self.vector_size,
                    distance=Distance.COSINE
                )
            )
            logger.info(f"Коллекция {self.collection_name} создана успешно")
        except Exception as e:
            logger.error(f"Ошибка при создании коллекции: {e}")
            raise
    
    def _qa_examples_to_points(self, examples: List[Dict[str, Any]], id_offset: int = 0) -> List[PointStruct]:
        """Преобразование списка примеров Q/A в точки для Qdrant."""
        points = []
        for i, example in enumerate(examples):
            question = example.get('question', '')
            if not question or not example.get('sql', '').strip():
                continue
            embedding = self.model.encode(
                question,
                normalize_embeddings=SQL4AConfig.NORMALIZE_EMBEDDINGS
            ).tolist()
            sql = example.get('sql', '')
            table_names = self._extract_table_names(sql)
            point = PointStruct(
                id=id_offset + i,
                vector=embedding,
                payload={
                    "type": "qa_example",
                    "question": question,
                    "sql": sql,
                    "context": example.get('context', ''),
                    "category": example.get('category', ''),
                    "complexity": example.get('complexity', 1),
                    "business_entity": example.get('business_entity', ''),
                    "table_names": table_names,
                    "content": f"Вопрос: {question}\n\nSQL: {sql}"
                }
            )
            points.append(point)
        return points

    def load_qa_examples(self) -> List[PointStruct]:
        """Загрузка Q/A примеров из training_data/sql_examples.json и user_added_examples.json (если есть)."""
        points = []
        qa_file = self.kb_dir / "training_data" / "sql_examples.json"
        if qa_file.exists():
            logger.info(f"Загрузка Q/A примеров из {qa_file}")
            with open(qa_file, 'r', encoding='utf-8') as f:
                examples = json.load(f)
            if not isinstance(examples, list):
                examples = [examples]
            points.extend(self._qa_examples_to_points(examples, id_offset=0))
        else:
            logger.warning(f"Файл {qa_file} не найден")

        user_file = self.kb_dir / "training_data" / "user_added_examples.json"
        if user_file.exists():
            logger.info(f"Загрузка пользовательских примеров из {user_file}")
            try:
                with open(user_file, 'r', encoding='utf-8') as f:
                    user_examples = json.load(f)
                if not isinstance(user_examples, list):
                    user_examples = [user_examples]
                id_offset = 1_000_000
                points.extend(self._qa_examples_to_points(user_examples, id_offset=id_offset))
            except Exception as e:
                logger.warning(f"Не удалось загрузить {user_file}: {e}")

        logger.info(f"Загружено {len(points)} Q/A примеров")
        return points

    def reload_qa_examples_only(self) -> int:
        """Перезагрузить только Q/A примеры (sql_examples.json + user_added_examples.json) без полной перестройки KB.
        Удаляет из коллекции точки с type=qa_example и загружает примеры заново. Остальные точки (таблицы, представления, Confluence) не трогает.
        Возвращает количество загруженных точек."""
        self.create_collection(recreate=False)
        self.client.delete(
            collection_name=self.collection_name,
            points_selector=Filter(should=[FieldCondition(key="type", match=MatchValue(value="qa_example"))]),
        )
        logger.info("Удалены старые Q/A примеры из коллекции")
        points = self.load_qa_examples()
        if not points:
            logger.warning("Нет Q/A примеров для загрузки")
            return 0
        batch_size = 100
        for i in range(0, len(points), batch_size):
            batch = points[i : i + batch_size]
            self.client.upsert(collection_name=self.collection_name, points=batch)
        logger.info("Загружено %s Q/A примеров (без перестройки остальной KB)", len(points))
        return len(points)

    def load_table_documentation(self) -> List[PointStruct]:
        """Загрузка документации таблиц из tables/*.json"""
        tables_dir = self.kb_dir / "tables"
        
        if not tables_dir.exists():
            logger.warning(f"Директория {tables_dir} не найдена")
            return []
        
        logger.info(f"Загрузка документации таблиц из {tables_dir}")
        
        points = []
        table_files = list(tables_dir.glob("*.json"))
        
        for table_file in table_files:
            with open(table_file, 'r', encoding='utf-8') as f:
                table_data = json.load(f)
            
            table_name = table_data.get('table_name', '')
            
            # Формируем текст документации
            doc_text = self._format_table_documentation(table_data)
            
            # Генерация эмбеддинга (как в sql4A - с нормализацией)
            embedding = self.model.encode(
                doc_text, 
                normalize_embeddings=SQL4AConfig.NORMALIZE_EMBEDDINGS
            ).tolist()
            
            # DDL
            ddl = table_data.get('ddl', '')
            if ddl:
                ddl_embedding = self.model.encode(
                    ddl, 
                    normalize_embeddings=SQL4AConfig.NORMALIZE_EMBEDDINGS
                ).tolist()
                ddl_point = PointStruct(
                    id=hash(f"ddl_{table_name}") & 0x7FFFFFFFFFFFFFFF,
                    vector=ddl_embedding,
                    payload={
                        "type": "ddl",
                        "table_name": table_name,
                        "content": ddl,
                        "schema": table_data.get('schema', 'billing'),
                        "database": table_data.get('database', 'Oracle')
                    }
                )
                points.append(ddl_point)
            
            # Документация
            doc_point = PointStruct(
                id=hash(f"doc_{table_name}") & 0x7FFFFFFFFFFFFFFF,
                vector=embedding,
                payload={
                    "type": "documentation",
                    "table_name": table_name,
                    "content": doc_text,
                    "description": table_data.get('description', ''),
                    "key_columns": table_data.get('key_columns', {}),
                    "business_rules": table_data.get('business_rules', []),
                    "relationships": table_data.get('relationships', []),
                    "usage_notes": table_data.get('usage_notes', [])
                }
            )
            points.append(doc_point)
        
        logger.info(f"Загружено {len(points)} точек документации таблиц")
        return points
    
    def load_view_documentation(self) -> List[PointStruct]:
        """Загрузка документации представлений из views/*.json"""
        views_dir = self.kb_dir / "views"
        
        if not views_dir.exists():
            logger.warning(f"Директория {views_dir} не найдена")
            return []
        
        logger.info(f"Загрузка документации представлений из {views_dir}")
        
        points = []
        view_files = list(views_dir.glob("*.json"))
        
        for view_file in view_files:
            with open(view_file, 'r', encoding='utf-8') as f:
                view_data = json.load(f)
            
            view_name = view_data.get('view_name', '')
            
            # Формируем текст документации
            doc_text = self._format_view_documentation(view_data)
            
            # Генерация эмбеддинга (как в sql4A - с нормализацией)
            embedding = self.model.encode(
                doc_text, 
                normalize_embeddings=SQL4AConfig.NORMALIZE_EMBEDDINGS
            ).tolist()
            
            point = PointStruct(
                id=hash(f"view_{view_name}") & 0x7FFFFFFFFFFFFFFF,
                vector=embedding,
                payload={
                    "type": "view",
                    "view_name": view_name,
                    "content": doc_text,
                    "description": view_data.get('description', ''),
                    "columns": view_data.get('columns', {}),
                    "usage_notes": view_data.get('usage_notes', [])
                }
            )
            points.append(point)
        
        logger.info(f"Загружено {len(points)} точек документации представлений")
        return points
    
    def load_metadata(self) -> List[PointStruct]:
        """Загрузка метаданных схемы из metadata.json"""
        metadata_file = self.kb_dir / "metadata.json"
        
        if not metadata_file.exists():
            logger.warning(f"Файл {metadata_file} не найден")
            return []
        
        logger.info(f"Загрузка метаданных из {metadata_file}")
        
        with open(metadata_file, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        
        # Формируем текст метаданных
        metadata_text = self._format_metadata(metadata)
        
        # Генерация эмбеддинга (как в sql4A - с нормализацией)
        embedding = self.model.encode(
            metadata_text, 
            normalize_embeddings=SQL4AConfig.NORMALIZE_EMBEDDINGS
        ).tolist()
        
        point = PointStruct(
            id=hash("metadata_schema") & 0x7FFFFFFFFFFFFFFF,
            vector=embedding,
            payload={
                "type": "metadata",
                "content": metadata_text,
                "database": metadata.get('database', ''),
                "schema": metadata.get('schema', ''),
                "total_tables": metadata.get('total_tables', 0),
                "total_views": metadata.get('total_views', 0),
                "main_tables": metadata.get('main_tables', []),
                "business_domains": metadata.get('business_domains', [])
            }
        )
        
        logger.info("Загружены метаданные схемы")
        return [point]

    def _confluence_content_to_text(self, content: List[Dict[str, Any]]) -> str:
        """Сведение content[] (секции/подсекции из Confluence KB) в один текст для эмбеддинга."""
        parts: List[str] = []

        def walk(sections: List[Dict], prefix: str = "") -> None:
            for s in sections:
                if s.get("title"):
                    parts.append(f"{prefix}{s['title']}")
                if s.get("text"):
                    parts.append(s["text"])
                walk(s.get("subsections", []), prefix=prefix + "  ")

        walk(content)
        return "\n\n".join(parts).strip()

    def _confluence_section_to_text(self, section: Dict[str, Any]) -> str:
        """Текст одной секции (заголовок + текст + рекурсивно подсекции)."""
        parts = []
        if section.get("title"):
            parts.append(section["title"])
        if section.get("text"):
            parts.append(section["text"])
        for sub in section.get("subsections", []):
            parts.append(self._confluence_section_to_text(sub))
        return "\n\n".join(parts).strip()

    def load_confluence_docs(self) -> List[PointStruct]:
        """
        Загрузка документов из Confluence (схемы сети, документация спутниковых инженеров).
        Читает JSON из kb_billing/confluence_docs/*.json. Каждая секция страницы (в т.ч. «Вложение: …»)
        загружается отдельной точкой (confluence_section), чтобы поиск возвращал релевантные фрагменты,
        а не целую страницу. domain='satellite'.
        """
        confluence_docs_dir = self.kb_dir / "confluence_docs"
        if not confluence_docs_dir.exists():
            logger.info("Директория confluence_docs не найдена — документы Confluence не загружаются")
            return []

        outdated_file = confluence_docs_dir / "outdated.txt"
        outdated_ids = set()
        if outdated_file.exists():
            try:
                with open(outdated_file, "r", encoding="utf-8") as f:
                    outdated_ids = {line.strip() for line in f if line.strip()}
            except Exception as e:
                logger.warning("Не удалось прочитать %s: %s", outdated_file, e)

        points = []
        for json_file in confluence_docs_dir.glob("*.json"):
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    docs = json.load(f)
            except Exception as e:
                logger.warning("Не удалось прочитать %s: %s", json_file, e)
                continue
            if not isinstance(docs, list):
                docs = [docs]
            for doc in docs:
                source = doc.get("source") or {}
                page_id = source.get("page_id", "")
                if page_id and page_id in outdated_ids:
                    continue
                page_title = doc.get("title", "Без названия")
                source_url = source.get("url", "")
                content = doc.get("content", [])
                for idx, section in enumerate(content):
                    section_title = section.get("title", "").strip()
                    section_text = self._confluence_section_to_text(section)
                    if not section_text:
                        continue
                    # Один чанк = одна секция (в т.ч. «Вложение: filename»)
                    text_for_embed = f"{page_title}\n\n{section_title}\n\n{section_text}".strip()
                    if len(text_for_embed) > 15000:
                        text_for_embed = text_for_embed[:15000] + "\n\n[... обрезано ...]"
                    embedding = self.model.encode(
                        text_for_embed,
                        normalize_embeddings=SQL4AConfig.NORMALIZE_EMBEDDINGS,
                    ).tolist()
                    point_id = hash(f"confluence_{page_id}_{idx}_{section_title or idx}") & 0x7FFFFFFFFFFFFFFF
                    points.append(
                        PointStruct(
                            id=point_id,
                            vector=embedding,
                            payload={
                                "type": "confluence_section",
                                "domain": "satellite",
                                "title": page_title,
                                "section_title": section_title or "(без заголовка)",
                                "content": text_for_embed[:12000],
                                "source_url": source_url,
                                "page_id": page_id,
                                "last_updated": source.get("last_updated", ""),
                                "scope": doc.get("scope", ["general"]),
                            },
                        )
                    )
        logger.info("Загружено %s чанков Confluence (сектор спутниковых систем)", len(points))
        return points

    def load_all(self, recreate: bool = False):
        """Загрузка всех данных в Qdrant"""
        # Создаем коллекцию
        self.create_collection(recreate=recreate)
        
        # Загружаем все типы данных
        all_points = []
        
        # Q/A примеры
        qa_points = self.load_qa_examples()
        all_points.extend(qa_points)
        
        # Документация таблиц
        table_points = self.load_table_documentation()
        all_points.extend(table_points)
        
        # Документация представлений
        view_points = self.load_view_documentation()
        all_points.extend(view_points)
        
        # Метаданные
        metadata_points = self.load_metadata()
        all_points.extend(metadata_points)

        # Документы Confluence (схемы сети, документация спутниковых инженеров) — расширение единой KB
        confluence_points = self.load_confluence_docs()
        all_points.extend(confluence_points)
        
        # Загружаем в Qdrant батчами
        batch_size = 100
        total = len(all_points)
        
        logger.info(f"Загрузка {total} точек в Qdrant...")
        
        for i in range(0, total, batch_size):
            batch = all_points[i:i + batch_size]
            self.client.upsert(
                collection_name=self.collection_name,
                points=batch
            )
            logger.info(f"Загружено {min(i + batch_size, total)}/{total} точек")
        
        logger.info(f"Загрузка завершена. Всего загружено {total} точек")
        
        # Статистика
        collection_info = self.client.get_collection(self.collection_name)
        logger.info(f"Коллекция {self.collection_name} содержит {collection_info.points_count} точек")
    
    def _extract_table_names(self, sql: str) -> List[str]:
        """Простое извлечение имен таблиц из SQL"""
        import re
        # Ищем FROM и JOIN с именами таблиц
        patterns = [
            r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        ]
        
        table_names = set()
        for pattern in patterns:
            matches = re.findall(pattern, sql, re.IGNORECASE)
            table_names.update(matches)
        
        return list(table_names)
    
    def _format_table_documentation(self, table_data: Dict) -> str:
        """Форматирование документации таблицы в текст"""
        lines = []
        
        table_name = table_data.get('table_name', '')
        description = table_data.get('description', '')
        
        lines.append(f"Таблица: {table_name}")
        lines.append(f"Описание: {description}")
        lines.append("")
        
        # Ключевые колонки
        key_columns = table_data.get('key_columns', {})
        if key_columns:
            lines.append("Ключевые колонки:")
            for col, desc in key_columns.items():
                lines.append(f"  - {col}: {desc}")
            lines.append("")
        
        # Бизнес-правила
        business_rules = table_data.get('business_rules', [])
        if business_rules:
            lines.append("Бизнес-правила:")
            for rule in business_rules:
                lines.append(f"  - {rule}")
            lines.append("")
        
        # Связи
        relationships = table_data.get('relationships', [])
        if relationships:
            lines.append("Связи с другими таблицами:")
            for rel in relationships:
                target_table = rel.get('table', '')
                join_type = rel.get('type', '')
                join_on = rel.get('on', '')
                lines.append(f"  - {join_type} {target_table} ON {join_on}")
            lines.append("")
        
        return "\n".join(lines)
    
    def _format_view_documentation(self, view_data: Dict) -> str:
        """Форматирование документации представления в текст"""
        lines = []
        
        view_name = view_data.get('view_name', '')
        description = view_data.get('description', '')
        
        lines.append(f"Представление: {view_name}")
        lines.append(f"Описание: {description}")
        lines.append("")
        
        # Колонки
        columns = view_data.get('columns', {})
        if columns:
            lines.append("Колонки:")
            for col, desc in columns.items():
                lines.append(f"  - {col}: {desc}")
            lines.append("")
        
        return "\n".join(lines)
    
    def _format_metadata(self, metadata: Dict) -> str:
        """Форматирование метаданных схемы в текст"""
        lines = []
        
        lines.append(f"База данных: {metadata.get('database', '')}")
        lines.append(f"Схема: {metadata.get('schema', '')}")
        lines.append(f"Тип БД: {metadata.get('database_type', '')}")
        lines.append("")
        
        lines.append(f"Всего таблиц: {metadata.get('total_tables', 0)}")
        lines.append(f"Всего представлений: {metadata.get('total_views', 0)}")
        lines.append("")
        
        # Основные таблицы
        main_tables = metadata.get('main_tables', [])
        if main_tables:
            lines.append("Основные таблицы:")
            for table in main_tables:
                lines.append(f"  - {table}")
            lines.append("")
        
        # Бизнес-домены
        business_domains = metadata.get('business_domains', [])
        if business_domains:
            lines.append("Бизнес-домены:")
            for domain in business_domains:
                lines.append(f"  - {domain}")
        
        return "\n".join(lines)


if __name__ == "__main__":
    # Пример использования
    loader = KBLoader()
    loader.load_all(recreate=True)

