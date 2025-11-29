#!/usr/bin/env python3
"""
Конфигурация для RAG системы на основе sql4A
Использует те же настройки и модели, что и sql4A для совместимости
"""

import os
from pathlib import Path
from typing import Optional


class SQL4AConfig:
    """
    Конфигурация на основе sql4A проекта
    Использует те же модели эмбеддингов и настройки для русского языка
    Ориентирована на Oracle billing schema (STECCOM)
    """
    
    # ========== Модель эмбеддингов (как в sql4A) ==========
    # sql4A использует multilingual-e5-base для русского языка
    EMBEDDING_MODEL = os.getenv(
        "EMBEDDING_MODEL",
        "intfloat/multilingual-e5-base"  # 768 размерность, как в sql4A
    )
    
    EMBEDDING_DIMENSION = 768  # Размерность для multilingual-e5-base
    
    # Альтернативные модели (если нужны)
    EMBEDDING_MODEL_LARGE = "intfloat/multilingual-e5-large"  # 1024 размерность
    EMBEDDING_MODEL_TINY = "cointegrated/rubert-tiny2"  # 312 размерность
    
    # ========== Векторная БД (Qdrant) ==========
    # Используем Qdrant для векторного поиска (независимо от основной БД)
    QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
    QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
    QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "kb_billing")
    
    # ========== Oracle Database (основная БД для выполнения SQL) ==========
    # Oracle используется для выполнения SQL запросов, сгенерированных RAG системой
    ORACLE_USER = os.getenv("ORACLE_USER", "")
    ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "")
    ORACLE_HOST = os.getenv("ORACLE_HOST", "localhost")
    ORACLE_PORT = int(os.getenv("ORACLE_PORT", "1521"))
    ORACLE_SID = os.getenv("ORACLE_SID", "")
    ORACLE_SERVICE = os.getenv("ORACLE_SERVICE") or os.getenv("ORACLE_SID", "")
    ORACLE_SCHEMA = os.getenv("ORACLE_SCHEMA", "billing")  # Схема billing для STECCOM
    
    @classmethod
    def get_oracle_dsn(cls) -> str:
        """Получение DSN для Oracle (для выполнения SQL запросов)"""
        if cls.ORACLE_SID:
            return f"{cls.ORACLE_HOST}:{cls.ORACLE_PORT}/{cls.ORACLE_SID}"
        else:
            return f"{cls.ORACLE_HOST}:{cls.ORACLE_PORT}/{cls.ORACLE_SERVICE}"
    
    @classmethod
    def get_oracle_connection_string(cls) -> str:
        """Получение строки подключения для Oracle"""
        return f"{cls.ORACLE_USER}/{cls.ORACLE_PASSWORD}@{cls.get_oracle_dsn()}"
    
    # ========== Настройки поиска (как в sql4A) ==========
    DEFAULT_SEARCH_LIMIT = int(os.getenv("DEFAULT_SEARCH_LIMIT", "5"))
    SIMILARITY_THRESHOLD = float(os.getenv("SIMILARITY_THRESHOLD", "0.7"))
    
    # ========== Настройки генерации эмбеддингов (как в sql4A) ==========
    BATCH_SIZE = int(os.getenv("EMBEDDING_BATCH_SIZE", "32"))
    NORMALIZE_EMBEDDINGS = True  # Как в sql4A
    
    # ========== Пути к KB ==========
    KB_DIR = Path(__file__).parent.parent
    
    # ========== Настройки для генерации SQL ==========
    MAX_CONTEXT_EXAMPLES = int(os.getenv("MAX_CONTEXT_EXAMPLES", "5"))
    MAX_CONTEXT_TOKENS = int(os.getenv("MAX_CONTEXT_TOKENS", "2000"))
    
    # ========== Логирование ==========
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    @classmethod
    def load_from_env_file(cls, env_file: Optional[Path] = None):
        """
        Загрузка конфигурации из файла .env или config.env
        (как в sql4A)
        """
        if env_file is None:
            # Ищем config.env в корне проекта
            project_root = Path(__file__).parent.parent.parent
            env_file = project_root / "config.env"
        
        if env_file.exists():
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip().strip('"\'')
                        os.environ[key] = value
        
        # Обновляем конфигурацию после загрузки env файла
        cls.QDRANT_HOST = os.getenv("QDRANT_HOST", cls.QDRANT_HOST)
        cls.QDRANT_PORT = int(os.getenv("QDRANT_PORT", str(cls.QDRANT_PORT)))
        cls.QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", cls.QDRANT_COLLECTION)
        cls.ORACLE_USER = os.getenv("ORACLE_USER", cls.ORACLE_USER)
        cls.ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", cls.ORACLE_PASSWORD)
        cls.ORACLE_HOST = os.getenv("ORACLE_HOST", cls.ORACLE_HOST)
        cls.ORACLE_PORT = int(os.getenv("ORACLE_PORT", str(cls.ORACLE_PORT)))
        cls.ORACLE_SID = os.getenv("ORACLE_SID", cls.ORACLE_SID)
        cls.ORACLE_SERVICE = os.getenv("ORACLE_SERVICE") or os.getenv("ORACLE_SID", cls.ORACLE_SERVICE)
        cls.ORACLE_SCHEMA = os.getenv("ORACLE_SCHEMA", cls.ORACLE_SCHEMA)
        cls.EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", cls.EMBEDDING_MODEL)
    
    @classmethod
    def get_config_summary(cls) -> dict:
        """Получение сводки конфигурации"""
        return {
            "embedding_model": cls.EMBEDDING_MODEL,
            "embedding_dimension": cls.EMBEDDING_DIMENSION,
            "qdrant": {
                "host": cls.QDRANT_HOST,
                "port": cls.QDRANT_PORT,
                "collection": cls.QDRANT_COLLECTION
            },
            "oracle": {
                "host": cls.ORACLE_HOST,
                "port": cls.ORACLE_PORT,
                "schema": cls.ORACLE_SCHEMA,
                "user": cls.ORACLE_USER,
                "sid": cls.ORACLE_SID or cls.ORACLE_SERVICE
            },
            "search": {
                "default_limit": cls.DEFAULT_SEARCH_LIMIT,
                "similarity_threshold": cls.SIMILARITY_THRESHOLD
            }
        }


# Загружаем конфигурацию при импорте
SQL4AConfig.load_from_env_file()

