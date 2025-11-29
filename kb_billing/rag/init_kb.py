#!/usr/bin/env python3
"""
Скрипт инициализации KB_billing в Qdrant
"""

import sys
import os
from pathlib import Path

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from kb_billing.rag.kb_loader import KBLoader
import argparse


def main():
    parser = argparse.ArgumentParser(description="Инициализация KB_billing в Qdrant")
    parser.add_argument(
        "--host",
        default="localhost",
        help="Хост Qdrant сервера (по умолчанию: localhost)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=6333,
        help="Порт Qdrant сервера (по умолчанию: 6333)"
    )
    parser.add_argument(
        "--collection",
        default="kb_billing",
        help="Имя коллекции (по умолчанию: kb_billing)"
    )
    parser.add_argument(
        "--model",
        default="intfloat/multilingual-e5-base",
        help="Модель эмбеддингов (по умолчанию: intfloat/multilingual-e5-base)"
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Пересоздать коллекцию (удалить существующую)"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Инициализация KB_billing в Qdrant")
    print("=" * 60)
    print(f"Qdrant: {args.host}:{args.port}")
    print(f"Коллекция: {args.collection}")
    print(f"Модель: {args.model}")
    print(f"Пересоздать: {args.recreate}")
    print("=" * 60)
    print()
    
    try:
        loader = KBLoader(
            qdrant_host=args.host,
            qdrant_port=args.port,
            collection_name=args.collection,
            embedding_model=args.model
        )
        
        loader.load_all(recreate=args.recreate)
        
        print()
        print("=" * 60)
        print("✅ Инициализация завершена успешно!")
        print("=" * 60)
        
    except Exception as e:
        print()
        print("=" * 60)
        print(f"❌ Ошибка при инициализации: {e}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

