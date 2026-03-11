#!/usr/bin/env python3
"""
Тест способности биллинг-ассистента отвечать на запрос:
«5 самых доходных клиентов за февраль 2026 года»

Проверяет:
1. Инициализацию RAGAssistant (Qdrant доступен, KB загружена).
2. Получение контекста по вопросу (примеры/таблицы из KB).
3. Генерацию SQL через LLM (если задан OPENAI_API_KEY) и валидность результата.

Запускать на сервере (deploy), где есть Qdrant, config.env с OPENAI_API_KEY и обученная KB.
Для proxyapi.ru в config.env задайте: OPENAI_API_BASE=https://api.proxyapi.ru/openai/v1
Локально тест обычно пропускается из‑за отсутствия зависимостей/сервисов.

На сервере из корня проекта (или из deploy/):
  python -m tests.test_billing_assistant_top5
  python tests/test_billing_assistant_top5.py
"""
import os
import sys
from pathlib import Path

# Корень проекта
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
os.chdir(project_root)

# Загрузка config.env при наличии
config_env = project_root / "config.env"
if config_env.exists():
    from dotenv import load_dotenv
    load_dotenv(config_env)

QUESTION = "5 самых доходных клиентов за февраль 2026 года"


def _check_deps():
    """Проверка наличия зависимостей RAG (Qdrant, эмбеддинги). При отсутствии — пропуск с кодом 0."""
    try:
        import qdrant_client  # noqa: F401
        import sentence_transformers  # noqa: F401
        return True
    except ImportError as e:
        print("Пропуск теста: не установлены зависимости RAG (qdrant-client, sentence-transformers).")
        print("  Установите: pip install qdrant-client sentence-transformers")
        print(f"  Ошибка: {e}")
        return False


def _check_openai():
    """Проверка пакета openai для генерации SQL через LLM (в т.ч. proxyapi.ru)."""
    try:
        import openai  # noqa: F401
        return True
    except ImportError:
        return False


def test_rag_assistant_top5():
    """Проверка ответа ассистента на запрос про топ-5 доходных клиентов за февраль 2026."""
    if not _check_deps():
        return True, []  # skip, не считаем ошибкой

    errors = []
    # 1) Инициализация RAGAssistant
    try:
        from kb_billing.rag.config_sql4a import SQL4AConfig
        from kb_billing.rag.rag_assistant import RAGAssistant

        qdrant_host = os.getenv("QDRANT_HOST", SQL4AConfig.QDRANT_HOST)
        qdrant_port = int(os.getenv("QDRANT_PORT", SQL4AConfig.QDRANT_PORT))
        assistant = RAGAssistant(qdrant_host=qdrant_host, qdrant_port=qdrant_port)
        print("✓ RAGAssistant инициализирован (Qdrant доступен)")
    except Exception as e:
        errors.append(f"Инициализация RAGAssistant: {e}")
        print(f"✗ RAGAssistant не инициализирован: {e}")
        print("  Убедитесь, что Qdrant запущен: docker run -p 6333:6333 qdrant/qdrant")
        return False, errors

    # 2) Контекст по вопросу
    try:
        context = assistant.get_context_for_sql_generation(QUESTION, max_examples=5)
        examples = (context or {}).get("examples") or []
        tables = (context or {}).get("tables") or []
        print(f"✓ Контекст получен: примеров={len(examples)}, таблиц/представлений={len(tables)}")
        if not examples and not tables:
            errors.append("Контекст пустой: нет примеров и таблиц из KB")
    except Exception as e:
        errors.append(f"Получение контекста: {e}")
        print(f"✗ Ошибка получения контекста: {e}")
        return False, errors

    # 3) Генерация SQL через LLM (если есть API ключ; proxyapi.ru: OPENAI_API_BASE=https://api.proxyapi.ru/openai/v1)
    api_key = os.getenv("OPENAI_API_KEY")
    api_base = os.getenv("OPENAI_BASE_URL") or os.getenv("OPENAI_API_BASE")
    if api_base:
        print(f"  API (прокси): {api_base}")

    if not api_key:
        print("⊘ OPENAI_API_KEY не задан — проверка генерации SQL через LLM пропущена")
        return len(errors) == 0, errors

    if not _check_openai():
        errors.append("Пакет openai не установлен — генерация SQL через LLM недоступна.")
        print("✗ Пакет openai не установлен. Установите: pip install openai")
        print("  Для proxyapi.ru в config.env задайте: OPENAI_API_BASE=https://api.proxyapi.ru/openai/v1")
        return False, errors

    try:
        generated_sql = assistant.generate_sql_with_llm(
            question=QUESTION,
            context=context,
            api_key=api_key,
            api_base=api_base,
        )
    except Exception as e:
        errors.append(f"Генерация SQL через LLM: {e}")
        print(f"✗ Исключение при генерации SQL: {e}")
        return False, errors

    if not (generated_sql and generated_sql.strip()):
        errors.append("LLM вернул пустой SQL")
        print("✗ LLM вернул пустой SQL")
        return False, errors

    sql_upper = generated_sql.strip().upper()
    if not any(sql_upper.startswith(kw) for kw in ("SELECT", "WITH")):
        errors.append(f"SQL не начинается с SELECT/WITH: {generated_sql[:200]}...")
        print("✗ SQL не начинается с SELECT или WITH")
        return False, errors
    if "SELECT" in sql_upper and "FROM" not in sql_upper:
        errors.append("SQL SELECT без FROM")
        print("✗ В SQL нет ключевого слова FROM")
        return False, errors

    print("✓ Сгенерирован валидный SQL (начинается с SELECT/WITH, есть FROM)")
    print("\n--- Сгенерированный SQL (первые 800 символов) ---")
    print(generated_sql[:800])
    if len(generated_sql) > 800:
        print("...")
    print("---")
    return len(errors) == 0, errors


if __name__ == "__main__":
    print(f"Вопрос: «{QUESTION}»\n")
    ok, errs = test_rag_assistant_top5()
    if errs:
        print("\nОшибки:")
        for e in errs:
            print("  -", e)
    sys.exit(0 if ok else 1)
