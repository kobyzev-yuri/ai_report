#!/usr/bin/env python3
"""
Пример адаптации benchmark из sql4A для Oracle billing
Демонстрирует методологию тестирования KB независимо от векторной БД
"""

import asyncio
import json
from typing import List, Dict, Any, Tuple
from pathlib import Path

# Пример функции сравнения результатов для Oracle
async def compare_oracle_results(
    expected_sql: str,
    generated_sql: str,
    oracle_conn
) -> bool:
    """
    Сравнение результатов выполнения SQL в Oracle
    
    Args:
        expected_sql: Эталонный SQL
        generated_sql: Сгенерированный SQL
        oracle_conn: Подключение к Oracle
        
    Returns:
        True если результаты эквивалентны
    """
    try:
        # Выполняем эталонный SQL
        cursor = oracle_conn.cursor()
        cursor.execute(expected_sql)
        expected_rows = cursor.fetchall()
        expected_columns = [desc[0] for desc in cursor.description]
        
        # Выполняем сгенерированный SQL
        cursor.execute(generated_sql)
        generated_rows = cursor.fetchall()
        generated_columns = [desc[0] for desc in cursor.description]
        
        # Нормализация: преобразуем в множества для сравнения
        # (порядок строк не важен)
        expected_set = {
            tuple(str(v) for v in row) 
            for row in expected_rows
        }
        generated_set = {
            tuple(str(v) for v in row) 
            for row in generated_rows
        }
        
        # Сравнение множеств
        return expected_set == generated_set
        
    except Exception as e:
        print(f"Ошибка сравнения: {e}")
        return False


async def benchmark_kb_billing(
    test_questions: List[Dict[str, Any]],
    search_function,  # Функция поиска в векторной БД (независимо от типа)
    generate_sql_function,  # Функция генерации SQL
    oracle_conn,
    top_k: int = 5
) -> Dict[str, Any]:
    """
    Benchmark для kb_billing
    
    Методология из sql4A, адаптированная для Oracle
    
    Args:
        test_questions: Список тестовых вопросов с эталонными SQL
        search_function: Функция поиска в KB (семантический поиск)
        generate_sql_function: Функция генерации SQL (LLM + RAG)
        oracle_conn: Подключение к Oracle
        top_k: Количество результатов для поиска
        
    Returns:
        Dict с метриками и рекомендациями
    """
    results = {
        "total": 0,
        "correct": 0,
        "accuracy": 0.0,
        "top_k_hit_rate": 0.0,
        "details": [],
        "recommendations": []
    }
    
    hit_count = 0
    
    for question_data in test_questions:
        question = question_data["question"]
        expected_sql = question_data["sql"]
        category = question_data.get("category", "Общее")
        
        results["total"] += 1
        
        # 1. Поиск релевантных примеров в KB
        relevant_examples = await search_function(
            query=question,
            content_type="qa_example",
            category=category,
            top_k=top_k
        )
        
        # Проверка hit rate: есть ли эталонный SQL в результатах поиска?
        hit = any(
            expected_sql in ex.get("content", "") or 
            expected_sql in ex.get("metadata", {}).get("sql", "")
            for ex in relevant_examples
        )
        if hit:
            hit_count += 1
        
        # 2. Генерация SQL с контекстом
        context = format_rag_context(relevant_examples)
        generated_sql = await generate_sql_function(
            question=question,
            context=context
        )
        
        # 3. Валидация SQL
        is_correct = await compare_oracle_results(
            expected_sql=expected_sql,
            generated_sql=generated_sql,
            oracle_conn=oracle_conn
        )
        
        if is_correct:
            results["correct"] += 1
        
        # Сохраняем детали
        results["details"].append({
            "question": question,
            "category": category,
            "expected_sql": expected_sql,
            "generated_sql": generated_sql,
            "is_correct": is_correct,
            "hit_in_top_k": hit,
            "relevant_examples_count": len(relevant_examples)
        })
    
    # Расчет метрик
    results["accuracy"] = results["correct"] / results["total"] if results["total"] > 0 else 0.0
    results["top_k_hit_rate"] = hit_count / results["total"] if results["total"] > 0 else 0.0
    
    # Генерация рекомендаций
    results["recommendations"] = generate_recommendations(results["details"])
    
    return results


def format_rag_context(relevant_examples: List[Dict[str, Any]]) -> str:
    """
    Форматирование контекста для RAG из результатов поиска
    
    Независимо от типа векторной БД
    """
    context_parts = []
    
    for ex in relevant_examples:
        content = ex.get("content", "")
        metadata = ex.get("metadata", {})
        content_type = ex.get("content_type", "")
        
        if content_type == "qa_example":
            # Извлекаем вопрос и SQL из content или metadata
            question = metadata.get("question", "")
            sql = metadata.get("sql", "")
            if not sql and "A:" in content:
                parts = content.split("A:", 1)
                if len(parts) == 2:
                    question = parts[0].replace("Q:", "").strip()
                    sql = parts[1].strip()
            
            context_parts.append(f"Пример:\nQ: {question}\nA: {sql}")
        
        elif content_type == "documentation":
            context_parts.append(f"Документация:\n{content}")
        
        elif content_type == "ddl":
            context_parts.append(f"DDL:\n{content}")
    
    return "\n\n".join(context_parts)


def generate_recommendations(details: List[Dict[str, Any]]) -> List[str]:
    """
    Генерация рекомендаций на основе анализа ошибок
    
    Методология из sql4A
    """
    recommendations = []
    
    # Анализ по категориям
    category_errors = {}
    for detail in details:
        if not detail["is_correct"]:
            category = detail["category"]
            category_errors[category] = category_errors.get(category, 0) + 1
    
    # Рекомендации по категориям с ошибками
    for category, error_count in category_errors.items():
        recommendations.append(
            f"Добавить больше примеров для категории '{category}' "
            f"({error_count} ошибок)"
        )
    
    # Анализ hit rate
    low_hit_rate = [
        d for d in details 
        if not d["hit_in_top_k"] and d["relevant_examples_count"] < 3
    ]
    if low_hit_rate:
        recommendations.append(
            f"Улучшить качество поиска для {len(low_hit_rate)} вопросов "
            f"(низкий hit rate)"
        )
    
    # Анализ синтаксических ошибок
    syntax_errors = [
        d for d in details 
        if not d["is_correct"] and "ORA-" in str(d.get("error", ""))
    ]
    if syntax_errors:
        recommendations.append(
            f"Добавить больше примеров с правильным синтаксисом Oracle "
            f"({len(syntax_errors)} синтаксических ошибок)"
        )
    
    return recommendations


# Пример использования
async def main():
    """
    Пример запуска benchmark для kb_billing
    """
    # Загрузка тестовых вопросов из kb_billing
    kb_dir = Path("/mnt/ai/cnn/ai_report/kb_billing")
    training_file = kb_dir / "training_data" / "sql_examples.json"
    
    with open(training_file) as f:
        test_questions = json.load(f)
    
    # Ограничиваем для примера
    test_questions = test_questions[:10]
    
    # Функции поиска и генерации (зависит от реализации)
    # Здесь показаны сигнатуры, реализация зависит от векторной БД
    
    async def search_kb(query: str, content_type: str, category: str, top_k: int):
        """
        Пример функции поиска (адаптировать под конкретную векторную БД)
        """
        # Реализация зависит от векторной БД:
        # - pgvector: SQL запрос с векторным поиском
        # - Qdrant: API запрос с фильтрами
        # - Chroma: query с where фильтрами
        pass
    
    async def generate_sql(question: str, context: str):
        """
        Пример функции генерации SQL (LLM + RAG)
        """
        # Использовать LLM для генерации SQL с контекстом
        pass
    
    # Подключение к Oracle
    import cx_Oracle
    oracle_conn = cx_Oracle.connect(
        user=os.getenv("ORACLE_USER"),
        password=os.getenv("ORACLE_PASSWORD"),
        dsn=cx_Oracle.makedsn(
            os.getenv("ORACLE_HOST"),
            int(os.getenv("ORACLE_PORT", "1521")),
            service_name=os.getenv("ORACLE_SERVICE")
        )
    )
    
    # Запуск benchmark
    results = await benchmark_kb_billing(
        test_questions=test_questions,
        search_function=search_kb,
        generate_sql_function=generate_sql,
        oracle_conn=oracle_conn,
        top_k=5
    )
    
    # Вывод результатов
    print(f"""
Результаты benchmark для kb_billing:
- Всего вопросов: {results['total']}
- Правильных ответов: {results['correct']}
- Accuracy: {results['accuracy']:.2%}
- Top-k Hit Rate: {results['top_k_hit_rate']:.2%}

Рекомендации:
{chr(10).join(f"- {r}" for r in results['recommendations'])}
    """)
    
    # Сохранение отчета
    report_file = Path("kb_billing_benchmark_report.json")
    with open(report_file, 'w') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"Отчет сохранен: {report_file}")


if __name__ == "__main__":
    asyncio.run(main())















