#!/usr/bin/env python3
"""
Агент «Спутниковый библиотекарь» на базе Gemini.
Ведёт себя как квалифицированный инженер по подключению сетей заказчика к спутниковому сегменту:
формирует и курирует базу знаний (Confluence, схемы, инструкции), даёт советы по структуре KB и отвечает
на вопросы по документации.
"""
import os
import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Системный промпт: эксперт-инженер спутникового сегмента и куратор KB; совместная работа с инженером
SYSTEM_PROMPT = """Ты — Спутниковый библиотекарь: квалифицированный инженер в области подключения сетей заказчика к спутниковому сегменту и куратор базы знаний. С тобой в диалоге работает инженер (человек). Вы вместе уточняете, откуда брать документацию и как лучше сформировать базу знаний для работы агента.

Твоя экспертиза:
- Проектирование и пресейл решений: схемы сети спутникового сегмента клиента, терминалы (iDirect, Kingsat, и др.), антенны, модемы, интеграция с наземными сетями.
- Документация: инструкции производителей (iDirect X5, Kingsat P8), draw.io-схемы, типовые конфигурации, чек-листы для инженеров.
- Формирование и поддержка базы знаний: какие страницы Confluence и вложения индексировать, как структурировать разделы.

Совместная работа с инженером:
- Ты и инженер помогаете друг другу: ты задаёшь уточняющие вопросы, когда не хватает данных — например «В каком пространстве Confluence лежат инструкции по iDirect?», «Есть ли у вас схемы в draw.io во вложениях или только ссылки?», «Какие типы документов чаще всего ищут пресейлы?».
- Инженер подсказывает, откуда реально берётся документация (какие пространства, страницы, вложения), что уже выгружено в KB, что ещё стоит добавить.
- На основе ответов инженера ты даёшь конкретные рекомендации: какие страницы/вложения выгрузить, в каком порядке, на что обратить внимание. И наоборот: инженер может спросить тебя «что ещё нужно для KB?» — тогда ты предлагаешь варианты и спрашиваешь, где это у них лежит.

Твои задачи:
1. Помогать формировать KB: что индексировать, откуда (Confluence space/key, конкретные страницы, типы вложений).
2. Отвечать на вопросы по загруженной документации, опираясь на контекст из KB. Если данных нет — сказать об этом и предложить, что добавить, или спросить у инженера, где это искать.
3. В диалоге уточнять друг у друга источники и потребности: задавай короткие вопросы инженеру, когда нужно понять, откуда взять документы или как лучше организовать выгрузку.

Стиль: кратко, по делу. Не стесняйся задавать уточняющие вопросы инженеру — так вы быстрее определите, какую документацию откуда брать для работы агента."""


class SatelliteLibrarianAgent:
    """
    Агент-библиотекарь на Gemini: персона инженера спутникового сегмента + доступ к RAG (Confluence KB).
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        rag_assistant: Optional[Any] = None,
        max_rag_chunks: int = 10,
    ):
        self.api_key = (api_key or os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY", "")).strip()
        self.model = (model or os.getenv("GEMINI_MODEL", "gemini-2.0-flash")).strip()
        self.rag_assistant = rag_assistant
        self.max_rag_chunks = max(1, min(max_rag_chunks, 20))

    def _get_client(self):
        """Ленивая инициализация клиента Gemini."""
        try:
            from google import genai
        except ImportError:
            raise ImportError(
                "Для работы Спутникового библиотекаря установите: pip install google-genai"
            )
        if not self.api_key:
            raise ValueError(
                "Задайте GEMINI_API_KEY или GOOGLE_API_KEY в config.env или переменных окружения."
            )
        return genai.Client(api_key=self.api_key)

    def _get_rag_context(self, query: str) -> str:
        """Подтянуть из KB релевантные чанки Confluence по запросу."""
        if not self.rag_assistant:
            return ""
        try:
            docs = self.rag_assistant.search_semantic(
                query,
                content_type="confluence_section",
                limit=self.max_rag_chunks,
            )
            if not docs:
                docs = self.rag_assistant.search_semantic(
                    query,
                    content_type="confluence_doc",
                    limit=self.max_rag_chunks,
                )
        except Exception as e:
            logger.warning("RAG поиск не удался: %s", e)
            return ""
        if not docs:
            return ""
        parts = []
        for i, d in enumerate(docs, 1):
            title = d.get("title", "")
            section = d.get("section_title", "")
            content = (d.get("content") or "")[:4000]
            score = d.get("similarity", 0)
            parts.append(f"[Фрагмент {i} (релевантность {score:.2%})]\nСтраница: {title}\nСекция: {section}\n\n{content}")
        return "\n\n---\n\n".join(parts)

    def ask(
        self,
        user_message: str,
        use_rag: bool = True,
        rag_query: Optional[str] = None,
        history: Optional[List[Tuple[str, str]]] = None,
    ) -> str:
        """
        Задать вопрос библиотекарю. При use_rag=True в контекст подмешиваются релевантные чанки из KB.
        history: список пар (role, text), role — "user" (инженер) или "model" (библиотекарь), для многократного обмена уточнениями.
        """
        client = self._get_client()
        context_parts = []

        if use_rag and self.rag_assistant:
            q = (rag_query or user_message).strip()
            if q:
                rag_text = self._get_rag_context(q)
                if rag_text:
                    context_parts.append(
                        "Релевантные фрагменты из базы знаний (Confluence):\n\n" + rag_text
                    )

        user_content = user_message.strip()
        if history:
            lines = []
            for role, text in history:
                label = "Инженер" if role == "user" else "Библиотекарь"
                lines.append(f"{label}: {text.strip()}")
            context_parts.append(
                "Предыдущий обмен сообщениями (ты и инженер уточняете друг у друга):\n\n" + "\n\n".join(lines)
            )
        if context_parts:
            user_content = (
                "\n\n".join(context_parts)
                + "\n\n---\n\nСледующее сообщение инженера (ответь и при необходимости задай уточняющий вопрос):\n\n"
                + user_content
            )

        full_prompt = (
            SYSTEM_PROMPT
            + "\n\n[Конец системной инструкции. Далее — контекст и очередное сообщение инженера.]\n\n"
            + user_content
        )
        try:
            kwargs = {"model": self.model, "contents": full_prompt}
            try:
                from google.genai import types
                kwargs["config"] = types.GenerateContentConfig(
                    temperature=0.3,
                    max_output_tokens=4096,
                )
            except (ImportError, AttributeError):
                pass
            response = client.models.generate_content(**kwargs)
        except Exception as e:
            logger.exception("Ошибка вызова Gemini: %s", e)
            return f"Ошибка при обращении к модели: {e}"

        if not response:
            return "Пустой ответ модели. Попробуйте переформулировать вопрос."
        if hasattr(response, "text") and response.text:
            return response.text.strip()
        if getattr(response, "candidates", None):
            c = response.candidates[0]
            if getattr(c, "content", None) and getattr(c.content, "parts", None) and c.content.parts:
                p = c.content.parts[0]
                return (getattr(p, "text", None) or str(p)).strip()
        return "Пустой ответ модели."

    def suggest_what_to_index(self, space_or_pages_description: str) -> str:
        """Рекомендации: что имеет смысл индексировать в KB для заданного пространства или списка страниц."""
        prompt = (
            "Пользователь описывает пространство Confluence или набор страниц для выгрузки в базу знаний. "
            "Дай краткие рекомендации инженера: какие типы страниц и вложений наиболее ценны для пресейлов и инженеров спутникового сегмента, "
            "на что обратить внимание (PDF инструкции, draw.io схемы, таблицы конфигураций), что можно пропустить.\n\n"
            "Описание:\n" + (space_or_pages_description or "(не указано)")
        )
        return self.ask(prompt, use_rag=False)
