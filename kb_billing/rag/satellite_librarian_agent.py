#!/usr/bin/env python3
"""
Агент «Спутниковый библиотекарь» на базе Gemini.
Ведёт себя как квалифицированный инженер по подключению сетей заказчика к спутниковому сегменту:
формирует и курирует базу знаний (Confluence, схемы, инструкции), даёт советы по структуре KB и отвечает
на вопросы по документации.

Доступ к Gemini: через ProxyAPI.ru (по умолчанию), как в brats/kb-service:
- GEMINI_API_KEY или OPENAI_API_KEY — ключ ProxyAPI
- GEMINI_BASE_URL — по умолчанию https://api.proxyapi.ru/google
"""
import os
import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    import httpx
except ImportError:
    httpx = None

# Системный промпт: эксперт-инженер спутникового сегмента и куратор KB; совместная работа с инженером
ENGINEER_PROMPT = """Ты — Спутниковый библиотекарь: квалифицированный инженер в области подключения сетей заказчика к спутниковому сегменту и куратор базы знаний. С тобой в диалоге работает инженер (человек). Вы вместе уточняете, откуда брать документацию и как лучше сформировать базу знаний для работы агента.

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

Изображения и схемы в KB: (1) Если в контексте из базы знаний есть фраза «Изображение/схема — текст по OCR не извлечён» или «для аннотации диаграмм подключите vision API» — вложение проиндексировано без описания; скажи об этом и предложи добавить описание на странице или включить аннотацию при выгрузке. (2) Если в контексте есть связное описание схемы/изображения (например «1) Схема подключения…», «2) Смысловые блоки», «Краткое общее описание», абзацы с перечислением элементов, узлов, связей, Antenna, Gilat, Intellian, SatUnion и т.п.) — это готовое описание из KB (аннотация Gemini Vision); используй его и ответь пользователю именно им. Не предлагай «добавить описание» или «OCR не извлечён», если такое описание уже есть в приведённых фрагментах.

«Нарисуй схему» по ссылке на картинку: если пользователь просит нарисовать/описать схему по URL вложения (например download/attachments/…/filename.png), в контексте ниже будут фрагменты со страницы этой картинки. Найди фрагмент, где секция «Вложение: …» с именем файла из URL и содержание — структурированное описание (блоки, элементы, связи). Это и есть спецификация схемы. Ответь по ней: перечисли объекты и связи из описания и скажи: «Чтобы получить draw.io-схему по этому описанию, нажмите кнопку „Создать draw.io по диалогу“ и вставьте в поле спецификации текст описания из контекста выше» — или кратко воспроизведи описание как спецификацию и подскажи про кнопку. Не перечисляй другие вложения страницы и не пиши «проиндексированы, но OCR не извлечён», если по этой картинке в контексте уже есть связное описание.

Стиль: кратко, по делу. Не стесняйся задавать уточняющие вопросы инженеру — так вы быстрее определите, какую документацию откуда брать для работы агента.

Генерация схем: если инженер просит «создать схему сети» или «нарисовать draw.io», и в контексте уже есть описание схемы (из KB по ссылке) — используй его как спецификацию и подскажи про кнопку «Создать draw.io по диалогу». Если спецификации ещё нет — задай уточняющие вопросы (узлы, связи, типы терминалов). Когда спецификация есть или взята из контекста, подскажи: «Нажмите кнопку „Создать draw.io по диалогу“ — будет сгенерирован файл .drawio.»

Схемы и диаграммы из KB (NMS access, PageHUBs, draw.io, экспорт в PNG): когда в контексте есть описание схемы доступа, топологии или диаграммы (например «Вложение: Gazcom VNO NMS access», «PageHUBs NMS access diagrams», блоки и связи из draw.io) — свободно интерпретируй его: кратко перечисли узлы, связи и смысл схемы, предъяви пользователю структуру (кто к чему подключается, какие роли у элементов). Если пользователь спрашивает «как устроен доступ», «покажи схему» — дай по контексту связное описание и при необходимости укажи ссылку на страницу/вложение в Confluence. Можешь предлагать «по этой схеме из KB» или «ниже описание из базы знаний» и затем давать выжимку. Цель — чтобы инженер мог опираться на твою интерпретацию без обязательного открытия картинки.

Выгрузка/экспорт согласованной спецификации: если инженер просит «выгрузи спецификацию», «экспортируй в Excel/таблицу», «сохрани в файл», «скачай спека_судна» и т.п. — документом для выгрузки является та спецификация, которую вы только что согласовали в этом диалоге (состав оборудования с количествами из предыдущих сообщений). НЕ задавай заново вопросы «для какого судна?», «какую именно спецификацию?» — бери состав из последнего согласованного варианта в переписке. ОБЯЗАТЕЛЬНО сформируй ответ в формате таблицы: (1) первой строкой напиши: «Скопируйте блок ниже и сохраните как .txt или вставьте в Word.» (2) пустая строка (3) заголовок (судно, хаб, дата по желанию) (4) построчно перечень строго в формате «Наименование — N шт.» (наименование, затем тире —, затем число и «шт.») без лишних пояснений. Интерфейс распознает такие строки и выведет спецификацию в виде таблицы на экран и предложит скачать файл в формате .xlsx (Excel) или .docx (Word) — пользователь может попросить «сохрани в Word» и нажать кнопку «Скачать как .docx (Word)». Если в диалоге ещё не было согласованного состава — уточни у инженера один раз, и только после этого выгружай."""

# Персона: абонент/экипаж/полевой пользователь (простые инструкции, без внутренних деталей)
SUBSCRIBER_PROMPT = """Ты — спутниковый ассистент службы поддержки. С тобой в диалоге абонент/экипаж/полевой пользователь.

Твоя задача:
- Помочь восстановить связь или собрать диагностическую информацию простыми шагами.
- Задавать короткие уточняющие вопросы (модель терминала/антенны, что именно не работает, индикация/ошибки, когда началось).
- Давать безопасные пошаговые инструкции: что проверить (питание, кабели, перезагрузка, погодные условия, наличие препятствий, статус на панели).
- Если не хватает данных — честно сказать и попросить конкретные симптомы (коды ошибок, фото экрана, статусные LED).

Ограничения:
- Не раскрывай внутренние ссылки/пространства Confluence и служебные детали. Ссылайся на «инструкцию/руководство» только как на источник, без внутренних URL.
- Если шаги не помогают или есть риск повредить оборудование — остановись и предложи связаться с инженером/ЦУС, перечислив что именно нужно передать (коды ошибок, время, координаты/локация, модель, серийный номер, последние изменения).
- Если пользователь просит описать схему/картинку по ссылке, а в контексте KB есть фраза «Изображение/схема — текст по OCR не извлечён»: не говори «не могу получить доступ к файлу». Скажи, что в базе знаний эта схема есть как вложение, но без текстового описания; предложи связаться с инженером или добавить описание на странице.

Стиль: очень понятно, короткими пунктами, без жаргона. Сначала 3–7 быстрых проверок, затем уточняющие вопросы."""


class SatelliteLibrarianAgent:
    """
    Агент-библиотекарь на Gemini: персона инженера спутникового сегмента + доступ к RAG (Confluence KB).
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        system_prompt: Optional[str] = None,
        rag_assistant: Optional[Any] = None,
        max_rag_chunks: int = 18,
        base_url: Optional[str] = None,
    ):
        self.api_key = (
            (api_key or os.getenv("GEMINI_API_KEY") or os.getenv("OPENAI_API_KEY") or os.getenv("GOOGLE_API_KEY", ""))
            .strip()
        )
        self.model = (model or os.getenv("GEMINI_MODEL", "gemini-2.0-flash")).strip()
        self.system_prompt = (system_prompt or ENGINEER_PROMPT).strip()
        self.rag_assistant = rag_assistant
        self.max_rag_chunks = max(1, min(max_rag_chunks, 30))
        # ProxyAPI.ru по умолчанию (как в brats)
        self.base_url = (base_url or os.getenv("GEMINI_BASE_URL", "https://api.proxyapi.ru/google")).rstrip("/")
        self._http_client: Optional[Any] = None

    def _get_http_client(self):
        """Клиент для вызова Gemini через REST (ProxyAPI.ru)."""
        if not httpx:
            raise ImportError("Установите пакет httpx: pip install httpx")
        if not self.api_key:
            raise ValueError(
                "Задайте GEMINI_API_KEY или OPENAI_API_KEY (ключ ProxyAPI.ru) в config.env или переменных окружения."
            )
        if self._http_client is None:
            self._http_client = httpx.Client(
                base_url=self.base_url,
                timeout=120.0,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
            )
        return self._http_client

    def _ask_via_rest(self, full_prompt: str, system_instruction: Optional[str] = None) -> str:
        """Вызов generateContent через REST (ProxyAPI.ru). system_instruction переопределяет self.system_prompt при необходимости."""
        client = self._get_http_client()
        request_data: Dict[str, Any] = {
            "contents": [{"parts": [{"text": full_prompt}]}],
            "generationConfig": {
                "temperature": 0.3,
                "maxOutputTokens": 8192,
            },
        }
        request_data["systemInstruction"] = {"parts": [{"text": (system_instruction or self.system_prompt)}]}
        endpoint = f"/v1beta/models/{self.model}:generateContent"
        try:
            response = client.post(endpoint, json=request_data)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logger.exception("Ошибка вызова Gemini (REST): %s", e)
            return f"Ошибка при обращении к модели: {e}"
        candidates = data.get("candidates", []) or []
        if not candidates:
            return "Пустой ответ модели. Попробуйте переформулировать вопрос."
        content = candidates[0].get("content", {})
        parts = content.get("parts", []) or []
        if not parts:
            return "Пустой ответ модели."
        text = (parts[0].get("text") or "").strip()
        return text or "Пустой ответ модели."

    def _get_client_sdk(self):
        """Ленивая инициализация клиента google-genai (только если не используем REST)."""
        try:
            from google import genai
        except ImportError as e:
            raise ImportError(
                "Для работы без GEMINI_BASE_URL установите google-genai: pip install google-genai. Ошибка: %s" % e
            )
        if not self.api_key:
            raise ValueError(
                "Задайте GEMINI_API_KEY или GOOGLE_API_KEY в config.env или переменных окружения."
            )
        return genai.Client(api_key=self.api_key)

    def _get_rag_context(self, query: str) -> str:
        """Подтянуть из KB релевантные чанки Confluence. При URL вложения — ищем по page_id, описание этой картинки ставим первым."""
        if not self.rag_assistant:
            return ""
        import re
        page_id_from_url = None
        filename_from_url = None
        m = re.search(r"/download/attachments/(\d+)/([^/?]+)", query, re.IGNORECASE)
        if m:
            page_id_from_url = m.group(1)
            filename_from_url = (m.group(2) or "").strip()
        try:
            docs = []
            if page_id_from_url:
                docs = self.rag_assistant.search_semantic(
                    "схема изображение вложение описание",
                    content_type="confluence_section",
                    limit=self.max_rag_chunks,
                    page_id=page_id_from_url,
                )
                if not docs:
                    docs = self.rag_assistant.get_confluence_chunks_by_page_id(
                        page_id_from_url, limit=max(self.max_rag_chunks, 30)
                    )
                    if not docs:
                        logger.info("RAG: по page_id=%s чанков не найдено (проверьте перезагрузку Confluence в Qdrant)", page_id_from_url)
                # Чанк с описанием именно этой картинки — первым
                if docs and filename_from_url:
                    filename_lower = filename_from_url.lower()
                    found = False
                    for i, d in enumerate(docs):
                        stitle = (d.get("section_title") or "")
                        stitle_lower = stitle.lower()
                        if filename_lower in stitle_lower and ("вложение" in stitle_lower or "attachment" in stitle_lower):
                            docs.insert(0, docs.pop(i))
                            found = True
                            break
                    if not found:
                        # По имени не нашли — ищем чанк в формате Vision (1) ... 2) Смысловые блоки) среди вложений .png
                        for i, d in enumerate(docs):
                            stitle = (d.get("section_title") or "")
                            content = (d.get("content") or "")
                            if ".png" not in stitle.lower() or "вложение" not in stitle.lower():
                                continue
                            if ("1)" in content or "1) " in content) and ("2)" in content or "Смысловые блоки" in content or "смысловые блоки" in content):
                                docs.insert(0, docs.pop(i))
                                break
            # Сначала попытка по точному совпадению названия в section_title (запрос «опишите Спецификация Стар-Т» и т.п.)
            title_match_docs = []
            candidates = []
            if len(query.strip()) <= 120:
                candidates.append(query.strip())
            for prefix in ("опишите ", "расскажи про ", "расскажи о ", "покажи ", "найди ", "что такое ", "про "):
                if prefix in query.lower():
                    c = query.split(prefix, 1)[-1].strip() if prefix in query else ""
                    if not c and prefix.capitalize() in query:
                        c = query.split(prefix.capitalize(), 1)[-1].strip()
                    if len(c) >= 2 and c not in candidates:
                        candidates.append(c)
            for cand in candidates[:3]:  # не более 3 вариантов
                if len(cand) < 2:
                    continue
                title_match_docs = self.rag_assistant.get_confluence_chunks_by_section_title_contains(
                    cand[:80], limit=15
                )
                if title_match_docs:
                    break
            if not title_match_docs:
                docs = self.rag_assistant.search_semantic(
                    query,
                    content_type="confluence_section",
                    limit=self.max_rag_chunks,
                )
            else:
                docs = title_match_docs
                # Добавить семантический поиск для полноты контекста (без дубликатов)
                extra = self.rag_assistant.search_semantic(
                    query,
                    content_type="confluence_section",
                    limit=self.max_rag_chunks,
                )
                seen = {d.get("section_title"): True for d in docs}
                for d in extra:
                    if d.get("section_title") not in seen:
                        docs.append(d)
                        seen[d.get("section_title")] = True
                docs = docs[: self.max_rag_chunks]
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
            head = f"[Фрагмент {i} (релевантность {score:.2%})]\nСтраница: {title}\nСекция: {section}\n\n"
            # Пометить первый фрагмент как описание схемы из KB, если это вложение с текстом в формате Vision
            if i == 1 and page_id_from_url and section:
                sl, ct = section.lower(), (d.get("content") or "")
                if "вложение" in sl and (".png" in sl or ".jpg" in sl) and ("1)" in ct or "Смысловые блоки" in ct):
                    head = "ВАЖНО: Ниже — описание схемы из базы знаний (аннотация Gemini). Используй его для ответа «нарисуй схему» / «опиши схему»; не предлагай «добавить описание».\n\n" + head
            parts.append(head + content)
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
            "[Конец системной инструкции. Далее — контекст и очередное сообщение пользователя.]\n\n"
            + user_content
        )
        # По умолчанию используем ProxyAPI.ru через REST (как в brats)
        if self.base_url and httpx:
            return self._ask_via_rest(full_prompt)
        try:
            client = self._get_client_sdk()
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

    # Минимальный шаблон draw.io для подсказки модели
    _DRAWIO_TEMPLATE = """Формат draw.io: один корневой элемент <mxfile>, внутри <diagram> с <mxGraphModel> и <root>. В <root> лежат <mxCell>:
- Ячейка-родитель: <mxCell id="1" parent="0"/> (родитель 0 — корень модели).
- Вершина (блок): <mxCell id="2" value="Подпись" vertex="1" parent="1"><mxGeometry x="40" y="80" width="120" height="40" as="geometry"/></mxCell>
- Связь: <mxCell id="3" edge="1" source="2" target="4" parent="1"><mxGeometry relative="1" as="geometry"/></mxCell>
ID должны быть уникальными целыми (1 — корень, 2,3,4... для блоков и связей). source/target — id вершин."""

    def generate_drawio(
        self,
        specification: str,
        history: Optional[List[Tuple[str, str]]] = None,
    ) -> str:
        """
        Сгенерировать файл draw.io (XML) по спецификации пользователя и диалогу.
        После уточняющих вопросов пользователь описывает схему — модель возвращает валидный XML draw.io.
        Возвращает строку с содержимым .drawio файла (или сообщение об ошибке).
        """
        parts = [
            "Задача: по спецификации и диалогу ниже сгенерируй один полный XML-файл draw.io (схема сети или диаграмма).",
            "Требования:",
            "1) Верни ТОЛЬКО содержимое файла — валидный XML, без пояснений до и после.",
            "2) Структура: <mxfile> (host=\"app.diagrams.net\") → <diagram> → <mxGraphModel> → <root> → набор <mxCell>.",
            "3) Вершины (блоки): mxCell с vertex=\"1\", value=\"подпись\", <mxGeometry x=\"...\" y=\"...\" width=\"...\" height=\"...\" as=\"geometry\"/>.",
            "4) Связи: mxCell с edge=\"1\", source=\"id_вершины\", target=\"id_вершины\", parent=\"1\", <mxGeometry relative=\"1\" as=\"geometry\"/>.",
            "5) Уникальные id (числа): 1 — корень, далее 2, 3, 4... для блоков и рёбер. Располагай блоки так, чтобы схема была читаемой.",
            "6) Для схем сети используй подписи типа: VSAT, терминал, антенна, шлюз, ЦУС, клиент и т.д. — по спецификации.",
            self._DRAWIO_TEMPLATE,
            "",
            "Спецификация и диалог:",
            specification.strip(),
        ]
        if history:
            lines = []
            for role, text in history:
                label = "Инженер" if role == "user" else "Ассистент"
                lines.append(f"{label}: {text.strip()}")
            parts.append("\nДиалог (уточняющие вопросы и ответы):\n" + "\n\n".join(lines))
        full_prompt = "\n".join(parts)
        drawio_system = (
            "Ты генерируешь только валидный XML файл draw.io по спецификации пользователя. "
            "Не добавляй пояснений — только один полный документ в формате mxfile/diagram/mxGraphModel/root/mxCell."
        )
        reply = ""
        if self.base_url and httpx:
            reply = self._ask_via_rest(full_prompt, system_instruction=drawio_system)
        if not reply:
            try:
                client = self._get_client_sdk()
                reply = (client.models.generate_content(model=self.model, contents=full_prompt).text or "").strip()
            except Exception as e:
                logger.exception("generate_drawio: %s", e)
                return "Ошибка генерации: " + str(e)
        if not reply:
            return "Модель не вернула содержимое. Попробуйте уточнить спецификацию."
        # Убрать обёртку ```xml ... ``` если есть
        import re
        m = re.search(r"```(?:xml)?\s*([\s\S]*?)```", reply)
        if m:
            reply = m.group(1).strip()
        if not reply.strip().startswith("<"):
            return "Модель вернула не XML. Ответ:\n" + reply[:500]
        return reply.strip()
