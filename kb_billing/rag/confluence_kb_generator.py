#!/usr/bin/env python3
"""
Генератор KB из Confluence: получение страниц → парсинг Storage + вложения (PDF, DOCX, изображения) → единый формат KB → JSON.
Сектор спутниковых систем: полная индексация контента — тело страницы и все вложения с контролируемой обработкой.
"""
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from kb_billing.rag.confluence_client import ConfluenceClient
from kb_billing.rag.attachment_parsers import parse_attachment

logger = logging.getLogger(__name__)

# Максимальный размер вложения для скачивания (байт), по умолчанию 50 MB
DEFAULT_MAX_ATTACHMENT_SIZE = 50 * 1024 * 1024
# Максимальная длина извлечённого текста из одного вложения (символы)
DEFAULT_MAX_ATTACHMENT_TEXT_LENGTH = 500_000
# Служебные/временные форматы Confluence — не скачивать и не индексировать
ATTACHMENT_SKIP_EXTENSIONS = {".tmp", ".render", ".tfss"}


def _strip_html_to_text(html: str) -> str:
    """Удаление тегов и лишних пробелов, замена блочных элементов на переносы."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _parse_storage_to_content(storage_html: str) -> List[Dict[str, Any]]:
    """
    Парсинг Confluence Storage (XHTML-подобный) в структуру content[]:
    [{ "title": "...", "text": "...", "subsections": [...] }].
    Заголовки h1–h6 задают уровни секций.
    """
    if not storage_html:
        return [{"title": "", "text": "", "subsections": []}]
    soup = BeautifulSoup(storage_html, "html.parser")
    root_sections: List[Dict[str, Any]] = []
    stack: List[Dict[str, Any]] = []  # (level, section); level 0 = h1, 1 = h2, ...
    current_text: List[str] = []
    current_level = -1

    def flush_text(section: Dict[str, Any]) -> None:
        t = "\n".join(current_text).strip()
        if t:
            section["text"] = t
        current_text.clear()

    def make_section(title: str) -> Dict[str, Any]:
        return {"title": title, "text": "", "subsections": []}

    for node in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "ul", "ol", "table"]):
        if node.name in ("h1", "h2", "h3", "h4", "h5", "h6"):
            level = int(node.name[1]) - 1  # 0..5
            heading_text = _strip_html_to_text(str(node))
            if not heading_text:
                continue
            new_section = make_section(heading_text)
            # Закрываем текущий блок текста у текущей секции
            if stack:
                flush_text(stack[-1]["section"])
            # Поднимаемся по стеку до уровня родителя
            while stack and stack[-1]["level"] >= level:
                stack.pop()
            if not stack:
                root_sections.append(new_section)
                stack.append({"level": level, "section": new_section})
            else:
                stack[-1]["section"]["subsections"].append(new_section)
                stack.append({"level": level, "section": new_section})
            current_level = level
        else:
            # Текст/таблица в текущую секцию
            if stack:
                current_text.append(_strip_html_to_text(node.get_text(separator="\n")))
            else:
                root_sections.append(make_section(""))
                stack.append({"level": -1, "section": root_sections[-1]})
                current_text.append(_strip_html_to_text(node.get_text(separator="\n")))

    if stack:
        flush_text(stack[-1]["section"])

    # Если парсер не нашёл заголовков — один блок с полным текстом
    if not root_sections:
        root_sections = [{"title": "", "text": _strip_html_to_text(storage_html), "subsections": []}]
    return root_sections


def _content_to_plain_text(content: List[Dict[str, Any]]) -> str:
    """Сведение content[] в один текст для эмбеддинга и поиска."""
    parts: List[str] = []

    def walk(sections: List[Dict[str, Any]], prefix: str = "") -> None:
        for s in sections:
            if s.get("title"):
                parts.append(f"{prefix}{s['title']}")
            if s.get("text"):
                parts.append(s["text"])
            walk(s.get("subsections", []), prefix=prefix + "  ")

    walk(content)
    return "\n\n".join(parts).strip()


class ConfluenceKBGenerator:
    """Confluence → формат KB (JSON) с полной индексацией: тело страницы + вложения (PDF, DOCX, изображения OCR)."""

    def __init__(
        self,
        client: Optional[ConfluenceClient] = None,
        output_dir: Optional[Path] = None,
        default_audience: Optional[List[str]] = None,
        default_scope: Optional[List[str]] = None,
        default_status: str = "reference",
        index_attachments: bool = True,
        max_attachment_size: int = DEFAULT_MAX_ATTACHMENT_SIZE,
        max_attachment_text_length: int = DEFAULT_MAX_ATTACHMENT_TEXT_LENGTH,
    ):
        self.client = client or ConfluenceClient()
        self.output_dir = Path(output_dir) if output_dir else Path(__file__).parent.parent / "confluence_docs"
        self.default_audience = default_audience or ["user", "admin"]
        self.default_scope = default_scope or ["general"]
        self.default_status = default_status
        self.index_attachments = index_attachments
        self.max_attachment_size = max_attachment_size
        self.max_attachment_text_length = max_attachment_text_length
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _process_attachments(
        self, page_id: str, page_title: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Скачать вложения страницы, извлечь текст парсерами (PDF, DOCX, Draw.io, изображения OCR + Gemini Vision).
        page_title передаётся в vision-аннотацию изображений как контекст.
        Возвращает (список секций для content[], список записей для attachments_processed).
        """
        sections: List[Dict[str, Any]] = []
        processed: List[Dict[str, Any]] = []
        if not self.index_attachments or not page_id:
            return sections, processed
        try:
            attachments = self.client.get_page_attachments(page_id)
        except Exception as e:
            logger.warning("Не удалось получить вложения страницы %s: %s", page_id, e)
            return sections, processed
        for att in attachments:
            title_att = att.get("title") or "вложение"
            _ext_match = re.search(r"\.[a-z0-9]+$", (title_att or "").lower())
            ext = _ext_match.group(0) if _ext_match else ""
            if ext in ATTACHMENT_SKIP_EXTENSIONS:
                processed.append({"filename": title_att, "status": "skipped", "reason": "служебный формат Confluence"})
                continue
            size = att.get("extensions", {}).get("fileSize") or 0
            if size and size > self.max_attachment_size:
                logger.info("Пропуск вложения %s: размер %s > %s", title_att, size, self.max_attachment_size)
                sections.append({
                    "title": f"Вложение: {title_att}",
                    "text": f"[Файл не индексирован: размер {size} байт превышает лимит]",
                    "subsections": [],
                })
                processed.append({"filename": title_att, "status": "skipped", "reason": "размер превышает лимит"})
                continue
            data = self.client.download_attachment(att)
            if not data:
                sections.append({
                    "title": f"Вложение: {title_att}",
                    "text": "[Не удалось скачать файл]",
                    "subsections": [],
                })
                processed.append({"filename": title_att, "status": "error", "reason": "не удалось скачать"})
                continue
            if len(data) > self.max_attachment_size:
                logger.info("Пропуск индексации %s: размер после скачивания %s", title_att, len(data))
                sections.append({
                    "title": f"Вложение: {title_att}",
                    "text": f"[Файл не индексирован: размер {len(data)} байт превышает лимит]",
                    "subsections": [],
                })
                processed.append({"filename": title_att, "status": "skipped", "reason": "размер превышает лимит"})
                continue
            content_type = (att.get("extensions") or {}).get("mediaType") or ""
            text, err = parse_attachment(
                data, title_att, content_type, context_text=page_title
            )
            if err:
                sections.append({
                    "title": f"Вложение: {title_att}",
                    "text": f"[Ошибка извлечения текста: {err}]",
                    "subsections": [],
                })
                processed.append({"filename": title_att, "status": "error", "reason": err})
                continue
            if len(text) > self.max_attachment_text_length:
                text = text[: self.max_attachment_text_length] + "\n\n[... текст обрезан по лимиту ...]"
            sections.append({
                "title": f"Вложение: {title_att}",
                "text": text,
                "subsections": [],
            })
            processed.append({"filename": title_att, "status": "indexed", "text_length": len(text)})
        return sections, processed

    def page_to_kb_doc(self, page: Dict[str, Any], base_url: str) -> Dict[str, Any]:
        """Преобразование одной страницы Confluence в документ формата KB (тело страницы + вложения)."""
        page_id = str(page.get("id", "") or "").strip()
        title = page.get("title", "Без названия")
        storage = self.client.get_page_content_storage(page)
        version_date = self.client.get_page_version_date(page)
        link = f"{base_url}/pages/viewpage.action?pageId={page_id}" if base_url else ""
        content = _parse_storage_to_content(storage)
        attachment_sections, attachments_processed = self._process_attachments(
            page_id, page_title=title
        )
        if attachment_sections:
            content = content + attachment_sections
        doc: Dict[str, Any] = {
            "title": title,
            "audience": self.default_audience,
            "scope": self.default_scope,
            "status": self.default_status,
            "content": content,
            "source": {
                "type": "confluence",
                "url": link,
                "page_id": page_id,
                "last_updated": version_date,
            },
        }
        if attachments_processed:
            doc["attachments_processed"] = attachments_processed
        return doc

    def sync_space(
        self,
        space_key: str,
        base_url: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Синхронизация пространства: скачать страницы, преобразовать в KB, сохранить в JSON.
        Возвращает список созданных документов KB.
        """
        space_key = (space_key or "").strip()
        if not space_key:
            return []
        base_url = base_url or self.client.base_url
        docs: List[Dict[str, Any]] = []
        count = 0
        for page in self.client.get_pages_in_space(space_key):
            try:
                doc = self.page_to_kb_doc(page, base_url)
                docs.append(doc)
                count += 1
                if limit is not None and count >= limit:
                    break
            except Exception as e:
                logger.warning("Ошибка обработки страницы %s: %s", page.get("id"), e)
        out_file = self.output_dir / f"confluence_{space_key}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(docs, f, ensure_ascii=False, indent=2)
        logger.info("Сохранено %s документов в %s", len(docs), out_file)
        return docs

    def sync_page_ids(
        self,
        page_ids: List[str],
        base_url: Optional[str] = None,
        output_suffix: str = "custom_pages",
    ) -> List[Dict[str, Any]]:
        """
        Синхронизация выбранных страниц по ID или URL (из URL извлекается pageId).
        В каждой строке можно добавить описание для удобства: «URL  описание» или «URL\tописание».
        Сохраняет в confluence_<output_suffix>.json; описание попадает в source.engineer_comment.
        """
        import re
        base_url = base_url or self.client.base_url
        # Парсим строки: url_or_id [табуляция или два+ пробела] описание
        parsed: List[Tuple[str, Optional[str]]] = []
        for raw in page_ids:
            raw = raw.strip()
            if not raw:
                continue
            if "\t" in raw:
                url_part, desc = raw.split("\t", 1)
                url_part, desc = url_part.strip(), (desc or "").strip() or None
            elif re.search(r"  +", raw):
                parts = re.split(r"  +", raw, maxsplit=1)
                url_part = (parts[0] or "").strip()
                desc = (parts[1] or "").strip() or None if len(parts) > 1 else None
            else:
                url_part, desc = raw, None
            if not url_part:
                continue
            # Извлечь pageId из url_part
            if url_part.isdigit():
                parsed.append((url_part, desc))
                continue
            m = re.search(r"pageId=(\d+)", url_part, re.IGNORECASE)
            if m:
                parsed.append((m.group(1), desc))
                continue
            m_att = re.search(r"/download/attachments/(\d+)(?:/|$|\?)", url_part, re.IGNORECASE)
            if m_att:
                parsed.append((m_att.group(1), desc))
                continue
            m_pages = re.search(r"/pages/(\d+)(?:/|$|\?)", url_part, re.IGNORECASE)
            if m_pages:
                parsed.append((m_pages.group(1), desc))
                continue
            logger.warning("Не удалось извлечь pageId из строки: %s", url_part[:80])
        # Дедупликация с сохранением порядка; при повторе page_id оставляем первое описание
        seen: set = set()
        unique_list: List[Tuple[str, Optional[str]]] = []
        for page_id, desc in parsed:
            if page_id not in seen:
                seen.add(page_id)
                unique_list.append((page_id, desc))
        docs: List[Dict[str, Any]] = []
        for page_id, engineer_comment in unique_list:
            try:
                page = self.client.get_page_by_id(page_id)
                doc = self.page_to_kb_doc(page, base_url)
                if engineer_comment:
                    (doc.setdefault("source", {}))["engineer_comment"] = engineer_comment
                docs.append(doc)
            except Exception as e:
                logger.warning("Ошибка обработки страницы %s: %s", page_id, e)
        out_file = self.output_dir / f"confluence_{output_suffix}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(docs, f, ensure_ascii=False, indent=2)
        logger.info("Сохранено %s документов в %s", len(docs), out_file)
        return docs

    def get_synced_docs_path(self) -> Path:
        """Директория с JSON файлами Confluence KB."""
        return self.output_dir

    def get_outdated_path(self) -> Path:
        """Файл со списком page_id устаревших документов (по одному на строку)."""
        return self.output_dir / "outdated.txt"

    def read_outdated_ids(self) -> set:
        """Прочитать множество page_id, помеченных как устаревшие."""
        path = self.get_outdated_path()
        if not path.exists():
            return set()
        with open(path, "r", encoding="utf-8") as f:
            return {line.strip() for line in f if line.strip()}

    def add_to_outdated(self, page_id: str) -> None:
        """Добавить page_id в список устаревших."""
        page_id = (page_id or "").strip()
        if not page_id:
            return
        ids = self.read_outdated_ids()
        ids.add(page_id)
        with open(self.get_outdated_path(), "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(ids)) + "\n")
        logger.info("Помечен устаревшим: page_id=%s", page_id)

    def remove_from_outdated(self, page_id: str) -> None:
        """Убрать page_id из списка устаревших."""
        page_id = (page_id or "").strip()
        if not page_id:
            return
        ids = self.read_outdated_ids()
        ids.discard(page_id)
        path = self.get_outdated_path()
        if ids:
            with open(path, "w", encoding="utf-8") as f:
                f.write("\n".join(sorted(ids)) + "\n")
        elif path.exists():
            path.unlink()
        logger.info("Снята пометка устаревшего: page_id=%s", page_id)

    def update_docs_by_page_ids(
        self,
        page_ids: List[str],
        base_url: Optional[str] = None,
    ) -> int:
        """
        Обновить документы по списку page_id: заново забрать из Confluence и перезаписать в JSON.
        Возвращает количество обновлённых документов.
        """
        base_url = base_url or self.client.base_url
        updated = 0
        for page_id in page_ids:
            page_id = (page_id or "").strip()
            if not page_id or not page_id.isdigit():
                continue
            try:
                page = self.client.get_page_by_id(page_id)
                new_doc = self.page_to_kb_doc(page, base_url)
            except Exception as e:
                logger.warning("Не удалось обновить страницу %s: %s", page_id, e)
                continue
            # Найти файл, в котором лежит этот page_id, и заменить документ
            for json_file in self.output_dir.glob("*.json"):
                try:
                    with open(json_file, "r", encoding="utf-8") as f:
                        docs = json.load(f)
                except Exception:
                    continue
                if not isinstance(docs, list):
                    continue
                for i, doc in enumerate(docs):
                    src = doc.get("source") or {}
                    if str(src.get("page_id", "")) == str(page_id):
                        docs[i] = new_doc
                        with open(json_file, "w", encoding="utf-8") as f:
                            json.dump(docs, f, ensure_ascii=False, indent=2)
                        updated += 1
                        logger.info("Обновлён документ %s в %s", page_id, json_file.name)
                        break
        return updated
