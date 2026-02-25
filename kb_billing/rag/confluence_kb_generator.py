#!/usr/bin/env python3
"""
Генератор KB из Confluence: получение страниц → парсинг Storage → единый формат KB → JSON.
Совместим с форматом из KB_FROM_CONFLUENCE.md (title, audience, scope, status, content, source).
Сектор спутниковых систем наполняется структурой документов из Confluence (страницы, секции),
а не примерами SQL — один документ = одна страница с разделами и текстом.
"""
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup

from kb_billing.rag.confluence_client import ConfluenceClient

logger = logging.getLogger(__name__)


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
    """Confluence → формат KB (JSON) и сохранение в файлы."""

    def __init__(
        self,
        client: Optional[ConfluenceClient] = None,
        output_dir: Optional[Path] = None,
        default_audience: Optional[List[str]] = None,
        default_scope: Optional[List[str]] = None,
        default_status: str = "reference",
    ):
        self.client = client or ConfluenceClient()
        self.output_dir = Path(output_dir) if output_dir else Path(__file__).parent.parent / "confluence_docs"
        self.default_audience = default_audience or ["user", "admin"]
        self.default_scope = default_scope or ["general"]
        self.default_status = default_status
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def page_to_kb_doc(self, page: Dict[str, Any], base_url: str) -> Dict[str, Any]:
        """Преобразование одной страницы Confluence в документ формата KB."""
        page_id = page.get("id", "")
        title = page.get("title", "Без названия")
        storage = self.client.get_page_content_storage(page)
        version_date = self.client.get_page_version_date(page)
        link = f"{base_url}/pages/viewpage.action?pageId={page_id}" if base_url else ""
        content = _parse_storage_to_content(storage)
        return {
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

    def get_synced_docs_path(self) -> Path:
        """Директория с JSON файлами Confluence KB."""
        return self.output_dir
