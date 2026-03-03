#!/usr/bin/env python3
"""
Парсеры вложений Confluence: извлечение текста из PDF, DOCX, Draw.io (схемы сети),
изображений (OCR) для полной индексации контента в KB.
"""
import base64
import io
import logging
import re
import zlib
import xml.etree.ElementTree as ET
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Расширения и MIME для выбора парсера
PDF_EXT = (".pdf",)
PDF_MIME = ("application/pdf",)
DOCX_EXT = (".docx", ".doc")
DOCX_MIME = (
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/msword",
)
DRAWIO_EXT = (".drawio", ".drawio.xml", ".xml")
# Draw.io в Confluence часто как application/xml или octet-stream
IMAGE_EXT = (".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff", ".tif", ".webp")
IMAGE_MIME = ("image/png", "image/jpeg", "image/gif", "image/bmp", "image/tiff", "image/webp")


def _normalize_ext(filename: str) -> str:
    if not filename:
        return ""
    p = filename.rfind(".")
    return filename[p:].lower() if p >= 0 else ""


def extract_text_from_pdf(data: bytes, filename: str = "") -> Tuple[str, Optional[str]]:
    """
    Извлечь текст из PDF. Возвращает (plain_text, error_message).
    """
    try:
        import fitz  # PyMuPDF
    except ImportError:
        return "", "PyMuPDF не установлен: pip install pymupdf"
    try:
        doc = fitz.open(stream=data, filetype="pdf")
        parts = []
        for page in doc:
            parts.append(page.get_text())
        doc.close()
        text = "\n\n".join(p.strip() for p in parts if p.strip())
        return text.strip() or "(документ без извлекаемого текста)", None
    except Exception as e:
        logger.warning("PDF parse %s: %s", filename or "?", e)
        return "", str(e)


def extract_text_from_docx(data: bytes, filename: str = "") -> Tuple[str, Optional[str]]:
    """
    Извлечь текст из DOCX. Возвращает (plain_text, error_message).
    """
    try:
        from docx import Document
    except ImportError:
        return "", "python-docx не установлен: pip install python-docx"
    try:
        doc = Document(io.BytesIO(data))
        parts = []
        for p in doc.paragraphs:
            if p.text.strip():
                parts.append(p.text)
        for table in doc.tables:
            for row in table.rows:
                cells = [c.text.strip() for c in row.cells if c.text.strip()]
                if cells:
                    parts.append(" | ".join(cells))
        text = "\n\n".join(parts)
        return text.strip() or "(документ без текста)", None
    except Exception as e:
        logger.warning("DOCX parse %s: %s", filename or "?", e)
        return "", str(e)


def _strip_html_fragment(html: str) -> str:
    """Убрать теги из фрагмента HTML (value в draw.io может быть <p>...</p>)."""
    if not html:
        return ""
    return re.sub(r"<[^>]+>", " ", html).replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").strip()


def _drawio_decompress(diagram_content: str) -> Optional[str]:
    """Распаковать сжатый контент из <diagram> (base64 + raw deflate)."""
    s = (diagram_content or "").strip()
    if not s or s.startswith("<"):
        return s or None
    try:
        raw = base64.b64decode(s)
        # raw deflate без zlib-заголовка
        return zlib.decompress(raw, -zlib.MAX_WBITS).decode("utf-8", errors="replace")
    except Exception:
        return None


def extract_text_from_drawio(data: bytes, filename: str = "") -> Tuple[str, Optional[str]]:
    """
    Извлечь текст из схемы Draw.io (.drawio, .drawio.xml) — схемы сети в формате draw.io.
    Парсит XML (в т.ч. сжатый mxfile/diagram), собирает все атрибуты value и текст из mxCell —
    подписи узлов, названия блоков, соединений. Для индексации схем сети в KB.
    Возвращает (plain_text, error_message).
    """
    try:
        text = data.decode("utf-8", errors="replace")
    except Exception as e:
        return "", f"Ошибка декодирования: {e}"
    text = text.strip()
    if not text.startswith("<"):
        return "", "Файл не похож на XML (draw.io: сохраните без сжатия в настройках)"
    try:
        root = ET.fromstring(text)
    except ET.ParseError as e:
        return "", f"Ошибка разбора XML: {e}"

    # Сжатый draw.io: mxfile > diagram с base64+deflate внутри
    tag = root.tag.split("}")[-1] if "}" in root.tag else root.tag
    if tag == "mxfile":

        def find_diagram(elem: ET.Element):
            if elem.tag.split("}")[-1] == "diagram":
                return elem
            for c in elem:
                found = find_diagram(c)
                if found is not None:
                    return found
            return None

        diagram = find_diagram(root)
        if diagram is not None:
            raw = (diagram.text or "").strip()
            if raw and not raw.startswith("<"):
                inner = _drawio_decompress(raw)
                if inner:
                    try:
                        root = ET.fromstring(inner)
                    except ET.ParseError:
                        pass
            elif raw and raw.startswith("<"):
                try:
                    root = ET.fromstring(raw)
                except ET.ParseError:
                    pass

    texts: list[str] = []

    def collect_mxcell(elem: ET.Element) -> None:
        t = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        if t == "mxCell":
            val = elem.get("value") or ""
            if val:
                texts.append(_strip_html_fragment(val))
            if elem.text and elem.text.strip():
                texts.append(elem.text.strip())
        for child in elem:
            collect_mxcell(child)
            if child.tail and child.tail.strip():
                texts.append(child.tail.strip())

    collect_mxcell(root)
    seen = set()
    unique = []
    for t in texts:
        t = t.strip()
        if t and t not in seen:
            seen.add(t)
            unique.append(t)
    result = "\n".join(unique)
    return result or "(схема без текстовых подписей)", None


def extract_text_from_image(data: bytes, filename: str = "") -> Tuple[str, Optional[str]]:
    """
    Извлечь текст из изображения (OCR) и/или аннотировать схему/диаграмму.
    Сейчас: OCR через pytesseract при наличии; в будущем — опционально vision API для описания схем.
    Возвращает (plain_text, error_message).
    """
    try:
        import pytesseract
        from PIL import Image
    except ImportError:
        return "", "Для OCR нужны: pip install pytesseract pillow; установите tesseract-ocr в системе"
    try:
        img = Image.open(io.BytesIO(data))
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        text = pytesseract.image_to_string(img, lang="rus+eng")
        text = (text or "").strip()
        if text:
            return text, None
        # Нет текста по OCR — можно оставить пометку для будущей аннотации диаграмм
        return "[Изображение/схема — текст по OCR не извлечён; для аннотации диаграмм подключите vision API]", None
    except Exception as e:
        logger.warning("Image OCR %s: %s", filename or "?", e)
        return "", str(e)


def parse_attachment(
    data: bytes,
    filename: str,
    content_type: Optional[str] = None,
    *,
    skip_images_without_ocr: bool = True,
) -> Tuple[str, Optional[str]]:
    """
    Универсальный парсер: по имени файла и/или content_type выбирает парсер,
    извлекает текст. Возвращает (plain_text, error_message).
    skip_images_without_ocr: если True и OCR недоступен — не возвращать ошибку, а короткую пометку.
    """
    ext = _normalize_ext(filename)
    ct = (content_type or "").strip().lower()

    if ext in PDF_EXT or ct in PDF_MIME:
        return extract_text_from_pdf(data, filename)
    if ext in DOCX_EXT or ct in DOCX_MIME:
        return extract_text_from_docx(data, filename)
    # Draw.io — схемы сети (.drawio, .drawio.xml); .xml только если имя похоже на drawio
    if ext in (".drawio", ".drawio.xml") or (ext == ".xml" and "drawio" in (filename or "").lower()):
        return extract_text_from_drawio(data, filename)
    if ext in IMAGE_EXT or (ct and ct.startswith("image/")):
        text, err = extract_text_from_image(data, filename)
        if err and skip_images_without_ocr:
            return "[Вложение: изображение/диаграмма — OCR не выполнен]", None
        return text, err

    # Текстовые типы — как есть (если в будущем добавим)
    if ct.startswith("text/"):
        try:
            return data.decode("utf-8", errors="replace").strip(), None
        except Exception:
            pass
    return "", f"Неподдерживаемый тип вложения: {filename} ({content_type or '?'})"
