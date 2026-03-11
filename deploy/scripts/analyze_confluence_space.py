#!/usr/bin/env python3
"""
Анализ структуры и типов контента пространства Confluence (для спутникового библиотекаря).

Цель: обследовать 1–2 пространства, выяснить какие объекты хранятся (страницы, вложения),
в каких форматах (jpg, ppt, pdf, draw.io и т.д.), чтобы:
- составить план извлечения схем сетей и документов;
- натравить нужные парсеры и Gemini для извлечения смысловых блоков и сохранения в KB.

Использование:
  export CONFLUENCE_URL=https://docs.steccom.ru CONFLUENCE_TOKEN=...
  python scripts/analyze_confluence_space.py ~a.zhegalov
  python scripts/analyze_confluence_space.py ~a.zhegalov --limit 20 --output report.json

Скрипт не меняет KB и не трогает биллинг; только читает Confluence API.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

# Загрузка config.env из корня репозитория
_repo_root = Path(__file__).resolve().parent.parent
_env_file = _repo_root / "config.env"
if _env_file.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_env_file)
    except ImportError:
        for line in _env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

# Импорт клиента без загрузки всего RAG (qdrant и т.д.)
sys.path.insert(0, str(_repo_root))
import importlib.util
_confluence_client_path = _repo_root / "kb_billing" / "rag" / "confluence_client.py"
_spec = importlib.util.spec_from_file_location("confluence_client", _confluence_client_path)
_confluence_client = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_confluence_client)
ConfluenceClient = _confluence_client.ConfluenceClient

# Служебные/временные форматы Confluence — при индексации пропускать (не «unsupported»)
SKIP_EXT = {".tmp", ".render", ".tfss"}

# Соответствие расширений/MIME нашим парсерам (см. attachment_parsers)
PARSER_BY_EXT = {
    ".pdf": "pdf",
    ".docx": "docx", ".doc": "docx",
    ".xls": "xls", ".xlsx": "xlsx",
    ".drawio": "drawio", ".drawio.xml": "drawio",
    ".png": "image", ".jpg": "image", ".jpeg": "image", ".gif": "image",
    ".bmp": "image", ".tiff": "image", ".tif": "image", ".webp": "image",
}
PARSER_BY_MIME = {
    "application/pdf": "pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "application/msword": "docx",
    "application/vnd.ms-excel": "xls",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
    "image/png": "image", "image/jpeg": "image", "image/gif": "image",
    "image/bmp": "image", "image/tiff": "image", "image/webp": "image",
}
# Форматы, которые часто содержат схемы сетей (для отчёта)
DIAGRAM_LIKE = {"image", "drawio", "pdf"}


def _ext(filename: str) -> str:
    if not filename:
        return ""
    m = re.search(r"\.([a-z0-9]+)$", (filename or "").lower())
    return ("." + m.group(1)) if m else ""


def _parser_for_attachment(title: str, media_type: str) -> str:
    ext = _ext(title)
    if ext in SKIP_EXT:
        return "skip"
    if ext in PARSER_BY_EXT:
        return PARSER_BY_EXT[ext]
    mt = (media_type or "").strip().lower()
    if mt in PARSER_BY_MIME:
        return PARSER_BY_MIME[mt]
    if ext == ".xml" and "drawio" in (title or "").lower():
        return "drawio"
    if mt.startswith("image/"):
        return "image"
    # draw.io в Confluence иногда без расширения, mediaType application/xml
    if (not ext or ext == "") and ("xml" in mt or "drawio" in (title or "").lower()):
        return "drawio"
    return "unsupported"


def analyze_space(
    client: ConfluenceClient,
    space_key: str,
    page_limit: int | None = 50,
) -> dict:
    """
    Собрать по пространству: страницы, вложения, типы файлов, применимые парсеры.
    """
    space_key = (space_key or "").strip()
    if not space_key:
        return {"error": "space_key пустой"}

    # Информация о пространстве
    space_info = client.get_space_info(space_key)
    space_name = (space_info or {}).get("name") or space_key
    space_type = (space_info or {}).get("type", "unknown")

    # Сбор страниц: сначала по spaceKey, если пусто — по CQL (для личных пространств)
    pages: list[dict] = []
    try:
        for p in client.get_pages_in_space(space_key, limit=page_limit or 100):
            pages.append(p)
            if page_limit and len(pages) >= page_limit:
                break
    except Exception as e:
        pass  # ниже попробуем CQL

    if not pages and "~" in space_key:
        cql = f'space="{space_key}" and type=page'
        try:
            for p in client.get_pages_by_cql(cql, limit_per_page=25, max_results=page_limit or 100):
                pages.append(p)
        except Exception as e:
            pass

    # Агрегаты по вложениям
    by_ext: dict[str, list[str]] = defaultdict(list)   # примеры имён файлов
    by_mime: dict[str, int] = defaultdict(int)
    by_parser: dict[str, int] = defaultdict(int)
    total_attachments = 0
    total_size = 0
    page_details: list[dict] = []

    for page in pages:
        page_id = str(page.get("id", ""))
        title = page.get("title", "")
        try:
            atts = client.get_page_attachments(page_id, limit=200)
        except Exception as e:
            atts = []
        att_list = []
        for a in atts:
            name = a.get("title") or ""
            size = (a.get("extensions") or {}).get("fileSize") or 0
            mime = (a.get("extensions") or {}).get("mediaType") or ""
            parser = _parser_for_attachment(name, mime)
            by_ext[_ext(name)].append(name)
            if mime:
                by_mime[mime] += 1
            by_parser[parser] += 1
            total_attachments += 1
            total_size += size
            att_list.append({
                "filename": name,
                "ext": _ext(name),
                "mediaType": mime,
                "size": size,
                "parser": parser,
            })
        page_details.append({
            "page_id": page_id,
            "title": title,
            "attachments_count": len(atts),
            "attachments": att_list[:30],  # первые 30 на страницу для примера
        })

    # Сводка по расширениям (числа)
    ext_counts = {ext: len(names) for ext, names in by_ext.items()}
    # Какие парсеры понадобятся
    parsers_needed = [p for p, c in by_parser.items() if c > 0 and p != "skip"]
    diagram_candidates = sum(by_parser.get(p, 0) for p in DIAGRAM_LIKE)
    unsupported_count = by_parser.get("unsupported", 0)
    skip_count = by_parser.get("skip", 0)

    # Краткие рекомендации для плана извлечения в KB
    plan_hints = []
    if by_parser.get("image"):
        plan_hints.append("Изображения (png/jpg/…): парсер image + Gemini Vision для смысловых блоков и аннотаций.")
    if by_parser.get("drawio"):
        plan_hints.append("Draw.io: парсер drawio + Gemini для описания структуры схемы.")
    if by_parser.get("pdf"):
        plan_hints.append("PDF: парсер pdf (текст + картинки через Vision).")
    if by_parser.get("docx"):
        plan_hints.append("DOCX: парсер docx (извлечение текста).")
    if by_parser.get("xls") or by_parser.get("xlsx"):
        plan_hints.append("XLS/XLSX: парсер xls/xlsx — списки устройств, таблицы конфигураций (контекст для KB).")
    if skip_count:
        plan_hints.append(
            f"Пропустить при индексации (служебные Confluence): {skip_count} вложений (.tmp, .render, .tfss)."
        )
    if unsupported_count:
        plan_hints.append(
            f"Неподдерживаемые форматы: {unsupported_count} вложений — проверить mediaType (draw.io без расширения?), добавить парсер или не индексировать."
        )

    report = {
        "space_key": space_key,
        "space_name": space_name,
        "space_type": space_type,
        "pages_analyzed": len(pages),
        "total_attachments": total_attachments,
        "total_attachments_size_bytes": total_size,
        "by_extension": ext_counts,
        "by_media_type": dict(by_mime),
        "by_parser": dict(by_parser),
        "parsers_needed": parsers_needed,
        "diagram_like_count": diagram_candidates,
        "unsupported_count": unsupported_count,
        "plan_hints": plan_hints,
        "pages": page_details,
        "sample_filenames_by_ext": {
            ext or "(no ext)": list(names)[:10] for ext, names in sorted(by_ext.items())
        },
    }
    return report


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Анализ структуры пространства Confluence для плана извлечения в KB (схемы, документы)."
    )
    ap.add_argument(
        "space_key",
        nargs="?",
        default="",
        help="Ключ пространства, например ~a.zhegalov или TEAM",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Максимум страниц для анализа (по умолчанию 50)",
    )
    ap.add_argument(
        "--output",
        "-o",
        type=str,
        default="",
        help="Путь к JSON-отчёту (если не задан — только в stdout)",
    )
    ap.add_argument(
        "--no-print",
        action="store_true",
        help="Не выводить текстовую сводку в stdout",
    )
    args = ap.parse_args()

    space_key = (args.space_key or os.getenv("CONFLUENCE_SPACE", "")).strip()
    if not space_key:
        print("Задайте space_key аргументом или переменной CONFLUENCE_SPACE", file=sys.stderr)
        sys.exit(1)

    url = os.getenv("CONFLUENCE_URL", "").rstrip("/")
    token = os.getenv("CONFLUENCE_TOKEN", "")
    if not url or not token:
        print("Задайте CONFLUENCE_URL и CONFLUENCE_TOKEN (или config.env)", file=sys.stderr)
        sys.exit(1)

    client = ConfluenceClient(base_url=url, token=token)
    ok, msg = client.check_connection()
    if not ok:
        print(f"Confluence: {msg}", file=sys.stderr)
        sys.exit(1)

    report = analyze_space(client, space_key, page_limit=args.limit)

    if report.get("error"):
        print(report["error"], file=sys.stderr)
        sys.exit(1)

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print(f"Отчёт записан: {out_path}", file=sys.stderr)

    if not args.no_print:
        print("\n=== Сводка по пространству ===")
        print(f"Пространство: {report['space_name']} (key={report['space_key']}, type={report['space_type']})")
        print(f"Страниц проанализировано: {report['pages_analyzed']}")
        print(f"Всего вложений: {report['total_attachments']} ({report['total_attachments_size_bytes']} байт)")
        print("\nПо расширениям:", report["by_extension"])
        print("По парсерам (pdf/docx/drawio/image/skip/unsupported):", report["by_parser"])
        print("Парсеры, которые понадобятся:", report["parsers_needed"])
        print(f"Вложений «схемоподобных» (image/drawio/pdf): {report['diagram_like_count']}")
        if report.get("plan_hints"):
            print("\nРекомендации для плана извлечения в KB:")
            for h in report["plan_hints"]:
                print(f"  • {h}")
        print("\nПримеры имён по типам файлов:")
        for ext, names in report.get("sample_filenames_by_ext", {}).items():
            print(f"  {ext}: {names[:5]}")


if __name__ == "__main__":
    main()
