#!/usr/bin/env python3
"""
Синхронизация выбранных страниц Confluence по URL или page_id в спутниковую KB.

Поддерживает режим дополнения/апдейта (--merge): существующий confluence_custom_pages.json
загружается, переданные страницы обновляются или добавляются, остальные сохраняются.

Использование:
  export CONFLUENCE_URL=... CONFLUENCE_TOKEN=...
  python scripts/sync_confluence_pages.py https://docs.steccom.ru/pages/viewpage.action?pageId=4392673
  python scripts/sync_confluence_pages.py 4392673 123456
  python scripts/sync_confluence_pages.py --file urls.txt --merge
  echo "4392673  Инструкция ЦУС" | python scripts/sync_confluence_pages.py --merge --output-suffix custom_pages
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

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

sys.path.insert(0, str(_repo_root))


def main() -> None:
    ap = argparse.ArgumentParser(description="Синхронизация страниц Confluence по URL/ID в KB.")
    ap.add_argument("urls_or_ids", nargs="*", help="URL страниц/вложений или page_id (по одному)")
    ap.add_argument("--file", "-f", type=str, help="Файл: по одной строке URL или page_id (опционально с описанием через таб/пробелы)")
    ap.add_argument("--output-suffix", type=str, default="custom_pages", help="Имя файла: confluence_{suffix}.json")
    ap.add_argument("--merge", action="store_true", help="Дополнить/обновить существующий JSON (не перезаписывать целиком)")
    ap.add_argument("--with-children", action="store_true", help="Рекурсивно загрузить дочерние страницы (по API child/page)")
    ap.add_argument("--max-depth", type=int, default=1, help="При --with-children: макс. глубина (1 = только прямые дети)")
    ap.add_argument("--limit-children", type=int, default=200, help="Макс. число дочерних страниц всего")
    ap.add_argument("--output-dir", type=str, default="", help="Каталог confluence_docs")
    args = ap.parse_args()

    lines = list(args.urls_or_ids or [])
    if args.file:
        path = Path(args.file)
        if path.exists():
            lines.extend(path.read_text(encoding="utf-8").splitlines())
        else:
            print(f"Файл не найден: {path}", file=sys.stderr)
            sys.exit(1)
    if not lines:
        print("Укажите URL/page_id аргументами или --file", file=sys.stderr)
        sys.exit(1)

    if not os.getenv("CONFLUENCE_URL") or not os.getenv("CONFLUENCE_TOKEN"):
        print("Задайте CONFLUENCE_URL и CONFLUENCE_TOKEN (или config.env)", file=sys.stderr)
        sys.exit(1)

    from kb_billing.rag.confluence_sync_runner import get_client, sync_pages

    ok, msg = get_client().check_connection()
    if not ok:
        print(f"Confluence: {msg}", file=sys.stderr)
        sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else None
    path, count = sync_pages(
        lines,
        output_dir=output_dir,
        output_suffix=args.output_suffix,
        merge=args.merge,
        include_children=args.with_children,
        max_depth=args.max_depth,
        limit_children=args.limit_children,
    )
    print(f"Сохранено документов: {count} в {path}")
    if count > 0:
        print("Для обновления поиска выполните перезагрузку Confluence в Qdrant.")


if __name__ == "__main__":
    main()
