#!/usr/bin/env python3
"""
Деактивация и удаление устаревшего контента спутниковой KB.

- Добавить page_id в outdated.txt: при перезагрузке Confluence в Qdrant эти страницы не попадут в поиск.
- Убрать page_id из outdated.txt: снова учитывать при перезагрузке.
- Удалить документы из JSON и пометить устаревшими: убрать из файлов в confluence_docs и добавить в outdated.txt.

Использование:
  python scripts/confluence_kb_outdated.py add 4392673 123456
  python scripts/confluence_kb_outdated.py remove 4392673
  python scripts/confluence_kb_outdated.py delete 4392673 123456   # удалить из JSON + добавить в outdated
  python scripts/confluence_kb_outdated.py list                    # показать текущие page_id в outdated.txt
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
    ap = argparse.ArgumentParser(description="Деактивация/удаление устаревшего контента спутниковой KB.")
    ap.add_argument("action", choices=["add", "remove", "delete", "list"], help="add=в outdated, remove=из outdated, delete=из JSON+outdated, list=показать outdated")
    ap.add_argument("page_ids", nargs="*", help="page_id (для list не нужны)")
    ap.add_argument("--output-dir", type=str, default="", help="Каталог confluence_docs")
    ap.add_argument("--no-outdated", action="store_true", help="При delete: только удалить из JSON, не добавлять в outdated.txt")
    args = ap.parse_args()

    output_dir = Path(args.output_dir) if args.output_dir else None

    from kb_billing.rag.confluence_sync_runner import (
        get_generator,
        mark_outdated,
        unmark_outdated,
        remove_pages_from_kb,
    )

    if args.action == "list":
        gen = get_generator(output_dir=output_dir)
        path = gen.get_outdated_path()
        ids = gen.read_outdated_ids()
        if not ids:
            print("Список устаревших пуст.")
            return
        print(f"Устаревшие page_id ({path}):")
        for pid in sorted(ids):
            print(f"  {pid}")
        return

    if not args.page_ids:
        print("Укажите хотя бы один page_id (для action add/remove/delete).", file=sys.stderr)
        sys.exit(1)

    if args.action == "add":
        n = mark_outdated(args.page_ids, output_dir=output_dir)
        print(f"Добавлено в outdated: {n} (всего id в списке: {len(args.page_ids)})")
    elif args.action == "remove":
        unmark_outdated(args.page_ids, output_dir=output_dir)
        print("Удалено из outdated:", ", ".join(args.page_ids))
    elif args.action == "delete":
        removed = remove_pages_from_kb(
            args.page_ids,
            output_dir=output_dir,
            also_mark_outdated=not args.no_outdated,
        )
        print(f"Удалено документов из JSON: {removed}. Помечены устаревшими: {not args.no_outdated}")


if __name__ == "__main__":
    main()
