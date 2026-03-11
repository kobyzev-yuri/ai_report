#!/usr/bin/env python3
"""
Автоматическая синхронизация пространств Confluence в спутниковую KB.

Использует общий модуль kb_billing.rag.confluence_sync_runner. После запуска нужно
выполнить «Перезагрузить в Qdrant только Confluence» (в веб-интерфейсе или через loader),
чтобы чанки попали в векторную БД.

Использование:
  # Указанные пространства:
  python scripts/sync_confluence_spaces.py ~a.zhegalov SPC
  python scripts/sync_confluence_spaces.py ~a.zhegalov --limit 20

  # Все пространства Confluence (по списку из API):
  python scripts/sync_confluence_spaces.py --all
  python scripts/sync_confluence_spaces.py --all --limit 30 --exclude DEMO,TEST
  python scripts/sync_confluence_spaces.py --all --personal-only   # только личные (~user)
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
    ap = argparse.ArgumentParser(description="Синхронизация пространств Confluence в KB (JSON).")
    ap.add_argument("spaces", nargs="*", help="Ключи пространств (например ~a.zhegalov SPC). Не нужны при --all.")
    ap.add_argument("--all", action="store_true", help="Синхронизировать все пространства из API (по всему Confluence)")
    ap.add_argument("--limit", type=int, default=50, help="Макс. страниц на пространство (0 = без ограничения)")
    ap.add_argument("--exclude", type=str, default="", help="Ключи пространств через запятую, которые пропустить (при --all)")
    ap.add_argument("--personal-only", action="store_true", help="При --all: только личные пространства (type=personal)")
    ap.add_argument("--output-dir", type=str, default="", help="Каталог confluence_docs (по умолчанию kb_billing/confluence_docs)")
    args = ap.parse_args()

    if not os.getenv("CONFLUENCE_URL") or not os.getenv("CONFLUENCE_TOKEN"):
        print("Задайте CONFLUENCE_URL и CONFLUENCE_TOKEN (или config.env)", file=sys.stderr)
        sys.exit(1)

    from kb_billing.rag.confluence_sync_runner import get_client, sync_spaces, sync_all_spaces

    client = get_client()
    ok, msg = client.check_connection()
    if not ok:
        print(f"Confluence: {msg}", file=sys.stderr)
        sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else None
    limit = None if args.limit <= 0 else args.limit

    if args.all:
        exclude_list = [k.strip() for k in args.exclude.split(",") if k.strip()] if args.exclude else None
        result = sync_all_spaces(
            limit_per_space=limit,
            output_dir=output_dir,
            client=client,
            exclude_keys=exclude_list,
            include_only_personal=args.personal_only,
        )
    else:
        if not args.spaces:
            print("Укажите ключи пространств или используйте --all для синхронизации всех пространств.", file=sys.stderr)
            sys.exit(1)
        result = sync_spaces(args.spaces, limit=limit, output_dir=output_dir, client=client)

    if not result:
        print("Ни одного пространства не синхронизировано.", file=sys.stderr)
        sys.exit(1)
    for key, path in result.items():
        print(f"  {key} -> {path}")
    print("Готово. Для обновления поиска выполните перезагрузку Confluence в Qdrant.")


if __name__ == "__main__":
    main()
