#!/usr/bin/env python3
"""
Зачистка спутниковой части KB перед тестами: удаление всех документов Confluence из каталога.

После зачистки нужно выполнить «Перезагрузить в Qdrant только Confluence» (веб или loader),
чтобы удалить чанки из векторной БД. Биллинг не затрагивается.

Использование:
  python scripts/clean_satellite_kb.py
  python scripts/clean_satellite_kb.py --backup   # переместить JSON и outdated.txt в backup/ с меткой времени
  python scripts/clean_satellite_kb.py --output-dir /path/to/deploy/kb_billing/confluence_docs
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path
from datetime import datetime

_repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo_root))


def main() -> None:
    ap = argparse.ArgumentParser(description="Зачистка спутниковой KB (confluence_docs): удалить или в backup.")
    ap.add_argument("--backup", action="store_true", help="Переместить JSON и outdated.txt в backup/ с меткой времени")
    ap.add_argument("--output-dir", type=str, default="", help="Каталог confluence_docs (по умолчанию kb_billing/confluence_docs от текущего каталога)")
    ap.add_argument("--dry-run", action="store_true", help="Только показать, что будет удалено/перенесено")
    args = ap.parse_args()

    if args.output_dir:
        out_dir = Path(args.output_dir)
    else:
        # По умолчанию: kb_billing/confluence_docs относительно текущего каталога (для запуска из корня или из deploy)
        out_dir = Path.cwd() / "kb_billing" / "confluence_docs"
        if not out_dir.exists() and (_repo_root / "kb_billing" / "confluence_docs").exists():
            out_dir = _repo_root / "kb_billing" / "confluence_docs"

    if not out_dir.exists():
        print(f"Каталог не найден: {out_dir}", file=sys.stderr)
        print("Спутниковая KB уже пуста или укажите --output-dir.", file=sys.stderr)
        sys.exit(0)

    json_files = list(out_dir.glob("*.json"))
    outdated_file = out_dir / "outdated.txt"

    if not json_files and not outdated_file.exists():
        print("В каталоге нет JSON и outdated.txt — нечего чистить.")
        sys.exit(0)

    if args.dry_run:
        print("Dry-run. Будет затронуто:")
        for f in json_files:
            print(f"  {f.name}")
        if outdated_file.exists():
            print(f"  {outdated_file.name}")
        if args.backup:
            print("  → backup/ с меткой времени")
        else:
            print("  → удаление")
        sys.exit(0)

    if args.backup:
        backup_dir = out_dir / "backup"
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_sub = backup_dir / stamp
        backup_sub.mkdir(parents=True, exist_ok=True)
        for f in json_files:
            shutil.move(str(f), str(backup_sub / f.name))
            print(f"  backup: {f.name}")
        if outdated_file.exists():
            shutil.move(str(outdated_file), str(backup_sub / outdated_file.name))
            print(f"  backup: {outdated_file.name}")
        print(f"Резервная копия: {backup_sub}")
    else:
        for f in json_files:
            f.unlink()
            print(f"  удалён: {f.name}")
        if outdated_file.exists():
            outdated_file.unlink()
            print(f"  удалён: {outdated_file.name}")

    print("Готово. Выполните «Перезагрузить в Qdrant только Confluence» (веб или loader), чтобы обновить поиск.")


if __name__ == "__main__":
    main()
