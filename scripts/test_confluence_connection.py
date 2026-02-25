#!/usr/bin/env python3
"""
Проверка доступа к Confluence (например docs.steccom.ru).

Что нужно:
  - CONFLUENCE_URL  — базовый URL, например https://docs.steccom.ru
  - CONFLUENCE_TOKEN — Personal Access Token (Bearer) с правами на чтение

Запуск (переменные из окружения):
  export CONFLUENCE_URL=https://docs.steccom.ru
  export CONFLUENCE_TOKEN=your-token
  python scripts/test_confluence_connection.py

Или с аргументами (токен лучше не передавать в командной строке):
  python scripts/test_confluence_connection.py --url https://docs.steccom.ru
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# Подгрузить config.env из корня проекта (CONFLUENCE_URL, CONFLUENCE_TOKEN)
_config_env = Path(__file__).parent.parent / "config.env"
if _config_env.exists():
    with open(_config_env, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip("'\""))

# Импорт только confluence_client (без kb_loader/sentence_transformers)
_confluence_client_py = Path(__file__).parent.parent / "kb_billing" / "rag" / "confluence_client.py"
import importlib.util
_spec = importlib.util.spec_from_file_location("confluence_client", _confluence_client_py)
_confluence_client = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_confluence_client)
ConfluenceClient = _confluence_client.ConfluenceClient


def main():
    import argparse
    p = argparse.ArgumentParser(description="Проверка доступа к Confluence")
    p.add_argument("--url", help="CONFLUENCE_URL (иначе из переменной окружения)")
    p.add_argument("--token", default="", help="CONFLUENCE_TOKEN (лучше задать через export)")
    p.add_argument("--dry-run", action="store_true", help="Только проверить клиент без запроса к серверу")
    args = p.parse_args()

    url = args.url
    token = args.token
    if not url:
        url = os.getenv("CONFLUENCE_URL")
    if not token:
        token = os.getenv("CONFLUENCE_TOKEN", "")

    if args.dry_run:
        client = ConfluenceClient(base_url=url, token=token or None)
        if not client.base_url:
            print("OK (dry-run): CONFLUENCE_URL не задан — клиент создан")
        elif not client.token:
            print("OK (dry-run): CONFLUENCE_TOKEN не задан — клиент создан")
        else:
            print("OK (dry-run): URL и токен заданы, клиент готов")
        return 0

    client = ConfluenceClient(base_url=url, token=token or None)
    ok, msg = client.check_connection()
    if ok:
        print("OK:", msg)
        # опционально: список пространств
        try:
            spaces = client.get_spaces(limit=10)
            if spaces:
                print("Пространства (первые 10):")
                for s in spaces:
                    print("  -", s.get("key"), s.get("name", ""))
        except Exception as e:
            print("Список пространств:", e)
        return 0
    else:
        print("Ошибка:", msg)
        return 1


if __name__ == "__main__":
    sys.exit(main())
