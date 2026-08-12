"""Shared helpers for Eastar NMS → Zabbix collectors."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_env_file(path: Path) -> None:
    """Load KEY=VALUE lines into os.environ if key is not already set."""
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


def bootstrap_env() -> None:
    here = Path(__file__).resolve().parent
    load_env_file(here / "config.env")
    load_env_file(here / "config.env.example")


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None or value == "":
        raise SystemExit(f"Missing required parameter: {name}")
    return value


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--nms-url",
        default=None,
        help="NMS base URL (env EASTAR_NMS_URL)",
    )
    parser.add_argument(
        "--login",
        default=None,
        help="NMS login (env EASTAR_NMS_LOGIN)",
    )
    parser.add_argument(
        "--password",
        default=None,
        help="NMS password (env EASTAR_NMS_PASSWORD)",
    )
    parser.add_argument(
        "--net-id",
        default=None,
        help="Network id (env EASTAR_NET_ID)",
    )
    parser.add_argument(
        "--mode",
        choices=("stub", "live"),
        default=None,
        help="stub (default) or live (not implemented yet)",
    )


def resolve_common(args: argparse.Namespace) -> dict[str, Any]:
    bootstrap_env()
    nms_url = (args.nms_url or env("EASTAR_NMS_URL", "https://start.steccom.ru")).rstrip("/")
    login = args.login or env("EASTAR_NMS_LOGIN", "CHANGE_ME")
    password = args.password or env("EASTAR_NMS_PASSWORD", "CHANGE_ME")
    net_id_raw = args.net_id or env("EASTAR_NET_ID", "1")
    mode = args.mode or env("EASTAR_MODE", "stub")
    try:
        net_id = int(net_id_raw)
    except ValueError as exc:
        raise SystemExit(f"EASTAR_NET_ID / --net-id must be int, got: {net_id_raw!r}") from exc

    return {
        "nms_url": nms_url,
        "login": login,
        "password": password,
        "net_id": net_id,
        "mode": mode,
    }


def ensure_stub_or_exit(mode: str, script: str) -> None:
    if mode == "stub":
        return
    print(
        f"{script}: mode=live is not implemented yet "
        "(waiting for network access and credentials).",
        file=sys.stderr,
    )
    raise SystemExit(2)
