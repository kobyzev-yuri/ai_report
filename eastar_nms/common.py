"""Общие аргументы Python-коллекторов Eastar NMS. Тот же config.env, что у Perl."""

from __future__ import print_function

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def json_out(data):
    text = json.dumps(data, sort_keys=True, indent=3, ensure_ascii=False)
    sys.stdout.write(text)
    if not text.endswith("\n"):
        sys.stdout.write("\n")


def load_env_file(path):
    """KEY=VALUE в os.environ, если ключ ещё не задан."""
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


def bootstrap_env():
    here = Path(__file__).resolve().parent
    load_env_file(here / "config.env")
    load_env_file(here / "config.env.example")


def _normalize_url(url):
    url = (url or "").strip().rstrip("/")
    if not url:
        url = "https://192.168.10.49"
    if not re.match(r"^https?://", url, re.IGNORECASE):
        scheme = (os.environ.get("EASTAR_NMS_SCHEME") or "https").lower()
        if scheme not in ("http", "https"):
            scheme = "https"
        url = "%s://%s" % (scheme, url)
    return url


def add_common_args(parser):
    parser.add_argument("--nms-url", default=None, help="NMS base URL (env EASTAR_NMS_URL)")
    parser.add_argument("--login", default=None, help="NMS login (env EASTAR_NMS_LOGIN)")
    parser.add_argument("--password", default=None, help="NMS password (env EASTAR_NMS_PASSWORD)")
    parser.add_argument("--net-id", default=None, help="Network id (env EASTAR_NET_ID)")
    parser.add_argument(
        "--mode",
        choices=("stub", "live"),
        default=None,
        help="live (default) or stub sample JSON",
    )


def resolve_common(args):
    bootstrap_env()
    nms_url = _normalize_url(args.nms_url or os.environ.get("EASTAR_NMS_URL"))
    login = args.login if args.login is not None else os.environ.get("EASTAR_NMS_LOGIN", "")
    password = args.password if args.password is not None else os.environ.get("EASTAR_NMS_PASSWORD", "")
    net_id_raw = args.net_id if args.net_id is not None else os.environ.get("EASTAR_NET_ID", "1")
    mode = args.mode or os.environ.get("EASTAR_MODE") or "live"
    timeout_raw = os.environ.get("EASTAR_TIMEOUT") or "20"
    try:
        net_id = int(net_id_raw)
    except ValueError:
        raise SystemExit("EASTAR_NET_ID / --net-id must be int, got: %r" % (net_id_raw,))
    try:
        timeout = int(timeout_raw)
    except ValueError:
        timeout = 20
    return {
        "nms_url": nms_url,
        "login": login,
        "password": password,
        "net_id": net_id,
        "mode": mode,
        "timeout": timeout,
    }


def require_credentials(cfg):
    login = cfg.get("login") or ""
    password = cfg.get("password") or ""
    if not login or login == "CHANGE_ME" or not password or password == "CHANGE_ME":
        raise SystemExit(
            "Set EASTAR_NMS_LOGIN and EASTAR_NMS_PASSWORD in config.env "
            "(copy config.env.example)."
        )
