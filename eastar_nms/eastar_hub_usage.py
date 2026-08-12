#!/usr/bin/env python3
"""Zabbix collector B: WEB NMS Eastar hub_usage (URL2).

Stub mode prints sample JSON filtered by controller key.
Live fetch will use:
  {NMS_URL}/#/hub_usage/?net_id={NETID}
and keep rows whose controller name contains --filter / EASTAR_FILTER.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from common import (
    add_common_args,
    bootstrap_env,
    ensure_stub_or_exit,
    env,
    resolve_common,
    utc_now_iso,
)


def stub_payload(net_id: int, filter_key: str) -> dict:
    # Example controllers from TZ / hub_usage screenshot for filter «AM6 E04».
    all_controllers = [
        {"name": "AM6 E04 01 B08 R_V 2_1200", "tx_kbit_s": 5175.6, "rx_kbit_s": 2437.9},
        {"name": "AM6 E04 02 B06 2_1200", "tx_kbit_s": 0.0, "rx_kbit_s": 0.0},
        {"name": "AM6 E04 03 B08 2_800", "tx_kbit_s": 0.0, "rx_kbit_s": 720.0},
        {"name": "AM6 E03 01 B05 L_H 4_800", "tx_kbit_s": 100.0, "rx_kbit_s": 50.0},
    ]
    key = filter_key.strip().lower()
    matched = [c for c in all_controllers if key in c["name"].lower()]
    return {
        "source": "hub_usage",
        "net_id": net_id,
        "filter": filter_key,
        "ts": utc_now_iso(),
        "stub": True,
        "controllers": matched,
    }


def fetch_live(cfg: dict, filter_key: str) -> dict:
    raise NotImplementedError(
        "live hub_usage: login to {url} as {login}, "
        "fetch net_id={net_id}, filter={filter_key!r}".format(
            url=cfg["nms_url"],
            login=cfg["login"],
            net_id=cfg["net_id"],
            filter_key=filter_key,
        )
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Eastar NMS hub_usage → JSON for Zabbix")
    add_common_args(parser)
    parser.add_argument(
        "--filter",
        default=None,
        help="Controller name key/filter (env EASTAR_FILTER), e.g. 'AM6 E04'",
    )
    args = parser.parse_args(argv)
    cfg = resolve_common(args)
    bootstrap_env()
    filter_key = args.filter or os.environ.get("EASTAR_FILTER") or env("EASTAR_FILTER", "AM6 E04")
    ensure_stub_or_exit(cfg["mode"], "eastar_hub_usage.py")

    _ = (cfg["login"], cfg["password"], cfg["nms_url"])

    if cfg["mode"] == "stub":
        payload = stub_payload(cfg["net_id"], filter_key)
    else:
        payload = fetch_live(cfg, filter_key)

    json.dump(payload, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
