#!/usr/bin/env python3
"""Zabbix collector: WEB NMS Eastar hub_usage.

Тот же JSON, что у eastar_hub_usage.pl. По умолчанию live.
На русском NMS трафик читается с таблицы страницы hub_usage
(Передача/Приём). На английском — из WidgetControllerStatus.
"""

from __future__ import print_function

import argparse
import os
import sys

from common import (
    add_common_args,
    bootstrap_env,
    json_out,
    require_credentials,
    resolve_common,
    utc_now_iso,
)
from hub import collect_controllers
from nms import NmsClient


def stub_payload(net_id, filter_key):
    all_controllers = [
        {"name": "AM6 E04 01 B08 R_V 2_1200", "tx_kbit_s": 5175.6, "rx_kbit_s": 2437.9},
        {"name": "AM6 E04 02 B06 2_1200", "tx_kbit_s": 0.0, "rx_kbit_s": 0.0},
        {"name": "AM6 E04 03 B08 2_800", "tx_kbit_s": 0.0, "rx_kbit_s": 720.0},
        {"name": "AM6 E03 01 B05 L_H 4_800", "tx_kbit_s": 100.0, "rx_kbit_s": 50.0},
    ]
    key = filter_key.strip().lower()
    matched = [c for c in all_controllers if not key or key in c["name"].lower()]
    return {
        "source": "hub_usage",
        "net_id": net_id,
        "filter": filter_key,
        "ts": utc_now_iso(),
        "stub": True,
        "controllers": matched,
    }


def fetch_live(cfg, filter_key):
    require_credentials(cfg)
    client = NmsClient(cfg)
    client.login()
    client.select_net()
    page = client.select_hub()
    tree = client.updatetree()

    def fetch_widget(cid):
        return client.update(
            {"what": "widget", "datasrc": "WidgetControllerStatus:%s" % cid}
        )

    rows = collect_controllers(
        page,
        list(tree.get("controllers") or []),
        cfg["net_id"],
        filter_key,
        fetch_widget=fetch_widget,
    )
    return {
        "source": "hub_usage",
        "net_id": int(cfg["net_id"]),
        "filter": filter_key,
        "ts": utc_now_iso(),
        "stub": False,
        "controllers": rows,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="Eastar NMS hub_usage → JSON for Zabbix")
    add_common_args(parser)
    parser.add_argument(
        "--filter",
        default=None,
        help="Controller name substring (env EASTAR_FILTER), e.g. 'AM6 E04'",
    )
    args = parser.parse_args(argv)
    cfg = resolve_common(args)
    bootstrap_env()
    if args.filter is not None:
        filter_key = args.filter
    else:
        filter_key = os.environ.get("EASTAR_FILTER", "")
    filter_key = filter_key.strip()
    if cfg["mode"] == "stub":
        payload = stub_payload(cfg["net_id"], filter_key)
    else:
        payload = fetch_live(cfg, filter_key)
    json_out(payload)
    return 0


if __name__ == "__main__":
    sys.exit(main())
