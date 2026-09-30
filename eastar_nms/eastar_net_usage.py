#!/usr/bin/env python3
"""Zabbix collector: WEB NMS Eastar net_usage.

Тот же JSON, что у eastar_net_usage.pl. По умолчанию live.
`--mode stub` печатает пример без сети.
"""

from __future__ import print_function

import argparse
import sys

from common import add_common_args, json_out, require_credentials, resolve_common, utc_now_iso
from nms import NmsClient
from parse import build_net_usage


def stub_payload(net_id):
    return {
        "source": "net_usage",
        "net_id": net_id,
        "ts": utc_now_iso(),
        "stub": True,
        "stations_enabled": "1 / 3",
        "stations_online": 1,
        "outroute_cn_db": 9.6,
        "inroute_cn_db": 5.8,
        "network_tx_kbit_s": 6.6,
        "network_rx_kbit_s": 8.6,
    }


def fetch_live(cfg):
    require_credentials(cfg)
    client = NmsClient(cfg)
    client.login()
    client.select_net()
    html = client.update(
        {"what": "widget", "datasrc": "WidgetNetworkStatus:%s" % cfg["net_id"]}
    )
    return build_net_usage(html, cfg["net_id"], utc_now_iso())


def main(argv=None):
    parser = argparse.ArgumentParser(description="Eastar NMS net_usage → JSON for Zabbix")
    add_common_args(parser)
    args = parser.parse_args(argv)
    cfg = resolve_common(args)
    if cfg["mode"] == "stub":
        payload = stub_payload(cfg["net_id"])
    else:
        payload = fetch_live(cfg)
    json_out(payload)
    return 0


if __name__ == "__main__":
    sys.exit(main())
