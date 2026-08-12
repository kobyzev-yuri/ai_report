#!/usr/bin/env python3
"""Zabbix collector A: WEB NMS Eastar net_usage (URL1).

Stub mode prints sample JSON. Live fetch will use:
  {NMS_URL}/#/net_usage/?net_id={NETID}
"""

from __future__ import annotations

import argparse
import json
import sys

from common import add_common_args, ensure_stub_or_exit, resolve_common, utc_now_iso


def stub_payload(net_id: int) -> dict:
    # Shape mirrors highlighted fields on net_usage screenshot / TZ URL1.
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


def fetch_live(cfg: dict) -> dict:
    # Placeholder until VPN + credentials + real XHR endpoints are available.
    raise NotImplementedError(
        "live net_usage: login to {url} as {login}, fetch net_id={net_id}".format(
            url=cfg["nms_url"],
            login=cfg["login"],
            net_id=cfg["net_id"],
        )
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Eastar NMS net_usage → JSON for Zabbix")
    add_common_args(parser)
    args = parser.parse_args(argv)
    cfg = resolve_common(args)
    ensure_stub_or_exit(cfg["mode"], "eastar_net_usage.py")

    # Credentials are resolved but unused in stub mode on purpose.
    _ = (cfg["login"], cfg["password"], cfg["nms_url"])

    if cfg["mode"] == "stub":
        payload = stub_payload(cfg["net_id"])
    else:
        payload = fetch_live(cfg)

    json.dump(payload, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
