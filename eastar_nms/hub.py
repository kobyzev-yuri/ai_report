"""Сбор контроллеров hub_usage: страница хаба и виджеты по дереву."""

from __future__ import print_function

import re

from parse import parse_hub_channels, parse_link_rates


def _norm_name(name):
    text = re.sub(r"\s+", " ", (name or "").replace("\xa0", " ")).strip().lower()
    return re.sub(r"^[^\w]+", "", text, flags=re.UNICODE).strip()


def _same_net(controller, net_id):
    raw = controller.get("netid")
    if raw is None:
        return True
    try:
        return int(raw) == int(net_id)
    except (TypeError, ValueError):
        return False


def _name_ok(name, key):
    if not key:
        return True
    return key.lower() in (name or "").lower()


def _match_cid(name, by_name):
    wanted = _norm_name(name)
    if not wanted:
        return None
    direct = by_name.get(wanted)
    if direct is not None and direct.get("cid") is not None:
        return int(direct["cid"])
    best = None
    best_len = 0
    for candidate, controller in by_name.items():
        if len(candidate) < 4 or len(wanted) < 4:
            continue
        if candidate not in wanted and wanted not in candidate:
            continue
        cid = controller.get("cid")
        if cid is None or len(candidate) <= best_len:
            continue
        best = int(cid)
        best_len = len(candidate)
    return best


def collect_controllers(page_html, tree_controllers, net_id, key, fetch_widget=None):
    """Если hub_usage уже содержит имена (русский NMS), трафик берётся со страницы.

    Английская сводка без имён разбирается через WidgetControllerStatus, как Perl.
    """
    controllers = [c for c in tree_controllers if _same_net(c, net_id)]
    by_name = {}
    for controller in controllers:
        title = controller.get("n") or ""
        if title:
            by_name[_norm_name(title)] = controller

    named = parse_hub_channels(page_html)
    if named:
        out = []
        for row in named:
            name = row["name"]
            if not _name_ok(name, key):
                continue
            out.append(
                {
                    "name": name,
                    "cid": _match_cid(name, by_name),
                    "tx_kbit_s": row.get("tx_kbit_s"),
                    "rx_kbit_s": row.get("rx_kbit_s"),
                }
            )
        return out

    if key:
        controllers = [c for c in controllers if key.lower() in (c.get("n") or "").lower()]

    out = []
    for controller in controllers:
        cid = controller.get("cid")
        if cid is None:
            continue
        html = fetch_widget(int(cid)) if fetch_widget else ""
        tx, rx = parse_link_rates(html)
        out.append(
            {
                "name": controller.get("n") or "",
                "cid": int(cid),
                "tx_kbit_s": tx,
                "rx_kbit_s": rx,
            }
        )
    return out
