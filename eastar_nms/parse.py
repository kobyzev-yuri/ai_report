"""Разбор виджетов Eastar NMS: английский и русский интерфейс.

Только стандартная библиотека, чтобы на хосте агента не ставить Perl и pip-пакеты.
"""

from __future__ import print_function

import re
from html.parser import HTMLParser

_RATE_RE = re.compile(
    r"([0-9]+(?:[.,][0-9]+)?)\s*(?:kbps|кбит/с)",
    re.IGNORECASE,
)
_TX_RE = re.compile(
    r"(?:\bTX\b|Передача)\s*(?:traffic)?\s*:?\s*"
    r"([0-9]+(?:[.,][0-9]+)?)\s*(?:kbps|кбит/с)",
    re.IGNORECASE,
)
_RX_RE = re.compile(
    r"(?:\bRX\b|При[её]м)\s*(?:traffic)?\s*:?\s*"
    r"([0-9]+(?:[.,][0-9]+)?)\s*(?:kbps|кбит/с)",
    re.IGNORECASE,
)
_DB_RE = re.compile(
    r"([0-9]+(?:[.,][0-9]+)?)\s*(?:dB|дБ)",
    re.IGNORECASE,
)
_ENABLED_RE = re.compile(r"(\d+)\s*/\s*(\d+)")
_COUNT_RE = re.compile(r"\s*(-|\d+)\s*")
_OF_RE = re.compile(r"\d+\s*(?:of|из|/)\s*\d+", re.IGNORECASE)
_STATE_RE = re.compile(r"^(?:operation|работает|state|состояние)\b", re.IGNORECASE)


def _num(raw):
    return float(raw.replace(",", "."))


def parse_rate(text):
    if not text:
        return None
    match = _RATE_RE.search(text.replace("\xa0", " "))
    return _num(match.group(1)) if match else None


def parse_db(text):
    if not text:
        return None
    match = _DB_RE.search(text.replace("\xa0", " "))
    return _num(match.group(1)) if match else None


def parse_link_rates(text):
    """TX/RX или Передача/Приём, только вместе с единицей скорости."""
    if not text:
        return None, None
    cleaned = text.replace("\xa0", " ")
    tx_match = _TX_RE.search(cleaned)
    rx_match = _RX_RE.search(cleaned)
    tx = _num(tx_match.group(1)) if tx_match else None
    rx = _num(rx_match.group(1)) if rx_match else None
    return tx, rx


def _channel_kind(label):
    folded = label.lower().replace("ё", "е")
    if "контрол" in folded or "controller" in folded:
        return "controllers"
    if "станц" in folded or "station" in folded:
        return "stations"
    return None


def _controller_name(cell):
    text = re.sub(r"\s+", " ", cell.replace("\xa0", " ")).strip()
    text = re.sub(r"^[^\w]+", "", text, flags=re.UNICODE).strip()
    if not text or ":" in text or _STATE_RE.match(text):
        return None
    if _channel_kind(text) or _OF_RE.search(text) or _RATE_RE.search(text):
        return None
    if parse_db(text) is not None and len(text) < 24:
        return None
    if not re.search(r"\d", text) or not re.search(r"[A-Za-zА-Яа-яЁё]", text):
        return None
    if len(text) > 80:
        return None
    return text


class _TableParser(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self, convert_charrefs=True)
        self.rows = []
        self._skip = 0
        self._in_cell = False
        self._cell_parts = []
        self._row = []
        self._row_has_td = False

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        if tag in ("script", "style"):
            self._skip += 1
            return
        if self._skip:
            return
        if tag == "tr":
            self._row = []
            self._row_has_td = False
        elif tag in ("td", "th"):
            self._in_cell = True
            self._cell_parts = []
            if tag == "td":
                self._row_has_td = True
        elif tag == "br" and self._in_cell:
            self._cell_parts.append("\n")

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag in ("script", "style") and self._skip:
            self._skip -= 1
            return
        if self._skip:
            return
        if tag in ("td", "th") and self._in_cell:
            text = "".join(self._cell_parts).replace("\xa0", " ")
            text = re.sub(r"[ \t\r\f\v]+", " ", text)
            text = re.sub(r" *\n *", "\n", text).strip()
            self._row.append(text)
            self._in_cell = False
        elif tag == "tr" and self._row_has_td and self._row:
            self.rows.append(self._row)
            self._row = []
            self._row_has_td = False

    def handle_data(self, data):
        if self._skip or not self._in_cell:
            return
        self._cell_parts.append(data)


def iter_td_rows(html):
    parser = _TableParser()
    parser.feed(html or "")
    parser.close()
    return parser.rows


def parse_net_status(html):
    """Строка Network state / «Состояние сети»."""
    found = {}
    for cells in iter_td_rows(html):
        if not cells:
            continue
        kind = _channel_kind(cells[0])
        if kind is None:
            continue
        enabled = None
        cn = None
        rate = None
        counts = []
        for cell in cells[1:]:
            if rate is None and _RATE_RE.search(cell):
                rate = parse_rate(cell)
                continue
            if cn is None and "\n" not in cell:
                cn_val = parse_db(cell)
                if cn_val is not None:
                    cn = cn_val
                    continue
            if enabled is None:
                match = _ENABLED_RE.search(cell)
                if match and not _RATE_RE.search(cell):
                    enabled = "%s / %s" % (int(match.group(1)), int(match.group(2)))
                    continue
            count_match = _COUNT_RE.fullmatch(cell)
            if count_match:
                counts.append(0 if count_match.group(1) == "-" else int(count_match.group(1)))
        found["%s_enabled" % kind] = enabled
        found["%s_online" % kind] = counts[0] if counts else None
        found["%s_down" % kind] = counts[1] if len(counts) > 1 else None
        found["%s_cn_db" % kind] = cn
        found["%s_rx_kbit_s" % kind] = rate
    return found


def parse_hub_channels(html):
    """Строки контроллеров с таблицы hub_usage.

    Английская страница отдаёт сводку без имени контроллера — такие строки
    пропускаются, дальше имена берутся из /updatetree/. Русская страница
    сразу содержит имя и Передача/Приём.
    """
    parsed = []
    for cells in iter_td_rows(html):
        name = None
        traffic = None
        for cell in cells:
            if name is None:
                name = _controller_name(cell)
            if traffic is None and _RATE_RE.search(cell):
                traffic = cell
        if not name or traffic is None:
            continue
        tx, rx = parse_link_rates(traffic)
        parsed.append({"name": name, "tx_kbit_s": tx, "rx_kbit_s": rx})
    return parsed


def build_net_usage(html, net_id, ts):
    row = parse_net_status(html)
    network_rx = row.get("stations_rx_kbit_s")
    if network_rx is None:
        network_rx = row.get("controllers_rx_kbit_s")
    return {
        "source": "net_usage",
        "net_id": int(net_id),
        "ts": ts,
        "stub": False,
        "stations_enabled": row.get("stations_enabled"),
        "stations_online": row.get("stations_online"),
        "stations_down": row.get("stations_down"),
        "stations_cn_db": row.get("stations_cn_db"),
        "stations_rx_kbit_s": row.get("stations_rx_kbit_s"),
        "controllers_enabled": row.get("controllers_enabled"),
        "controllers_online": row.get("controllers_online"),
        "controllers_down": row.get("controllers_down"),
        "controllers_cn_db": row.get("controllers_cn_db"),
        "controllers_rx_kbit_s": row.get("controllers_rx_kbit_s"),
        "inroute_cn_db": row.get("stations_cn_db"),
        "network_rx_kbit_s": network_rx,
    }
