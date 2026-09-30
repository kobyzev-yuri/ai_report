"""Разбор виджетов Eastar без сети: EN как у Perl, RU как на ГП КС."""

from __future__ import print_function

import unittest

from hub import collect_controllers
from parse import build_net_usage, parse_hub_channels, parse_link_rates


EN_NET = """
<table class="centered networkstate">
  <tr><td>Stations RX</td><td>5 / 6</td><td>1</td><td>4</td>
      <td>13.2 dB</td><td>0</td><td>0</td>
      <td>Station RX: 5.3 kbps</td></tr>
  <tr><td>Controllers RX</td><td>5 / 6</td><td>1</td><td>4</td>
      <td>11 dB</td><td>0</td><td>0</td>
      <td>Controllers RX: 4.5 kbps</td></tr>
</table>
"""

RU_NET = """
<table class="centered networkstate">
  <tr><td>Приём станций</td><td>6 / 10</td><td>2</td><td>4</td>
      <td>16,2 дБ</td><td>0</td><td>0</td>
      <td>Приём станции: 8.6 кбит/с</td></tr>
  <tr><td>Прием контроллеров</td><td>6 / 10</td><td>2</td><td>4</td>
      <td>11 дБ</td><td>0</td><td>0</td>
      <td>Прием контроллеров: 4.1 кбит/с</td></tr>
</table>
"""

EN_HUB_PAGE = """
<table class="centered networkstate">
  <tr>
    <td colspan="2">Operation: 2026-09-28 11:53 (1d 23h)</td>
    <td>5 of 6</td><td>1</td><td>4</td>
    <td>Controller self: 15.9 dB</td>
    <td></td><td></td>
    <td>TX: 14.1 kbps<br/>RX: 3.6 kbps</td>
  </tr>
</table>
"""

EN_WIDGET = """
<table><tr>
  <td>Operation: 2026-09-28</td>
  <td>TX: 14.1 kbps<br/>RX: 3.6 kbps</td>
</tr></table>
"""

RU_HUB = """
<table>
  <tr>
    <td>&#8645; AM6 E04 01 B08 R_V 2_1200</td>
    <td>Работает: 2026-09-29 09:23</td>
    <td>125 из 165</td><td>21</td><td>104</td>
    <td>Собственный прием: 51,1 дБ</td>
    <td>Передача: 8<br>Прием: 3</td>
    <td></td>
    <td>Передача: 2431.3 кбит/с<br/>Прием: 112.6 кбит/с</td>
  </tr>
  <tr>
    <td>&#8675; I AM6 E03 01 B05 L_H 4_800</td>
    <td>Работает: 2026-09-29</td>
    <td>1 из 2</td><td>1</td><td>0</td>
    <td>Собственный прием: 11 дБ</td>
    <td>Передача: 4<br>Прием: 1</td>
    <td></td>
    <td>Передача: 10.5 кбит/с<br/>Прием: 1,5 кбит/с</td>
  </tr>
</table>
"""


class WidgetParseTest(unittest.TestCase):
    def test_english_net_matches_perl_fields(self):
        row = build_net_usage(EN_NET, 1, "x")
        self.assertEqual(row["stations_enabled"], "5 / 6")
        self.assertEqual(row["stations_online"], 1)
        self.assertEqual(row["stations_down"], 4)
        self.assertEqual(row["stations_cn_db"], 13.2)
        self.assertEqual(row["stations_rx_kbit_s"], 5.3)
        self.assertEqual(row["controllers_rx_kbit_s"], 4.5)
        self.assertEqual(row["controllers_cn_db"], 11.0)
        self.assertEqual(row["inroute_cn_db"], 13.2)
        self.assertEqual(row["network_rx_kbit_s"], 5.3)
        self.assertFalse(row["stub"])

    def test_russian_net_comma_db_and_kbit(self):
        row = build_net_usage(RU_NET, 9035, "x")
        self.assertEqual(row["stations_enabled"], "6 / 10")
        self.assertEqual(row["stations_online"], 2)
        self.assertEqual(row["stations_cn_db"], 16.2)
        self.assertEqual(row["stations_rx_kbit_s"], 8.6)
        self.assertEqual(row["controllers_rx_kbit_s"], 4.1)
        self.assertEqual(row["network_rx_kbit_s"], 8.6)

    def test_english_hub_page_has_no_named_rows(self):
        self.assertEqual(parse_hub_channels(EN_HUB_PAGE), [])

    def test_english_hub_widget_path(self):
        rows = collect_controllers(
            EN_HUB_PAGE,
            [{"cid": 13, "n": "AM8 BD10 SR1900 H_V SR1300", "netid": 1}],
            1,
            "AM8",
            fetch_widget=lambda cid: EN_WIDGET,
        )
        self.assertEqual(rows[0]["cid"], 13)
        self.assertEqual(rows[0]["tx_kbit_s"], 14.1)
        self.assertEqual(rows[0]["rx_kbit_s"], 3.6)

    def test_russian_hub_ignores_level_counters(self):
        self.assertEqual(parse_link_rates("Передача: 4\nПрием: 1"), (None, None))
        rows = collect_controllers(RU_HUB, [], 20, "AM6 E04")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["name"], "AM6 E04 01 B08 R_V 2_1200")
        self.assertEqual(rows[0]["tx_kbit_s"], 2431.3)
        self.assertEqual(rows[0]["rx_kbit_s"], 112.6)
        self.assertIsNone(rows[0]["cid"])

    def test_russian_hub_attaches_longest_tree_name(self):
        rows = collect_controllers(
            RU_HUB,
            [
                {"cid": 3, "n": "AM6 E04", "netid": 20},
                {"cid": 7, "n": "AM6 E04 01 B08 R_V 2_1200", "netid": 20},
            ],
            20,
            "AM6 E04",
        )
        self.assertEqual(rows[0]["cid"], 7)

    def test_dash_online_is_zero(self):
        html = """
        <table><tr>
          <td>Stations RX</td><td>1 / 2</td><td>-</td><td>1</td>
          <td>0 dB</td><td>0</td><td>0</td>
          <td>Station RX: 0.0 kbps</td>
        </tr></table>
        """
        row = build_net_usage(html, 1, "x")
        self.assertEqual(row["stations_online"], 0)
        self.assertEqual(row["stations_down"], 1)


if __name__ == "__main__":
    unittest.main()
