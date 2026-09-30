# Eastar NMS collectors (Zabbix)

Два варианта одного коллектора. JSON и ключи Zabbix одинаковые. Оба читают `config.env` из этого каталога.

| | Python | Perl |
|--|--------|------|
| Сеть | `eastar_net_usage.py` | `eastar_net_usage.pl` |
| Хаб | `eastar_hub_usage.py` | `eastar_hub_usage.pl` |
| Общее | `common.py`, `nms.py`, `parse.py`, `hub.py` | `EastarNms.pm` |
| Зависимости | `python3` 3.6+ | `perl`, LWP, JSON, HTTP::Cookies |
| Русский NMS (ГП КС) | да | нет, поля `null` |

```bash
cp config.env.example config.env
# EASTAR_NMS_URL, LOGIN, PASSWORD, NET_ID, FILTER
```

СТЭККОМ, сеть 1 и пустая сеть 21:

```bash
python3 eastar_net_usage.py --nms-url https://start.steccom.ru --net-id 1
python3 eastar_net_usage.py --nms-url https://start.steccom.ru --net-id 21

perl eastar_net_usage.pl --nms-url https://start.steccom.ru --net-id 1
perl eastar_net_usage.pl --nms-url https://start.steccom.ru --net-id 21
```

ГП КС. Цифры печатает Python (`6 / 10`, `16.2`, `8.6 кбит/с`). Perl на русских подписях оставляет `null`.

```bash
python3 eastar_net_usage.py --nms-url http://10.142.0.4 --net-id 9035
perl    eastar_net_usage.pl  --nms-url http://10.142.0.4 --net-id 9035
```

Хаб, фильтр — подстрока имени:

```bash
python3 eastar_hub_usage.py --nms-url https://start.steccom.ru --net-id 1 --filter AM8
perl    eastar_hub_usage.pl  --nms-url https://start.steccom.ru --net-id 1 --filter AM8

python3 eastar_hub_usage.py --nms-url http://10.142.0.4 --net-id 20 --filter 'AM6 E04'
perl    eastar_hub_usage.pl  --nms-url http://10.142.0.4 --net-id 20 --filter 'AM6 E04'
```

Без NMS, только Python:

```bash
python3 check_widgets.py
python3 eastar_net_usage.py --mode stub
```

После логина сессия NMS сидит на сети по умолчанию. Оба коллектора перед виджетами открывают `GET /net_usage/?net_id=N`. Python для хаба ещё открывает `GET /hub_usage/?net_id=N` и, если в таблице есть имена, читает трафик оттуда.

Подробно: деплой, `UserParameter`, JSONPath — [docs/eastar-nms-zabbix.md](../docs/eastar-nms-zabbix.md).
