# Eastar NMS collectors (Zabbix)

Два варианта одного коллектора. JSON одинаковый, ключи Zabbix не меняются.

**Python** (`eastar_*.py`) — то, чем пользоваться, если на хосте агента не хочется ставить Perl. Хватает `python3` из системы: HTTP и разбор HTML на стандартной библиотеке. Понимает английский интерфейс NMS (СТЭККОМ: `Stations RX`, `kbps`, `TX:`) и русский (ГП КС: `Приём станций`, `кбит/с`, `Передача` / `Приём`).

**Perl** (`eastar_*.pl`, `EastarNms.pm`) — прежний вариант. Его оставляем.

Оба читают один `config.env`.

- `eastar_net_usage.py` / `.pl` — сеть (`WidgetNetworkStatus`)
- `eastar_hub_usage.py` / `.pl` — контроллеры Tx/Rx, фильтр по имени
- `config.env.example` → скопировать в `config.env` на AGENT_HOST

После логина сессия NMS сидит на сети по умолчанию. Коллекторы делают `GET /net_usage/?net_id=N` перед виджетами. Для хаба Python дополнительно открывает `GET /hub_usage/?net_id=N`: на русском NMS таблица страницы уже содержит имена и трафик.

```bash
python3 eastar_net_usage.py --net-id 1
python3 eastar_hub_usage.py --filter 'AM6 E04'
# прежний запуск, если Perl уже стоит:
perl eastar_net_usage.pl --net-id 1
perl eastar_hub_usage.pl --filter 'AM8'
```

`--mode stub` печатает пример JSON без запроса к NMS.

Документация: [docs/eastar-nms-zabbix.md](../docs/eastar-nms-zabbix.md)
