# Eastar NMS collectors (Zabbix)

Live-коллекторы на **Perl**. Ставятся на **хост с Zabbix Agent**, у которого есть HTTPS до NMS (не обязательно vz3 / ai_report).

- `eastar_net_usage.pl` — сеть (`WidgetNetworkStatus`)
- `eastar_hub_usage.pl` — контроллеры Tx/Rx с фильтром имени
- `EastarNms.pm` — login / `/update/` / `/updatetree/`
- `config.env.example` → скопировать в `config.env` на AGENT_HOST

Документация (деплой на любой AGENT_HOST): [docs/eastar-nms-zabbix.md](../docs/eastar-nms-zabbix.md)

## Файлы на GitHub

- [eastar_net_usage.pl](https://github.com/kobyzev-yuri/ai_report/blob/main/eastar_nms/eastar_net_usage.pl)
- [eastar_hub_usage.pl](https://github.com/kobyzev-yuri/ai_report/blob/main/eastar_nms/eastar_hub_usage.pl)
- [EastarNms.pm](https://github.com/kobyzev-yuri/ai_report/blob/main/eastar_nms/EastarNms.pm)
- [config.env.example](https://github.com/kobyzev-yuri/ai_report/blob/main/eastar_nms/config.env.example)

