# Eastar NMS → Zabbix: подключение и конфигурация

Сбор метрик WEB NMS Eastar для Zabbix. Агент на **AGENT_HOST** вызывает коллектор, коллектор логинится в NMS и печатает JSON в stdout.

Два варианта коллектора, **один и тот же JSON** и одни и те же ключи Zabbix:

| | Python | Perl |
|--|--------|------|
| Когда | на хосте нет Perl и не хочется его ставить | Perl уже стоит |
| Запуск | `python3 eastar_*.py` | `perl eastar_*.pl` |
| Зависимости | только `python3` 3.6+ из системы | `perl` + `LWP::UserAgent`, `JSON`, `HTTP::Cookies` |
| Английский NMS (СТЭККОМ) | да | да |
| Русский NMS (ГП КС) | да (`кбит/с`, `дБ`, `Передача`/`Приём`) | нет, поля остаются `null` |

Оба читают один `config.env`. В Zabbix выбирается **один** вариант в `UserParameter`, не оба сразу.

Код: [`eastar_nms/`](../eastar_nms/).

| Набор | Файлы |
|-------|--------|
| Python | [eastar_net_usage.py](../eastar_nms/eastar_net_usage.py), [eastar_hub_usage.py](../eastar_nms/eastar_hub_usage.py), [common.py](../eastar_nms/common.py), [nms.py](../eastar_nms/nms.py), [parse.py](../eastar_nms/parse.py), [hub.py](../eastar_nms/hub.py) |
| Perl | [eastar_net_usage.pl](../eastar_nms/eastar_net_usage.pl), [eastar_hub_usage.pl](../eastar_nms/eastar_hub_usage.pl), [EastarNms.pm](../eastar_nms/EastarNms.pm) |
| общее | [config.env.example](../eastar_nms/config.env.example), [README](../eastar_nms/README.md) |

## Схема

```text
Zabbix Server  --(TCP 10050)-->  AGENT_HOST (zabbix_agentd + python3 или perl)
                                      |
                                      | http или https, как в EASTAR_NMS_URL
                                      v
                               Eastar NMS
                                      |
                                      | login + POST /update/
                                      v
                               JSON → stdout (UserParameter)
```

1. Zabbix Server опрашивает агент на **AGENT_HOST** (это не обязан быть vz2/vz3).
2. Агент вызывает **либо** `python3`, **либо** `perl`.
3. Скрипт логинится в NMS и печатает JSON.
4. Master-item хранит JSON; dependent items / LLD режут метрики (JSONPath). JSONPath одинаковый для обоих языков.

Коллекторы ставятся туда, где крутится агент и открыт путь до NMS. Хост приложения сам по себе не подходит, если с него NMS недоступен.

Проверенный пример: тестовый NMS `https://192.168.10.49` открывается с **vz3** (`192.168.3.23`) и не открывается с **vz2**.

## Подготовка AGENT_HOST

Порт берётся из URL: `https://` — 443, `http://` — 80. ГП КС в примерах ниже — HTTP.

```bash
# СТЭККОМ / тестовый NMS
curl -k -sS -o /dev/null -w 'http=%{http_code} time=%{time_total}\n' \
  --connect-timeout 5 https://192.168.10.49/

# ГП КС
curl -sS -o /dev/null -w 'http=%{http_code} time=%{time_total}\n' \
  --connect-timeout 5 http://10.142.0.4/
```

Ожидается `302` (редирект на логин) или `200`. Timeout — чинить маршрут с **этого** хоста, скрипты тут не помогут.

### Что поставить

Нужен Zabbix Agent и чтение `config.env` пользователем агента (обычно `zabbix`). Интерпретатор — один из двух.

**Python** (предпочтительно, если Perl ещё не стоит):

```bash
python3 --version
# 3.6 и новее. pip-пакеты не нужны
```

**Perl:**

```bash
perl -MLWP::UserAgent -MJSON -MHTTP::Cookies -e 'print "OK\n"'
```

Если модулей нет (RHEL/CentOS):

```bash
yum install -y perl-libwww-perl perl-JSON perl-HTTP-Cookies
```

Каталог на AGENT_HOST:

```text
/usr/local/projects/ai_report/eastar_nms/
```

Другой путь — поправьте его в `UserParameter`.

## Деплой

`AGENT_HOST` — SSH-имя или IP хоста с агентом.

### 1. Скопировать файлы

С машины, где есть репозиторий `ai_report`. Весь репозиторий на агент не нужен.

**Python:**

```bash
AGENT_HOST=vz3
DEST=/usr/local/projects/ai_report/eastar_nms

ssh "$AGENT_HOST" "mkdir -p $DEST"

rsync -az \
  eastar_nms/eastar_net_usage.py \
  eastar_nms/eastar_hub_usage.py \
  eastar_nms/common.py \
  eastar_nms/nms.py \
  eastar_nms/parse.py \
  eastar_nms/hub.py \
  eastar_nms/check_widgets.py \
  eastar_nms/config.env.example \
  "${AGENT_HOST}:${DEST}/"

ssh "$AGENT_HOST" "chmod 755 $DEST/*.py"
```

**Perl:**

```bash
rsync -az \
  eastar_nms/EastarNms.pm \
  eastar_nms/eastar_net_usage.pl \
  eastar_nms/eastar_hub_usage.pl \
  eastar_nms/config.env.example \
  "${AGENT_HOST}:${DEST}/"

ssh "$AGENT_HOST" "chmod 755 $DEST/*.pl"
```

Можно скопировать оба набора: они не мешают друг другу и читают один `config.env`.

### 2. `config.env`

Файл не коммитится.

```bash
cd /usr/local/projects/ai_report/eastar_nms
cp config.env.example config.env
chmod 640 config.env
chown root:zabbix config.env
```

| Переменная | Описание | Пример |
|------------|----------|--------|
| `EASTAR_NMS_URL` | Базовый URL без `#/...` | `https://start.steccom.ru` или `http://10.142.0.4` |
| `EASTAR_NMS_LOGIN` | Логин NMS | из ТЗ |
| `EASTAR_NMS_PASSWORD` | Пароль NMS | из ТЗ |
| `EASTAR_NET_ID` | `net_id` сети | `1`, на ГП КС например `9035` |
| `EASTAR_FILTER` | Подстрока имени контроллера (`hub_usage`) | `AM8` или `AM6 E04` |
| `EASTAR_TIMEOUT` | HTTP timeout, сек | `20` |

Приоритет у обоих коллекторов: **CLI → окружение → `config.env` → `config.env.example`**.

Пример для СТЭККОМ:

```bash
EASTAR_NMS_URL=https://start.steccom.ru
EASTAR_NET_ID=1
EASTAR_FILTER=AM8
```

Пример для ГП КС:

```bash
EASTAR_NMS_URL=http://10.142.0.4
EASTAR_NET_ID=9035
EASTAR_FILTER=AM6 E04
```

`python3` без логина и пароля (в example стоят `CHANGE_ME`) сразу завершается и просит заполнить `config.env`. В NMS при этом запрос не уходит.

### 3. Ручная проверка

Каталог один. Ниже одни и те же сценарии двумя командами.

```bash
cd /usr/local/projects/ai_report/eastar_nms
```

Сеть СТЭККОМ, живые данные (`net_id=1`) и пустая тестовая сеть (`net_id=21`, ожидаются нули):

```bash
python3 eastar_net_usage.py --nms-url https://start.steccom.ru --net-id 1
python3 eastar_net_usage.py --nms-url https://start.steccom.ru --net-id 21

perl eastar_net_usage.pl --nms-url https://start.steccom.ru --net-id 1
perl eastar_net_usage.pl --nms-url https://start.steccom.ru --net-id 21
```

Сеть ГП КС. Цифры в JSON отдаёт Python. Perl на этой странице ищет английские `kbps` / `dB` и оставляет поля `null`.

```bash
python3 eastar_net_usage.py --nms-url http://10.142.0.4 --net-id 9035
python3 eastar_net_usage.py --nms-url http://10.142.0.4 --net-id 8978

perl eastar_net_usage.pl --nms-url http://10.142.0.4 --net-id 9035
```

Хаб. Фильтр — подстрока имени контроллера.

```bash
python3 eastar_hub_usage.py --nms-url https://start.steccom.ru --net-id 1 --filter AM8
perl    eastar_hub_usage.pl  --nms-url https://start.steccom.ru --net-id 1 --filter AM8

python3 eastar_hub_usage.py --nms-url http://10.142.0.4 --net-id 20 --filter 'AM6 E04'
perl    eastar_hub_usage.pl  --nms-url http://10.142.0.4 --net-id 20 --filter 'AM6 E04'
```

Без сети, только проверка разбора HTML (есть у Python):

```bash
python3 check_widgets.py
python3 eastar_net_usage.py --mode stub --net-id 1
python3 eastar_hub_usage.py --mode stub --filter 'AM6 E04'
```

`--mode stub` в NMS не ходит и печатает учебный JSON (`"stub": true`). У Perl такого режима нет: `perl eastar_*.pl` всегда live.

В stdout живого прогона `"stub": false`. Пустой `controllers: []` значит, что фильтр не совпал с именами на этом NMS.

#### Пример JSON, сеть СТЭККОМ (`net_id=1`)

Так отвечают и Python, и Perl. Порядок ключей и пробелы могут отличаться, имена полей нет.

```json
{
  "source": "net_usage",
  "net_id": 1,
  "stub": false,
  "stations_enabled": "5 / 6",
  "stations_online": 1,
  "stations_down": 4,
  "stations_cn_db": 13.2,
  "stations_rx_kbit_s": 5.3,
  "controllers_enabled": "5 / 6",
  "controllers_online": 1,
  "controllers_down": 4,
  "controllers_cn_db": 11.0,
  "controllers_rx_kbit_s": 4.5,
  "inroute_cn_db": 13.2,
  "network_rx_kbit_s": 5.3,
  "ts": "2026-09-30T08:20:15Z"
}
```

Для `--net-id 21` те же поля, значения станций и трафика — нули.

#### Пример JSON, сеть ГП КС (`net_id=9035`, Python)

```json
{
  "source": "net_usage",
  "net_id": 9035,
  "stub": false,
  "stations_enabled": "6 / 10",
  "stations_online": 2,
  "stations_down": 4,
  "stations_cn_db": 16.2,
  "stations_rx_kbit_s": 8.6,
  "controllers_enabled": "6 / 10",
  "controllers_online": 2,
  "controllers_down": 4,
  "controllers_cn_db": 11.0,
  "controllers_rx_kbit_s": 4.1,
  "inroute_cn_db": 16.2,
  "network_rx_kbit_s": 8.6,
  "ts": "2026-09-30T08:21:36Z"
}
```

#### Пример JSON, хаб СТЭККОМ (`--filter AM8`)

```json
{
  "source": "hub_usage",
  "net_id": 1,
  "filter": "AM8",
  "stub": false,
  "controllers": [
    {
      "cid": 13,
      "name": "AM8 BD10 SR1900 H_V SR1300",
      "tx_kbit_s": 14.1,
      "rx_kbit_s": 3.6
    }
  ],
  "ts": "2026-09-30T08:43:56Z"
}
```

#### Пример JSON, хаб ГП КС (`--filter 'AM6 E04'`, Python)

`cid` заполняется, если `/updatetree/` вернул то же имя. Иначе в строке `cid: null`, трафик всё равно есть.

```json
{
  "source": "hub_usage",
  "net_id": 20,
  "filter": "AM6 E04",
  "stub": false,
  "controllers": [
    {
      "cid": null,
      "name": "AM6 E04 01 B08 R_V 2_1200",
      "tx_kbit_s": 2431.3,
      "rx_kbit_s": 112.6
    }
  ],
  "ts": "2026-09-30T08:41:05Z"
}
```

На этой же странице в колонке уровней бывает `Передача: 4` без `кбит/с`. Это не трафик, в `tx_kbit_s` оно не попадает.

### 4. UserParameter

Файл `/etc/zabbix/zabbix_agentd.d/eastar_nms.conf`. Ключи одинаковые, меняется только команда. Оставьте один блок.

**Python:**

```ini
UserParameter=eastar.nms.net_usage,/usr/bin/python3 /usr/local/projects/ai_report/eastar_nms/eastar_net_usage.py
UserParameter=eastar.nms.hub_usage[*],/usr/bin/python3 /usr/local/projects/ai_report/eastar_nms/eastar_hub_usage.py --filter "$1"
```

**Perl:**

```ini
UserParameter=eastar.nms.net_usage,/usr/bin/perl /usr/local/projects/ai_report/eastar_nms/eastar_net_usage.pl
UserParameter=eastar.nms.hub_usage[*],/usr/bin/perl /usr/local/projects/ai_report/eastar_nms/eastar_hub_usage.pl --filter "$1"
```

В конфиге агента:

```ini
Include=/etc/zabbix/zabbix_agentd.d/*.conf
Server=<IP_ZABBIX_SERVER>
# ServerActive=<IP_ZABBIX_SERVER>
Hostname=<имя_хоста_в_Zabbix>
```

```bash
systemctl restart zabbix-agent || service zabbix-agentd restart

zabbix_agentd -t eastar.nms.net_usage
zabbix_agentd -t 'eastar.nms.hub_usage[AM8]'
# с Zabbix Server:
zabbix_get -s <AGENT_HOST_IP> -k eastar.nms.net_usage
zabbix_get -s <AGENT_HOST_IP> -k 'eastar.nms.hub_usage[AM6 E04]'
```

### 5. Хост в Zabbix UI

Хост в UI — это **AGENT_HOST** (интерфейс Agent → IP этого хоста), не адрес NMS.

| Item | Type | Key | Type of info | Notes |
|------|------|-----|--------------|-------|
| Eastar net_usage JSON | Zabbix agent | `eastar.nms.net_usage` | Text | Master, 60–120s |
| Eastar hub_usage JSON | Zabbix agent | `eastar.nms.hub_usage[{$EASTAR.FILTER}]` | Text | Master |
| Stations online | Dependent | master = net_usage | Numeric | JSONPath |

| Макрос | Пример | Назначение |
|--------|--------|------------|
| `{$EASTAR.FILTER}` | `AM8` или `AM6 E04` | аргумент `hub_usage` |
| `{$EASTAR.NET_ID}` | `1` или `9035` | если пробрасываете `--net-id` в UserParameter |

`net_id` скрипт берёт из `config.env` / `EASTAR_NET_ID`, если в UserParameter нет `--net-id`. Для второй сети заведите отдельный ключ или отдельный `config.env`, а не один master на все сети.

#### JSONPath для `net_usage`

| Метрика | JSONPath |
|---------|----------|
| stations_online | `$.stations_online` |
| stations_down | `$.stations_down` |
| stations_cn_db | `$.stations_cn_db` |
| stations_rx_kbit_s | `$.stations_rx_kbit_s` |
| controllers_online | `$.controllers_online` |
| controllers_down | `$.controllers_down` |
| controllers_cn_db | `$.controllers_cn_db` |
| controllers_rx_kbit_s | `$.controllers_rx_kbit_s` |

`stations_enabled` (`"5 / 6"`) — текстовый item.

#### `hub_usage`

Один контроллер: `$.controllers[0].tx_kbit_s`, `$.controllers[0].rx_kbit_s`, `$.controllers[0].name`.

LLD:

```text
$.controllers[*]
{#CID}=$.cid
{#NAME}=$.name
```

| Name | JSONPath |
|------|----------|
| Tx [{#NAME}] | `$.controllers[?(@.cid=='{#CID}')].tx_kbit_s.first()` |
| Rx [{#NAME}] | `$.controllers[?(@.cid=='{#CID}')].rx_kbit_s.first()` |

Точный JSONPath зависит от версии Zabbix. Если Python на ГП КС отдал `cid: null`, LLD по `{#CID}` строку не сопоставит: смотрите `{#NAME}` или дождитесь `cid` из `/updatetree/`.

### 6. Триггеры

- `stations_online = 0` длительно → warning
- `controllers_down > 0` → warning
- нет данных master-item > 10 мин → high (скрипт, NMS, маршрут или агент)

## Чеклист

1. [ ] С AGENT_HOST открывается URL NMS (`curl`)
2. [ ] Есть `python3` **или** `perl` с LWP/JSON/Cookies
3. [ ] Скопирован соответствующий набор файлов из таблицы в начале
4. [ ] `config.env` заполнен, его читает пользователь агента
5. [ ] Ручной запуск из раздела 3 печатает JSON с `"stub": false`
6. [ ] В `eastar_nms.conf` один вариант UserParameter, агент перезапущен
7. [ ] `zabbix_agentd -t` и `zabbix_get` видят тот же JSON
8. [ ] В Zabbix UI интерфейс агента указывает на этот AGENT_HOST
9. [ ] В `Server=` / `ServerActive=` указан ваш Zabbix Server

## Смена хоста агента

1. На новом хосте повторить подготовку, копирование и UserParameter.
2. Проверить URL NMS именно с нового адреса (ACL часто привязан к source IP).
3. В Zabbix сменить Agent interface или завести новый хост и перенести шаблон.
4. На старом хосте убрать UserParameter. `config.env` с паролем не оставлять доступным всем.

## Как коллекторы ходят в NMS

| Скрипт | Запросы | Откуда цифры |
|--------|---------|----------------|
| `eastar_net_usage.py` и `.pl` | login → `GET /net_usage/?net_id=` → `POST /update/` `WidgetNetworkStatus:{net_id}` | таблица «Состояние сети» / Network state |
| `eastar_hub_usage.pl` | login → `GET /net_usage/?net_id=` → `/updatetree/` → `WidgetControllerStatus:{cid}` | виджет контроллера, подписи `TX:` / `RX:` / `kbps` |
| `eastar_hub_usage.py` | то же и ещё `GET /hub_usage/?net_id=` | если в таблице есть имена контроллеров — трафик из неё (`Передача`/`Приём` или `TX:`/`RX:`); иначе те же виджеты, что у Perl |

После логина NMS держит в сессии сеть по умолчанию. Один `WidgetNetworkStatus:{net_id}` сеть не переключает. Оба коллектора перед `/update/` делают `GET /net_usage/?net_id=N`. На хабе с одной сетью это незаметно, на ГП КС без этого приходят чужие метрики.

Отдельного REST JSON API у NMS нет.

Тестовый NMS: `https://192.168.10.49` (с vz3).  
СТЭККОМ: `https://start.steccom.ru`.  
ГП КС: `http://10.142.0.4`.

## Если цифры не сходятся

| Что видно | Что сделать |
|-----------|-------------|
| `curl` до NMS не проходит | маршрут с AGENT_HOST, не с рабочей станции. Для ГП КС это `http://`, не `https://` |
| `Missing EASTAR_NMS_LOGIN` (Perl) или просьба заполнить `config.env` (Python) | нет файла, нет прав, в example остались `CHANGE_ME` |
| login failed / timeout | URL, пароль, TLS, proxy, `EASTAR_TIMEOUT` |
| `net_id=21` совпадает с сетью 1 | старый код без `select_net`. Обновить скрипты |
| на ГП КС у Perl все поля `null`, у Python есть цифры | страница русская. Для этой площадки в UserParameter оставить Python |
| `controllers: []` | `--filter` / `EASTAR_FILTER` не входит в имя. Для хаба ГП КС пример фильтра: `AM6 E04` |
| `tx_kbit_s` слишком маленький (4, 8, …) | в ячейку попали уровни `Передача: 4` без `кбит/с`. Нужен актуальный `parse.py` |
| `[m\|ZBX_NOTSUPPORTED]` | путь к `python3` или `perl`, `Include=`, restart, SELinux |
| `zabbix_get` timeout | Server → AGENT_HOST `:10050`, `Server=` в агенте |
| JSON есть, dependent пустые | JSONPath. Для хаба без `cid` не фильтруйте LLD только по `{#CID}` |

Логи агента: обычно `/var/log/zabbix/zabbix_agentd.log`.
