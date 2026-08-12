# Eastar NMS → Zabbix: подключение и конфигурация

Сбор метрик WEB NMS Eastar для Zabbix через Perl-коллекторы на **хосте с Zabbix Agent**, у которого есть HTTPS-доступ до NMS.

Код в репозитории: [`eastar_nms/`](https://github.com/kobyzev-yuri/ai_report/tree/main/eastar_nms)

| Файл | На GitHub |
|------|-----------|
| `eastar_net_usage.pl` | [ссылка](https://github.com/kobyzev-yuri/ai_report/blob/main/eastar_nms/eastar_net_usage.pl) |
| `eastar_hub_usage.pl` | [ссылка](https://github.com/kobyzev-yuri/ai_report/blob/main/eastar_nms/eastar_hub_usage.pl) |
| `EastarNms.pm` | [ссылка](https://github.com/kobyzev-yuri/ai_report/blob/main/eastar_nms/EastarNms.pm) |
| `config.env.example` | [ссылка](https://github.com/kobyzev-yuri/ai_report/blob/main/eastar_nms/config.env.example) |
| `README.md` (каталог) | [ссылка](https://github.com/kobyzev-yuri/ai_report/blob/main/eastar_nms/README.md) |

## Схема

```text
Zabbix Server  --(TCP 10050)-->  AGENT_HOST (zabbix_agentd + Perl-коллекторы)
                                      |
                                      | HTTPS :443
                                      v
                               Eastar NMS
                                      |
                                      | login + POST /update/
                                      v
                               JSON → stdout (UserParameter)
```

1. Zabbix Server опрашивает **агент на AGENT_HOST** (не обязан совпадать с ai_report/vz2/vz3).
2. Агент вызывает Perl-скрипт (`UserParameter`).
3. Скрипт логинится в NMS, забирает виджеты через `POST /update/`, печатает JSON.
4. Master-item хранит JSON; dependent items / LLD режут метрики (JSONPath).

### Где ставить коллекторы

| Условие | Требование |
|---------|------------|
| Исходящий HTTPS до NMS `:443` | **обязательно** с AGENT_HOST |
| Zabbix Agent | на том же AGENT_HOST |
| Доступ Server → Agent `:10050` | с Zabbix Server до AGENT_HOST |
| ai_report / Streamlit | **не нужен** на AGENT_HOST |

Коллекторы ставятся **туда, где крутится агент и открыт путь до NMS**. Хост приложения (например vz2) сам по себе не подходит, если с него NMS недоступен.

**Проверенный пример:** тестовый NMS `https://192.168.10.49` доступен с **vz3** (`192.168.3.23`); с **vz2** — нет. Если агент будет на другом хосте — сначала проверьте `:443` до NMS с **этого** хоста.

## Выбор и подготовка AGENT_HOST

Перед деплоем на целевом хосте:

```bash
# подставьте URL вашего NMS
NMS_URL=https://192.168.10.49

curl -k -sS -o /dev/null -w 'http=%{http_code} time=%{time_total}\n' \
  --connect-timeout 5 "$NMS_URL/"
# ожидается http=302 (редирект на /login/) или 200 после follow
```

Если timeout / нет маршрута — открыть ACL/маршрут **AGENT_HOST → NMS:443**, иначе перенос скриптов бесполезен.

### Требования к AGENT_HOST

| Компонент | Зачем |
|-----------|--------|
| `perl` | запуск коллекторов |
| Perl-модули: `LWP::UserAgent`, `JSON`, `HTTP::Cookies` | HTTPS + JSON |
| `zabbix_agentd` (или agent2 с аналогом UserParameter) | ключи Zabbix |
| Исходящий HTTPS до NMS | live-данные |
| Пользователь агента может читать `config.env` | секреты |

Проверка модулей:

```bash
perl -MLWP::UserAgent -MJSON -MHTTP::Cookies -e 'print "OK\n"'
```

Если модулей нет (RHEL/CentOS пример):

```bash
yum install -y perl-libwww-perl perl-JSON perl-HTTP-Cookies
# или аналог через cpan / пакеты дистрибутива
```

Путь установки на AGENT_HOST (рекомендуемый):

```text
/usr/local/projects/ai_report/eastar_nms/
```

Можно любой другой каталог — тогда поправьте пути в `UserParameter`.

## Деплой на произвольный AGENT_HOST

Ниже `AGENT_HOST` — SSH-имя или IP хоста с агентом (vz3, другой VM, хост рядом с NMS и т.д.).

### 1. Скопировать файлы

С машины, где есть репозиторий `ai_report`:

```bash
AGENT_HOST=vz3          # <-- замените на ваш хост/алиас SSH
DEST=/usr/local/projects/ai_report/eastar_nms

ssh "$AGENT_HOST" "mkdir -p $DEST"

rsync -az \
  eastar_nms/EastarNms.pm \
  eastar_nms/eastar_net_usage.pl \
  eastar_nms/eastar_hub_usage.pl \
  eastar_nms/config.env.example \
  "${AGENT_HOST}:${DEST}/"

ssh "$AGENT_HOST" "chmod 755 $DEST/*.pl"
```

Без rsync:

```bash
scp eastar_nms/EastarNms.pm \
    eastar_nms/eastar_net_usage.pl \
    eastar_nms/eastar_hub_usage.pl \
    eastar_nms/config.env.example \
    "${AGENT_HOST}:${DEST}/"
```

На AGENT_HOST **не нужен** весь ai_report — достаточно каталога `eastar_nms` (`.pm` + `.pl` + `config.env`).

### 2. Секреты NMS (`config.env`)

Файл **не коммитится**. На AGENT_HOST:

```bash
DEST=/usr/local/projects/ai_report/eastar_nms
cd "$DEST"
cp config.env.example config.env
chmod 640 config.env
# пользователь агента обычно zabbix
chown root:zabbix config.env
```

| Переменная | Описание | Пример |
|------------|----------|--------|
| `EASTAR_NMS_URL` | Базовый URL NMS (без `#/...`) | `https://192.168.10.49` |
| `EASTAR_NMS_LOGIN` | Логин NMS | из ТЗ |
| `EASTAR_NMS_PASSWORD` | Пароль NMS | из ТЗ |
| `EASTAR_NET_ID` | `net_id` сети | `1` |
| `EASTAR_FILTER` | Подстрока имени контроллера (`hub_usage`) | `AM8` / `AM6 E04` |
| `EASTAR_TIMEOUT` | HTTP timeout, сек | `20` |

CLI перекрывает env/файл:

```bash
perl eastar_net_usage.pl --net-id 1
perl eastar_hub_usage.pl --filter 'AM8'
perl eastar_hub_usage.pl --nms-url https://start.steccom.ru --filter 'AM6 E04'
```

Приоритет: **CLI → окружение → `config.env` → `config.env.example`**.

### 3. Ручная проверка на AGENT_HOST

```bash
cd /usr/local/projects/ai_report/eastar_nms
perl eastar_net_usage.pl
perl eastar_hub_usage.pl --filter AM8
```

Ожидается JSON в stdout, `"stub": false`.  
`controllers: []` — фильтр не совпал с именами контроллеров на этом NMS.

Пример **net_usage**:

```json
{
  "source": "net_usage",
  "net_id": 1,
  "stub": false,
  "stations_enabled": "3 / 5",
  "stations_online": 0,
  "stations_down": 3,
  "stations_cn_db": 0,
  "stations_rx_kbit_s": 0,
  "controllers_enabled": "3 / 5",
  "controllers_online": 0,
  "controllers_down": 3,
  "controllers_cn_db": 0,
  "controllers_rx_kbit_s": 0,
  "ts": "2026-08-12T19:03:52Z"
}
```

Пример **hub_usage**:

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
      "tx_kbit_s": 7.5,
      "rx_kbit_s": 0
    }
  ],
  "ts": "2026-08-12T19:04:41Z"
}
```

### 4. UserParameter на AGENT_HOST

Создать `/etc/zabbix/zabbix_agentd.d/eastar_nms.conf` (путь к скриптам поправьте при другой установке):

```ini
# Eastar NMS collectors — host where agent can reach NMS
UserParameter=eastar.nms.net_usage,/usr/bin/perl /usr/local/projects/ai_report/eastar_nms/eastar_net_usage.pl
UserParameter=eastar.nms.hub_usage[*],/usr/bin/perl /usr/local/projects/ai_report/eastar_nms/eastar_hub_usage.pl --filter "$1"
```

В основном конфиге агента:

```ini
Include=/etc/zabbix/zabbix_agentd.d/*.conf
Server=<IP_ZABBIX_SERVER>          # passive checks
# ServerActive=<IP_ZABBIX_SERVER>  # если используете active
Hostname=<имя_хоста_в_Zabbix>
```

Перезапуск:

```bash
systemctl restart zabbix-agent || service zabbix-agentd restart
```

Проверка:

```bash
# на AGENT_HOST
zabbix_agentd -t eastar.nms.net_usage
zabbix_agentd -t 'eastar.nms.hub_usage[AM8]'

# с Zabbix Server (подставьте IP/DNS AGENT_HOST)
zabbix_get -s <AGENT_HOST_IP> -k eastar.nms.net_usage
zabbix_get -s <AGENT_HOST_IP> -k 'eastar.nms.hub_usage[AM8]'
```

### 5. Хост в Zabbix UI

В интерфейсе Zabbix заведите/используйте хост = **AGENT_HOST** (интерфейс Agent → IP этого хоста), не хост NMS и не обязательно сервер ai_report.

| Item | Type | Key | Type of info | Notes |
|------|------|-----|--------------|-------|
| Eastar net_usage JSON | Zabbix agent | `eastar.nms.net_usage` | Text | Master, 60–120s |
| Eastar hub_usage JSON | Zabbix agent | `eastar.nms.hub_usage[{$EASTAR.FILTER}]` | Text | Master |
| Stations online | Dependent | master = net_usage | Numeric | JSONPath |
| … | Dependent | … | … | … |

Макросы:

| Макрос | Пример | Назначение |
|--------|--------|------------|
| `{$EASTAR.FILTER}` | `AM8` | фильтр контроллеров |
| `{$EASTAR.NET_ID}` | `1` | при пробросе в CLI |

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

`stations_enabled` (`"3 / 5"`) — текстовый item или preprocessing.

#### `hub_usage`: простой вариант / LLD

**A — один контроллер:**

| Метрика | JSONPath |
|---------|----------|
| tx | `$.controllers[0].tx_kbit_s` |
| rx | `$.controllers[0].rx_kbit_s` |
| name | `$.controllers[0].name` |

**B — LLD по `controllers`:**

```text
$.controllers[*]
{#CID}=$.cid
{#NAME}=$.name
```

Item prototypes (Dependent), JSONPath зависит от версии Zabbix, например:

| Name | JSONPath |
|------|----------|
| Tx [{#NAME}] | `$.controllers[?(@.cid=='{#CID}')].tx_kbit_s.first()` |
| Rx [{#NAME}] | `$.controllers[?(@.cid=='{#CID}')].rx_kbit_s.first()` |

### 6. Триггеры (пример)

- `stations_online = 0` длительно → warning  
- `controllers_down > 0` → warning  
- нет данных master-item > 10m → high (скрипт / NMS / ACL / агент)

## Чеклист деплоя (любой AGENT_HOST)

1. [ ] С AGENT_HOST открывается NMS `:443` (`curl -k`)
2. [ ] На AGENT_HOST есть `perl` + LWP/JSON/Cookies
3. [ ] Скопированы `EastarNms.pm`, `eastar_*.pl`
4. [ ] Создан `config.env` (права для пользователя агента)
5. [ ] `perl eastar_net_usage.pl` вручную отдаёт JSON
6. [ ] Настроены UserParameter, агент перезапущен
7. [ ] `zabbix_agentd -t` / `zabbix_get` с Server видят JSON
8. [ ] В Zabbix UI хост указывает на **этот** AGENT_HOST
9. [ ] В `Server=` / `ServerActive=` агента указан ваш Zabbix Server

## Смена хоста агента (переезд)

Если агент переносят с vz3 на другой сервер:

1. На **новом** хосте повторить разделы «Подготовка» → «Деплой» → UserParameter.
2. Проверить NMS `:443` именно с нового хоста (ACL часто привязан к source IP).
3. В Zabbix сменить Agent interface хоста на IP нового AGENT_HOST (или создать новый хост и перенести шаблон).
4. На старом хосте отключить UserParameter / удалить скрипты при необходимости.
5. Не оставлять `config.env` с паролем world-readable.

## Как коллекторы ходят в NMS

| Скрипт | API | Источник метрик |
|--------|-----|-----------------|
| `eastar_net_usage.pl` | `POST /login/insert/` → `POST /update/` | `WidgetNetworkStatus:{net_id}` |
| `eastar_hub_usage.pl` | login → `POST /updatetree/` → `/update/` | контроллеры + `WidgetControllerStatus:{cid}` |

Отдельного REST JSON API у NMS нет: HTML виджетов парсится в Perl.

Тестовый NMS: `https://192.168.10.49` (проверено с vz3).  
Прод из ТЗ: `https://start.steccom.ru` — смените `EASTAR_NMS_URL`, если с AGENT_HOST есть маршрут/DNS.

## Python-заготовки

`eastar_*.py` — stub. На хостах со старым Python (например 3.6) live не использовать. Для Zabbix — **Perl**.

## Устранение проблем

| Симптом | Что проверить |
|---------|----------------|
| `curl` до NMS не проходит | ACL/маршрут **с AGENT_HOST**, не с рабочей станции |
| `Missing EASTAR_NMS_LOGIN` | нет `config.env` / права / путь |
| login failed / timeout | URL, пароль, TLS, proxy |
| пустой `controllers` | `--filter` / `EASTAR_FILTER` |
| `[m\|ZBX_NOTSUPPORTED]` | путь perl/скрипта, `Include=`, restart, SELinux |
| `zabbix_get` timeout | Server → AGENT_HOST `:10050`, firewall, `Server=` в агенте |
| JSON есть, dependent пустые | JSONPath / тип item |

Логи агента: обычно `/var/log/zabbix/zabbix_agentd.log`.
