# Голосовой диалог с ассистентами (тестовый подпроект)

Минимальный веб-интерфейс для диалога с **биллинг-** и **спутниковым** ассистентами с поддержкой голосового ввода. Логин и пароль — те же, что у основного портала (users.db).

## Зачем

- На Streamlit голосовой ввод оказался неудобным — здесь отдельное простое приложение.
- **Диалог с уточнениями**: директор может сформулировать запрос неточно; ассистент задаёт короткие уточняющие вопросы (для спутникового это уже есть, для биллинга добавлено в этом интерфейсе).
- Удобная транскрибация: нажал «Записать», сказал — текст подставляется в поле, можно отредактировать и отправить.

## Запуск

Из **корня проекта** (чтобы подтягивались config.env и users.db).

### Автоматизация (как для Streamlit)

В корне проекта есть скрипты для фонового запуска и перезапуска:

```bash
./run_voice_chat_background.sh   # Запуск в фоне (порт 5001, логи: voice_chat_5001.log)
./stop_voice_chat.sh             # Остановка
./restart_voice_chat.sh          # Перезапуск
./status_voice_chat.sh           # Статус и PID
```

Используется тот же venv и config.env, что и для Streamlit. PID сохраняется в `voice_chat_5001.pid` (или `voice_chat_<PORT>.pid` при `VOICE_CHAT_PORT` в config.env).

### Запуск вручную

```bash
pip install flask python-dotenv   # при необходимости
python -m voice_chat.app
# или
python voice_chat/app.py
```

Открой в браузере: http://localhost:5001

Переменные окружения (или config.env):

- `VOICE_CHAT_PORT` — порт (по умолчанию 5001).
- `VOICE_CHAT_SECRET_KEY` — секрет для сессий (в продакшене задать свой).
- Для транскрибации и ассистентов: `OPENAI_API_KEY`, `OPENAI_API_BASE` (proxyapi.ru), для спутникового — `GEMINI_API_KEY` / ProxyAPI, Qdrant и т.д. (как в основном приложении).
- Для голоса: на сервере должен быть установлен **ffmpeg** (конвертация webm → wav для Whisper API): `apt install ffmpeg` или `yum install ffmpeg`.

## Что есть

1. **Вход** — логин/пароль из портала (utils.auth_db_v2, users.db).
2. **Выбор ассистента** — переключатель: Биллинг / Спутниковый.
3. **Диалог** — история сообщений, поле ввода, кнопка «Отправить».
4. **Голос** — кнопка «Записать»: запись с микрофона (MediaRecorder) → отправка на `/api/transcribe` (Whisper через proxyapi.ru) → текст подставляется в поле.

## API

- `POST /api/transcribe` — форма с полем `audio` (файл) или body с аудио. Ответ: `{ "text": "…" }` или `{ "error": "…" }`.
- `POST /api/chat` — JSON `{ "assistant": "billing"|"satellite", "messages": [ { "role": "user"|"assistant", "content": "…" } ] }`. Ответ: `{ "reply": "…" }`.

## Деплой на сервер

Скопировать `voice_chat/` на сервер вместе с проектом. Запуск через systemd или вручную:

```bash
cd /usr/local/projects/ai_report
python3 -m voice_chat.app
```

Или с указанием порта и секрета:

```bash
VOICE_CHAT_PORT=5001 VOICE_CHAT_SECRET_KEY=your-secret python3 -m voice_chat.app
```

Рекомендуется проксировать через nginx (HTTPS) и при необходимости ограничить доступ по IP.

### Nginx: работа под префиксом (на одном порту со Streamlit)

На одном порту (например 7776) обычно уже висит Streamlit. Голосовой чат лучше отдать под отдельный путь, например **/voice/**.

В блок `server { listen 7776; server_name stat.steccom.ru; ... }` добавьте:

```nginx
# Голосовой диалог (Flask на 5001) — все пути /voice/* идут в приложение
location /voice/ {
    proxy_pass http://127.0.0.1:5001/;
    proxy_redirect off;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header X-Script-Name /voice;
}
```

Открывать в браузере: **http://stat.steccom.ru:7776/voice/** (слэш в конце можно). После входа редиректы и ссылки будут вида `/voice/`, `/voice/login`, `/voice/api/...` — все запросы пойдут в Flask.

**Почему не только `/login/`:** после входа приложение редиректит на главную страницу чата (`/`). Если проксировать только `/login/`, то запрос к `/` уйдёт в другой backend (Streamlit), и получится ошибка или пустая страница. Поэтому нужен общий префикс `/voice/` для всего приложения.
