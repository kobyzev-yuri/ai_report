#!/usr/bin/env python3
"""
Минимальный веб-интерфейс для голосового диалога с биллинг- и спутниковым ассистентами.
Логин/пароль — из портала (users.db). Удобная транскрипция голоса и диалог с уточнениями.

Запуск из корня проекта (чтобы подтянуть config.env и users.db):
  python -m voice_chat.app
  или
  cd /path/to/ai_report && python voice_chat/app.py

Порт по умолчанию: 5001 (чтобы не конфликтовать со Streamlit).
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

# config.env
config_env = ROOT / "config.env"
if config_env.exists():
    from dotenv import load_dotenv
    load_dotenv(config_env)

from flask import Flask, request, jsonify, session, redirect, url_for, render_template
from functools import wraps

app = Flask(__name__, template_folder=Path(__file__).parent / "templates", static_folder=Path(__file__).parent / "static")
app.secret_key = os.getenv("VOICE_CHAT_SECRET_KEY", "voice-chat-dev-secret-change-in-production")
app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024  # 25 MB для аудио

# Под префиксом (напр. /voice): nginx передаёт X-Script-Name: /voice и proxy_pass .../voice/ -> .../
# Тогда все url_for() дадут /voice/login, /voice/, /voice/api/...
SCRIPT_NAME = (os.getenv("VOICE_CHAT_SCRIPT_NAME") or "").strip().rstrip("/")
if SCRIPT_NAME and not SCRIPT_NAME.startswith("/"):
    SCRIPT_NAME = "/" + SCRIPT_NAME


class ScriptNameMiddleware:
    """Выставляет SCRIPT_NAME из заголовка X-Script-Name до создания Flask request (чтобы url_for давал правильный префикс)."""
    def __init__(self, app, script_name_header="X-Script-Name", fallback_script_name=""):
        self.app = app
        self.script_name_header = "HTTP_" + script_name_header.upper().replace("-", "_")
        self.fallback = (fallback_script_name or SCRIPT_NAME or "").strip().rstrip("/")
        if self.fallback and not self.fallback.startswith("/"):
            self.fallback = "/" + self.fallback

    def __call__(self, environ, start_response):
        prefix = environ.get(self.script_name_header, "").strip().rstrip("/") or self.fallback
        if prefix and not prefix.startswith("/"):
            prefix = "/" + prefix
        if prefix:
            environ["SCRIPT_NAME"] = prefix
        return self.app(environ, start_response)


app.wsgi_app = ScriptNameMiddleware(app.wsgi_app, fallback_script_name=SCRIPT_NAME)

# --- Auth (портал users.db) ---
def login_required(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if not session.get("username"):
            if request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.is_json:
                return jsonify({"error": "Требуется авторизация"}), 401
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapped


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return render_template("login.html")
    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""
    if not username or not password:
        return render_template("login.html", error="Введите логин и пароль")
    try:
        from utils.auth_db_v2 import authenticate_user
        ok, user, is_super, tabs = authenticate_user(username, password)
    except Exception as e:
        return render_template("login.html", error=f"Ошибка проверки: {e}")
    if not ok:
        return render_template("login.html", error="Неверный логин или пароль")
    session["username"] = user
    session["is_superuser"] = is_super
    session["allowed_tabs"] = tabs or []
    return redirect(url_for("chat"))


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@login_required
def chat():
    script_name = request.environ.get("SCRIPT_NAME", "") or SCRIPT_NAME
    return render_template("chat.html", username=session.get("username"), script_name=script_name)


# --- API: выполнение SQL и выгрузка в Excel ---
def _get_oracle_conn():
    """Подключение к Oracle (как в streamlit_assistant)."""
    try:
        import cx_Oracle
        u = os.getenv("ORACLE_USER")
        p = os.getenv("ORACLE_PASSWORD")
        h = os.getenv("ORACLE_HOST")
        port = int(os.getenv("ORACLE_PORT", "1521"))
        sid = os.getenv("ORACLE_SID")
        svc = os.getenv("ORACLE_SERVICE") or sid
        if not all([u, p, h]):
            return None
        if sid:
            dsn = cx_Oracle.makedsn(h, port, sid=sid)
        else:
            dsn = cx_Oracle.makedsn(h, port, service_name=svc)
        return cx_Oracle.connect(user=u, password=p, dsn=dsn)
    except Exception:
        return None


@app.route("/api/execute_sql", methods=["POST"])
@login_required
def api_execute_sql():
    data = request.get_json() or {}
    sql = (data.get("sql") or "").strip().rstrip(";").strip()
    if not sql:
        return jsonify({"error": "Нет SQL"}), 400
    sql_upper = sql.upper()
    if not sql_upper.startswith("SELECT") and not sql_upper.startswith("WITH"):
        return jsonify({"error": "Разрешены только SELECT-запросы"}), 400
    conn = _get_oracle_conn()
    if not conn:
        return jsonify({"error": "Не удалось подключиться к Oracle. Проверьте config.env."}), 503
    try:
        import pandas as pd
        df = pd.read_sql(sql, conn)
        conn.close()
        columns = list(df.columns)
        rows = df.fillna("").astype(str).values.tolist()
        session["last_sql_result"] = {"columns": columns, "rows": rows}
        return jsonify({"columns": columns, "rows": rows})
    except Exception as e:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        return jsonify({"error": str(e)}), 500


@app.route("/api/export_excel")
@login_required
def api_export_excel():
    data = session.get("last_sql_result")
    if not data or not data.get("rows"):
        return jsonify({"error": "Нет данных для выгрузки. Сначала выполните SQL."}), 400
    try:
        import pandas as pd
        import io
        df = pd.DataFrame(data["rows"], columns=data.get("columns") or [])
        buf = io.BytesIO()
        df.to_excel(buf, index=False, engine="openpyxl")
        buf.seek(0)
        from flask import send_file
        return send_file(
            buf,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name="result.xlsx",
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --- API: ответ ассистента (диалог с историей) ---
@app.route("/api/chat", methods=["POST"])
@login_required
def api_chat():
    data = request.get_json() or {}
    assistant_type = (data.get("assistant") or "billing").strip().lower()
    if assistant_type not in ("billing", "satellite"):
        return jsonify({"error": "assistant должен быть billing или satellite"}), 400
    messages = data.get("messages") or []
    if not isinstance(messages, list):
        return jsonify({"error": "messages должен быть массивом {role, content}"}), 400
    # Последнее сообщение пользователя
    user_text = ""
    for m in reversed(messages):
        if (m.get("role") or "").strip().lower() == "user":
            user_text = (m.get("content") or "").strip()
            break
    if not user_text:
        return jsonify({"error": "Нет сообщения пользователя"}), 400

    try:
        if assistant_type == "satellite":
            reply = _satellite_reply(user_text, messages)
        else:
            reply = _billing_reply(user_text, messages)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    return jsonify({"reply": reply or "Пустой ответ."})


def _satellite_reply(user_message: str, messages: list) -> str:
    """Спутниковый библиотекарь: RAG + Gemini, с историей диалога."""
    from kb_billing.rag.rag_assistant import RAGAssistant
    from kb_billing.rag.config_sql4a import SQL4AConfig
    from kb_billing.rag.satellite_librarian_agent import SatelliteLibrarianAgent, ENGINEER_PROMPT

    qdrant_host = os.getenv("QDRANT_HOST", SQL4AConfig.QDRANT_HOST)
    qdrant_port = int(os.getenv("QDRANT_PORT", SQL4AConfig.QDRANT_PORT))
    rag = RAGAssistant(qdrant_host=qdrant_host, qdrant_port=qdrant_port)
    agent = SatelliteLibrarianAgent(rag_assistant=rag, system_prompt=ENGINEER_PROMPT)
    history = []
    for m in messages:
        role = (m.get("role") or "").strip().lower()
        content = (m.get("content") or "").strip()
        if not content:
            continue
        if role == "user":
            history.append(("user", content))
        elif role in ("assistant", "model"):
            history.append(("model", content))
    # Последнее сообщение уже user_message; не дублируем
    if history and history[-1][0] == "user" and history[-1][1] == user_message:
        history = history[:-1]
    return agent.ask(user_message, use_rag=True, history=history if history else None)


def _billing_reply(user_message: str, messages: list) -> str:
    """Биллинг-ассистент: диалог с уточнениями или генерация SQL. Контекст из KB + история."""
    from openai import OpenAI
    from kb_billing.rag.rag_assistant import RAGAssistant
    from kb_billing.rag.config_sql4a import SQL4AConfig

    api_key = os.getenv("OPENAI_API_KEY")
    api_base = os.getenv("OPENAI_BASE_URL") or os.getenv("OPENAI_API_BASE")
    if not api_key:
        return "Задайте OPENAI_API_KEY в config.env для диалога с биллинг-ассистентом."
    client_kw = {"api_key": api_key}
    if api_base:
        client_kw["base_url"] = api_base
    client = OpenAI(**client_kw)
    model = os.getenv("OPENAI_MODEL", "gpt-3.5-turbo")

    qdrant_host = os.getenv("QDRANT_HOST", SQL4AConfig.QDRANT_HOST)
    qdrant_port = int(os.getenv("QDRANT_PORT", SQL4AConfig.QDRANT_PORT))
    rag = RAGAssistant(qdrant_host=qdrant_host, qdrant_port=qdrant_port)
    context = rag.get_context_for_sql_generation(user_message, max_examples=5)
    formatted = rag.format_context_for_llm(context) if context else ""

    system = """Ты — биллинг-ассистент для отчётов по биллингу (Oracle, Iridium M2M). У тебя есть контекст из базы знаний (примеры запросов и структура таблиц).

Твоя задача в диалоге:
1. Если запрос директора/пользователя однозначен — кратко подтверди и при необходимости предложи готовый SQL (одним блоком) или скажи, что запрос выполнен.
2. Если запрос неоднозначен (нет периода, непонятно «топ по чему», какой срез и т.д.) — задай 1–3 коротких уточняющих вопроса по-русски, без SQL.
3. Отвечай кратко, по делу. SQL выводи только когда запрос полностью ясен."""

    user_block = "Контекст из базы знаний (примеры и таблицы):\n\n" + (formatted or "Нет контекста.") + "\n\nДиалог:\n"
    for m in messages:
        r = (m.get("role") or "").strip().lower()
        c = (m.get("content") or "").strip()
        if not c:
            continue
        user_block += ("Пользователь" if r == "user" else "Ассистент") + ": " + c[:2000] + "\n"
    user_block += "\nОтветь на последнее сообщение пользователя: уточняющие вопросы или подтверждение и SQL."

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user_block},
        ],
        temperature=0.2,
        max_tokens=2000,
    )
    text = (resp.choices[0].message.content or "").strip()
    return text or "Пустой ответ."


if __name__ == "__main__":
    port = int(os.getenv("VOICE_CHAT_PORT", "5001"))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)
