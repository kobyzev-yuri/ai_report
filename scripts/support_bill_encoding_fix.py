#!/usr/bin/env python3
"""
Диагностика и перекодировка «кракозябр» в SUPPORT.BILL (bm7).

Два типа порчи в AL32UTF8:
  1) 'ҐааЁв®аЁп' — CP866/CP1251 перепутаны (fix: cp1251→cp866);
  2) 'РџР…РћР›' — UTF-8 байты сохранены как Unicode через CP1251 (fix: cp1251→utf-8).
В SQL*Plus без NLS_LANG=AL32UTF8 оба типа выглядят как кракозябры; нормальный текст (Москва) не трогаем.

Перед UPDATE всегда смотрите --dry-run. Пароль — только из env, не в коде.

Важно: таблица в схеме SUPPORT. Подключайтесь как support (не billing7 из config.env):

  export ORACLE_HOST=...
  export ORACLE_SERVICE=bm7
  export SUPPORT_ORACLE_USER=support
  export SUPPORT_ORACLE_PASSWORD='...'
  python3 scripts/support_bill_encoding_fix.py --dry-run

Опционально: SUPPORT_BILL_SCHEMA=support SUPPORT_BILL_TABLE=bill
"""
from __future__ import annotations

import argparse
import getpass
import os
import re
import sys
from datetime import datetime
from pathlib import Path

TEXT_COLUMNS = (
    "CUSTOMER",
    "SERVICE",
    "PAY_TYPE",
    "DESCRIPTION",
    "LETTER",
    "EXTCOMMENT",
    "CONTRACT_ID",
    "BDOG_EXT1C",
    "BCUST_EXT1C",
)

# При --b-id печатаем основные текстовые поля (включая LETTER)
B_ID_DIAG_COLUMNS = (
    "CUSTOMER",
    "SERVICE",
    "PAY_TYPE",
    "LETTER",
    "DESCRIPTION",
    "EXTCOMMENT",
)

# Символы, которых нет в нормальном русском тексте (типичный mojibake CP1251→UTF-8 в sqlplus).
# НЕ включать обычные буквы в/Ё/« — иначе «Москва» ошибочно «чинится» в кракозябры.
MOJIBAKE_MARKERS = re.compile(r"[ҐЋ®€¤¬]")
MOJIBAKE_MARKERS_SQL = "[ҐЋ®€¤¬]"
# UTF-8, ошибочно разобранный по CP1251 и записанный в VARCHAR2 (Рџ = байты D0 9F «П»).
UTF8_CP1251_MOJIBAKE = re.compile(r"(?:Р.|С.){2,}")
UTF8_CP1251_MOJIBAKE_SQL = "(Р.|С.){2,}"
# Символы-подстановки: UTF-8 continuation byte сохранён как Unicode (› = 0x9B и т.д.)
UTF8_MOJIBAKE_PUNCT_TO_BYTE: dict[str, int] = {
    "\u203a": 0x9B,
    "\u2039": 0x8B,
    "\u2019": 0x99,
    "\u2018": 0x98,
    "\u201c": 0x93,
    "\u201d": 0x94,
    "\u2013": 0x96,
    "\u2014": 0x97,
    "\u2026": 0x85,
    "\u20ac": 0x80,
    "\u2122": 0x99,
}

ROOT = Path(__file__).resolve().parent.parent


def load_config_env() -> None:
    for name in ("config.env", "config.secrets.env"):
        path = ROOT / name
        if not path.exists():
            continue
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip().strip("'\"")
                if k and not os.getenv(k):
                    os.environ[k] = v


def bill_fqn() -> tuple[str, str, str]:
    """(schema, table, schema.table) в верхнем регистре для SQL."""
    schema = (os.getenv("SUPPORT_BILL_SCHEMA") or "SUPPORT").strip().upper()
    table = (os.getenv("SUPPORT_BILL_TABLE") or "BILL").strip().upper()
    return schema, table, f"{schema}.{table}"


def connect(use_config_oracle: bool = False):
    try:
        import oracledb as cx
    except ImportError:
        import cx_Oracle as cx  # noqa: N813

    if use_config_oracle:
        user = os.getenv("ORACLE_USER", "support")
        password = os.getenv("ORACLE_PASSWORD")
    else:
        user = os.getenv("SUPPORT_ORACLE_USER") or os.getenv("ORACLE_USER", "support")
        password = os.getenv("SUPPORT_ORACLE_PASSWORD")
        if not password:
            password = os.getenv("ORACLE_PASSWORD")

    host = os.getenv("ORACLE_HOST", "localhost")
    port = int(os.getenv("ORACLE_PORT", "1521"))
    service = os.getenv("ORACLE_SERVICE") or os.getenv("ORACLE_SID") or "bm7"

    if not password and sys.stdin.isatty():
        password = getpass.getpass(f"Пароль Oracle для {user}: ")

    if not password:
        print(
            "Задайте SUPPORT_ORACLE_PASSWORD (схема support) "
            "или ORACLE_PASSWORD из config.env",
            file=sys.stderr,
        )
        sys.exit(1)

    dsn = cx.makedsn(host, port, service_name=service)
    try:
        conn = cx.connect(user=user, password=password, dsn=dsn)
    except Exception as e:
        err = str(e)
        if "ORA-01017" in err or "01017" in err:
            print(f"ORA-01017: неверный логин/пароль для пользователя {user!r}.", file=sys.stderr)
            print(
                "Проверьте в sqlplus: sqlplus support/<пароль>@bm7\n"
                "Пароль схемы support — не то же самое, что имя пользователя.",
                file=sys.stderr,
            )
        raise
    return conn, user


def ensure_bill_access(cur, schema: str, table: str, fqn: str, connected_user: str) -> None:
    cur.execute("SELECT USER FROM dual")
    session_user = (cur.fetchone()[0] or "").upper()
    cur.execute(
        """
        SELECT privilege
          FROM user_tab_privs
         WHERE owner = :owner AND table_name = :tname
        UNION ALL
        SELECT 'OWNER'
          FROM all_tables
         WHERE owner = :owner AND table_name = :tname
           AND owner = USER
        """,
        {"owner": schema, "tname": table},
    )
    privs = {row[0] for row in cur.fetchall()}
    if privs:
        print(f"Сессия: {session_user} (connect as {connected_user}), таблица {fqn}: {', '.join(sorted(privs))}")
        return

    cur.execute(
        "SELECT owner, table_name FROM all_tables WHERE table_name = :t ORDER BY owner",
        {"t": table},
    )
    owners = cur.fetchall()
    print(f"Ошибка: нет доступа к {fqn} для пользователя {session_user}.", file=sys.stderr)
    if owners:
        print("Таблица BILL найдена у схем:", ", ".join(f"{o}.{t}" for o, t in owners), file=sys.stderr)
    print(
        "Подключитесь как SUPPORT:\n"
        "  export SUPPORT_ORACLE_USER=support\n"
        "  export SUPPORT_ORACLE_PASSWORD='...'\n"
        "  python3 scripts/support_bill_encoding_fix.py --dry-run",
        file=sys.stderr,
    )
    sys.exit(1)


def fix_cp1251_via_cp866(text: str) -> str | None:
    try:
        return text.encode("cp1251").decode("cp866")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return None


def fix_latin1_cp1251(text: str) -> str | None:
    try:
        return text.encode("latin-1").decode("cp1251")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return None


def mojibake_er_count(text: str) -> int:
    """Число «Р» (U+0420) — типичный след UTF-8→CP1251 mojibake."""
    return text.count("\u0420")


def still_looks_like_utf8_mojibake(text: str) -> bool:
    """Испорченный текст: много «Р» относительно кириллицы (не путать с «РУСВЕТ…»)."""
    if not text:
        return False
    er = mojibake_er_count(text)
    if er < 2:
        return False
    cyr = cyrillic_letter_count(text)
    return er * 4 >= cyr


def looks_like_utf8_stored_as_cp1251_mojibake(text: str) -> bool:
    if not text or len(text) < 4:
        return False
    return still_looks_like_utf8_mojibake(text)


def char_to_mojibake_byte(ch: str) -> int | None:
    o = ord(ch)
    if o < 0x80:
        return o
    if o <= 0xFF:
        return o
    if ch in UTF8_MOJIBAKE_PUNCT_TO_BYTE:
        return UTF8_MOJIBAKE_PUNCT_TO_BYTE[ch]
    for enc in ("cp1251", "cp1252"):
        try:
            bb = ch.encode(enc)
        except UnicodeEncodeError:
            continue
        if len(bb) == 1:
            return bb[0]
    return None


def mojibake_unicode_to_utf8_bytes(text: str) -> bytes | None:
    """Восстановить UTF-8 байты из «грязного» mojibake (U+00A0, U+203A, cp1252…)."""
    out = bytearray()
    for c in text:
        b = char_to_mojibake_byte(c)
        if b is None:
            return None
        out.append(b)
    return bytes(out)


def fix_utf8_mojibake_mixed(text: str) -> str | None:
    """UTF-8 mojibake с частично перекодированными байтами (типично CUSTOMER)."""
    if not looks_like_utf8_stored_as_cp1251_mojibake(text):
        return None
    attempts = [text]
    if " " in text:
        attempts.append(text.replace(" ", "\xa0"))
    for candidate in attempts:
        raw = mojibake_unicode_to_utf8_bytes(candidate)
        if not raw:
            continue
        try:
            fixed = raw.decode("utf-8")
        except UnicodeDecodeError:
            continue
        if fixed and fixed != candidate:
            return fixed
    return None


def fix_utf8_mojibake(text: str) -> str | None:
    """UTF-8 байты, прочитанные как CP1251 и сохранённые как Unicode (РџР… → ПР…)."""
    attempts = [text]
    if looks_like_utf8_stored_as_cp1251_mojibake(text) and " " in text:
        # В sqlplus NBSP (0xA0) часто виден как обычный пробел.
        attempts.append(text.replace(" ", "\xa0"))
    for candidate in attempts:
        try:
            fixed = candidate.encode("cp1251").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue
        if fixed and fixed != text:
            return fixed
    return fix_utf8_mojibake_mixed(text)


def cyrillic_letter_count(text: str) -> int:
    return sum(1 for c in text if "\u0400" <= c <= "\u04ff")


def is_normal_cyrillic_text(text: str | None) -> bool:
    """Нормальный UTF-8 русский текст в AL32UTF8 — не трогать."""
    if not text or not text.strip():
        return False
    if MOJIBAKE_MARKERS.search(text):
        return False
    if looks_like_utf8_stored_as_cp1251_mojibake(text):
        return False
    letters = cyrillic_letter_count(text)
    if letters < 2:
        return False
    # «Москва», «аренда», «ГУФСИН по Ростовской обл.» — достаточно кириллицы, без маркеров
    return letters >= max(2, len(text.strip()) // 4)


def pick_fix(text: str) -> tuple[str | None, str]:
    if not text or not text.strip():
        return None, "empty"
    if is_normal_cyrillic_text(text):
        return None, "already_ok"
    utf8_moj = looks_like_utf8_stored_as_cp1251_mojibake(text)
    fix_order = (
        ("cp1251->utf8", fix_utf8_mojibake),
        ("cp1251->cp866", fix_cp1251_via_cp866),
        ("latin1->cp1251", fix_latin1_cp1251),
    )
    if not utf8_moj:
        fix_order = fix_order[1:] + fix_order[:1]
    candidates: list[tuple[str, str]] = []
    for name, fn in fix_order:
        fixed = fn(text)
        if not fixed or fixed == text:
            continue
        if MOJIBAKE_MARKERS.search(fixed):
            continue
        if still_looks_like_utf8_mojibake(fixed):
            continue
        if name != "cp1251->utf8" and cyrillic_letter_count(fixed) < cyrillic_letter_count(text):
            continue
        if cyrillic_letter_count(fixed) < max(2, len(fixed.strip()) // 4):
            continue
        candidates.append((name, fixed))
    if not candidates:
        return None, "no_fix"
    candidates.sort(key=lambda x: cyrillic_letter_count(x[1]), reverse=True)
    return candidates[0][1], candidates[0][0]


def looks_mojibake(text: str | None) -> bool:
    if not text or is_normal_cyrillic_text(text):
        return False
    if MOJIBAKE_MARKERS.search(text):
        return True
    if looks_like_utf8_stored_as_cp1251_mojibake(text):
        return True
    fixed, how = pick_fix(text)
    return fixed is not None and how != "already_ok"


def fetch_nls(cur) -> dict[str, str]:
    cur.execute(
        """
        SELECT parameter, value
          FROM nls_database_parameters
         WHERE parameter IN (
           'NLS_CHARACTERSET', 'NLS_NCHAR_CHARACTERSET', 'NLS_LANGUAGE'
         )
        """
    )
    return {row[0]: row[1] for row in cur.fetchall()}


def sample_hex(cur, fqn: str, col: str, limit: int = 3) -> None:
    cur.execute(
        f"""
        SELECT b_id, {col},
               RAWTOHEX(CAST({col} AS VARCHAR2(4000))) AS hx
          FROM {fqn}
         WHERE {col} IS NOT NULL
           AND ROWNUM <= :lim
        """,
        {"lim": limit},
    )
    print(f"\n--- HEX sample {col} ---")
    for row in cur.fetchall():
        print(row)


def find_candidates(
    cur, fqn: str, limit: int | None, bill_id_null: bool = False
) -> list[dict]:
    col_checks = []
    for c in TEXT_COLUMNS:
        col_checks.append(f"REGEXP_LIKE({c}, '{MOJIBAKE_MARKERS_SQL}')")
        col_checks.append(f"REGEXP_LIKE({c}, '{UTF8_CP1251_MOJIBAKE_SQL}')")
    marker_sql = " OR ".join(col_checks)
    sql = f"""
        SELECT b_id, {", ".join(TEXT_COLUMNS)}
          FROM {fqn}
         WHERE ({marker_sql})
    """
    if bill_id_null:
        sql += " AND bill_id IS NULL"
    if limit:
        sql += f" AND ROWNUM <= {int(limit)}"
    scope = "авансы (bill_id IS NULL)" if bill_id_null else "вся таблица"
    print(f"Поиск кандидатов REGEXP_LIKE ({scope})…")
    cur.execute(sql)
    cols = ["B_ID", *TEXT_COLUMNS]
    rows = [dict(zip(cols, raw)) for raw in cur.fetchall()]
    print(f"Кандидатов по REGEXP: {len(rows)}")
    return rows


def fetch_by_b_id(cur, fqn: str, b_id: int) -> list[dict]:
    cur.execute(
        f"SELECT b_id, {', '.join(TEXT_COLUMNS)} FROM {fqn} WHERE b_id = :id",
        {"id": b_id},
    )
    cols = ["B_ID", *TEXT_COLUMNS]
    return [dict(zip(cols, raw)) for raw in cur.fetchall()]


def scan_all(
    cur, fqn: str, batch: int, limit: int | None, bill_id_null: bool = False
) -> list[dict]:
    sql = f"SELECT b_id, {', '.join(TEXT_COLUMNS)} FROM {fqn}"
    where = []
    if bill_id_null:
        where.append("bill_id IS NULL")
    if limit:
        where.append(f"ROWNUM <= {int(limit)}")
    if where:
        sql += " WHERE " + " AND ".join(where)
    print("Полное сканирование таблицы (без REGEXP в SQL, может занять время)…")
    cur.execute(sql)
    cols = ["B_ID", *TEXT_COLUMNS]
    out = []
    scanned = 0
    while True:
        chunk = cur.fetchmany(batch)
        if not chunk:
            break
        for raw in chunk:
            scanned += 1
            row = dict(zip(cols, raw))
            if any(looks_mojibake(row.get(c)) for c in TEXT_COLUMNS):
                out.append(row)
        if scanned % 5000 == 0:
            print(f"  просмотрено {scanned} записей, кандидатов {len(out)}…")
    print(f"Сканирование завершено: {scanned} записей, кандидатов {len(out)}")
    return out


def print_report(
    rows: list[dict], max_show: int, report_file: str | None = None
) -> list[dict]:
    changes: list[dict] = []
    shown = 0
    show_limit = max_show if max_show > 0 else None
    report_lines: list[str] = []

    def emit(line: str = "", *, to_console: bool = True) -> None:
        report_lines.append(line)
        if to_console and (show_limit is None or shown < show_limit or line.startswith("Всего")):
            print(line)

    for row in rows:
        b_id = row["B_ID"]
        row_changes = {}
        for col in TEXT_COLUMNS:
            val = row.get(col)
            if not looks_mojibake(val):
                continue
            fixed, how = pick_fix(val or "")
            if fixed and fixed != val:
                row_changes[col] = (val, fixed, how)
        if not row_changes:
            continue
        changes.append({"B_ID": b_id, "cols": row_changes})
        if show_limit is not None and shown >= show_limit:
            continue
        shown += 1
        emit(f"\nB_ID={b_id}")
        for col, (old, new, how) in row_changes.items():
            emit(f"  {col} [{how}]")
            emit(f"    было: {old!r}")
            emit(f"    стало: {new!r}")

    nfields = sum(len(c["cols"]) for c in changes)
    emit(f"\nВсего записей bill к правке: {len(changes)}")
    emit(f"Всего полей к правке: {nfields}")

    if report_file and changes:
        path = Path(report_file)
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(report_lines))
            if not report_lines[-1].endswith("\n"):
                f.write("\n")
        print(f"\nПолный отчёт записан: {path.resolve()}")

    return changes


def apply_updates(cur, fqn: str, changes: list[dict], dry_run: bool) -> int:
    n = 0
    for item in changes:
        b_id = item["B_ID"]
        for col, (_, new, _) in item["cols"].items():
            n += 1
            if dry_run:
                continue
            cur.execute(
                f"UPDATE {fqn} SET {col} = :v WHERE b_id = :id",
                {"v": new, "id": b_id},
            )
    return n


def backup_table(cur, schema: str, fqn: str) -> str:
    suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = f"BILL_ENC_BAK_{suffix}"
    cur.execute(f"CREATE TABLE {schema}.{bak} AS SELECT * FROM {fqn}")
    print(f"Резервная копия: {schema}.{bak}")
    return bak


def main() -> None:
    load_config_env()
    schema, table, fqn = bill_fqn()

    ap = argparse.ArgumentParser(description="Перекодировка SUPPORT.BILL")
    ap.add_argument("--dry-run", action="store_true", help="Только отчёт (по умолчанию)")
    ap.add_argument("--apply", action="store_true", help="Выполнить UPDATE")
    ap.add_argument("--backup", action="store_true", help="CREATE TABLE AS SELECT перед UPDATE")
    ap.add_argument("--hex-sample", action="store_true", help="Показать RAWTOHEX по образцам")
    ap.add_argument("--fast-regexp", action="store_true", help="Только REGEXP_LIKE (быстро)")
    ap.add_argument("--limit", type=int, default=None, help="Ограничить число строк")
    ap.add_argument("--b-id", type=int, default=None, help="Проверить одну строку bill.b_id")
    ap.add_argument(
        "--bill-id-null",
        action="store_true",
        help="Только авансовые счета (bill_id IS NULL); без флага — вся таблица",
    )
    ap.add_argument(
        "--max-show",
        type=int,
        default=30,
        help="Сколько примеров вывести на экран (0 = все)",
    )
    ap.add_argument(
        "--report-file",
        default=None,
        help="Записать полный отчёт dry-run в файл (удобно для всей таблицы)",
    )
    ap.add_argument("--schema", default=None, help="Схема таблицы (по умолчанию SUPPORT)")
    ap.add_argument(
        "--config-oracle",
        action="store_true",
        help="ORACLE_USER/ORACLE_PASSWORD из config.env (нужен GRANT SELECT ON support.bill)",
    )
    args = ap.parse_args()
    if args.schema:
        schema = args.schema.strip().upper()
        fqn = f"{schema}.{table}"
        os.environ["SUPPORT_BILL_SCHEMA"] = schema

    dry_run = not args.apply

    conn, connect_user = connect(use_config_oracle=args.config_oracle)
    cur = conn.cursor()
    try:
        ensure_bill_access(cur, schema, table, fqn, connect_user)
        nls = fetch_nls(cur)
        print("NLS:", nls)
        print(f"Таблица: {fqn}")

        if args.hex_sample:
            for c in ("CUSTOMER", "LETTER", "PAY_TYPE"):
                sample_hex(cur, fqn, c)

        if args.b_id is not None:
            rows = fetch_by_b_id(cur, fqn, args.b_id)
            if not rows:
                print(f"Строка b_id={args.b_id} не найдена")
                return
            for col in B_ID_DIAG_COLUMNS:
                val = rows[0].get(col)
                if val is None:
                    print(f"b_id={args.b_id} {col}: NULL")
                    continue
                hx = val.encode("utf-8").hex().upper()
                moj = looks_mojibake(val)
                fixed, how = pick_fix(val)
                hint = ""
                if fixed and fixed != val:
                    hint = f"  -> {fixed!r} [{how}]"
                elif moj:
                    hint = "  (mojibake, авто-правка не подобрана)"
                print(f"b_id={args.b_id} {col}: {val!r}  utf8_hex={hx}{hint}")
        elif args.fast_regexp:
            rows = find_candidates(cur, fqn, args.limit, bill_id_null=args.bill_id_null)
        else:
            rows = scan_all(
                cur, fqn, batch=500, limit=args.limit, bill_id_null=args.bill_id_null
            )

        changes = print_report(rows, args.max_show, report_file=args.report_file)
        if not changes:
            print(
                "\nСтрок для перекодировки не найдено — это нормально, если в Python/oracledb "
                "текст уже читается как кириллица (AL32UTF8, HEX D0/D1…).\n"
                "Кракозябры только в SQL*Plus — настройте клиент, данные не UPDATE:\n"
                "  export NLS_LANG=AMERICAN_AMERICA.AL32UTF8   # или RUSSIAN_CIS.CL8MSWIN1251 под ваш терминал\n"
                "  sqlplus support/...@bm7\n"
                "Просмотр без правки: SELECT CONVERT(customer,'UTF8','AL32UTF8') ... — при AL32UTF8 обычно не нужен."
            )
            return

        if args.apply and args.backup:
            backup_table(cur, schema, fqn)

        ncol = apply_updates(cur, fqn, changes, dry_run=dry_run)
        if dry_run:
            print(f"\n[dry-run] Будет обновлено полей: {ncol}. Запустите с --apply --backup")
        else:
            conn.commit()
            print(f"Обновлено полей: {ncol}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
