#!/usr/bin/env python3
"""Запуск на сервере: проверка VIEW vs старый/новый фильтр Streamlit."""
import os
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
for line in (root / "config.env").read_text().splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        os.environ[k.strip()] = v.strip().strip('"').strip("'")

import oracledb as cx

IMEI = sys.argv[1] if len(sys.argv) > 1 else "300234069207960"
PERIOD = sys.argv[2] if len(sys.argv) > 2 else "2026-02"

user = os.environ["ORACLE_USER"]
pw = os.environ["ORACLE_PASSWORD"]
host = os.environ["ORACLE_HOST"]
port = int(os.environ.get("ORACLE_PORT", "1521"))
svc = os.environ.get("ORACLE_SERVICE") or os.environ.get("ORACLE_SID", "bm7")
dsn = cx.makedsn(host, port, service_name=svc)
conn = cx.connect(user=user, password=pw, dsn=dsn)
cur = conn.cursor()
params = {"i": IMEI, "p": PERIOD}


def run(title, sql):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    print(cols)
    for r in rows:
        print(r)


run(
    "1) VIEW (сырой)",
    """SELECT SERVICE_ID, CONTRACT_ID, IMEI, PERIOD_YYYYMM,
              REVENUE_SUSPEND_ABON, REVENUE_TOTAL
       FROM V_REVENUE_FROM_INVOICES
       WHERE IMEI = :i AND PERIOD_YYYYMM = :p""",
)

run(
    "2) Старый фильтр — COUNT строк",
    """SELECT COUNT(*) AS cnt FROM V_REVENUE_FROM_INVOICES v
       JOIN SERVICES s ON v.SERVICE_ID = s.SERVICE_ID
         AND (s.CLOSE_DATE IS NULL OR s.CLOSE_DATE > LAST_DAY(TO_DATE(v.PERIOD_YYYYMM||'-01','YYYY-MM-DD')))
       WHERE v.IMEI = :i AND v.PERIOD_YYYYMM = :p""",
)

run(
    "3) Новый фильтр — строки",
    """SELECT v.SERVICE_ID, v.CONTRACT_ID, v.IMEI, v.REVENUE_SUSPEND_ABON
       FROM V_REVENUE_FROM_INVOICES v
       LEFT JOIN SERVICES s ON v.SERVICE_ID = s.SERVICE_ID
         AND (s.CLOSE_DATE IS NULL OR s.CLOSE_DATE > LAST_DAY(TO_DATE(v.PERIOD_YYYYMM||'-01','YYYY-MM-DD')))
       WHERE v.IMEI = :i AND v.PERIOD_YYYYMM = :p
         AND (
           s.SERVICE_ID IS NOT NULL
           OR EXISTS (
             SELECT 1 FROM BM_INVOICE_ITEM ii2
             JOIN SERVICES s2 ON ii2.SERVICE_ID = s2.SERVICE_ID
             JOIN BM_PERIOD p ON ii2.PERIOD_ID = p.PERIOD_ID
             WHERE s2.VSAT = v.IMEI
               AND TO_CHAR(p.START_DATE,'YYYY-MM') = v.PERIOD_YYYYMM
               AND (s2.CLOSE_DATE IS NULL OR s2.CLOSE_DATE > LAST_DAY(TO_DATE(v.PERIOD_YYYYMM||'-01','YYYY-MM-DD')))
           )
         )""",
)

conn.close()
print("\nDone.")
