#!/usr/bin/env python3
"""
Диагностика: почему IMEI не попадает в V_REVENUE_FROM_INVOICES за период.
Запуск на сервере: cd /usr/local/projects/ai_report && source config.env 2>/dev/null; python3 scripts/diagnose_revenue_imei.py 300234069207960 2026-02
"""
import os
import sys
from pathlib import Path

# Загрузка config.env из корня проекта
root = Path(__file__).resolve().parent.parent
config_file = root / "config.env"
if config_file.exists():
    with open(config_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ[k.strip()] = v.strip().strip("'\"")

try:
    import oracledb as cx_Oracle
except ImportError:
    try:
        import cx_Oracle
    except ImportError:
        print("ERROR: oracledb or cx_Oracle not installed")
        sys.exit(1)

IMEI = sys.argv[1] if len(sys.argv) > 1 else "300234069207960"
PERIOD_YYYYMM = sys.argv[2] if len(sys.argv) > 2 else "2026-02"

cfg = {
    "username": os.getenv("ORACLE_USER"),
    "password": os.getenv("ORACLE_PASSWORD"),
    "host": os.getenv("ORACLE_HOST"),
    "port": int(os.getenv("ORACLE_PORT", "1521")),
    "sid": os.getenv("ORACLE_SID"),
    "service_name": os.getenv("ORACLE_SERVICE") or os.getenv("ORACLE_SID"),
}
if not cfg["username"] or not cfg["password"] or not cfg["host"]:
    print("ERROR: set ORACLE_USER, ORACLE_PASSWORD, ORACLE_HOST (or config.env)")
    sys.exit(1)

if cfg.get("sid"):
    dsn = cx_Oracle.makedsn(cfg["host"], cfg["port"], sid=cfg["sid"])
else:
    dsn = cx_Oracle.makedsn(cfg["host"], cfg["port"], service_name=cfg["service_name"])

conn = cx_Oracle.connect(user=cfg["username"], password=cfg["password"], dsn=dsn)

def run(q, title, params=None):
    if params is None:
        params = {"imei": IMEI, "period": PERIOD_YYYYMM}
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)
    cur = conn.cursor()
    cur.execute(q, params)
    cols = [c[0] for c in cur.description]
    rows = cur.fetchall()
    print("Columns:", cols)
    for r in rows[:20]:
        print(r)
    if len(rows) > 20:
        print("... (%d rows total)" % len(rows))
    cur.close()

# 1) Есть ли строка в VIEW за период
run(
    """SELECT SERVICE_ID, CONTRACT_ID, IMEI, ORGANIZATION_NAME,
              TO_CHAR(TO_DATE(TO_CHAR(BILL_MONTH), 'YYYYMM'), 'YYYY-MM') AS PERIOD_YYYYMM,
              REVENUE_SBD_ABON, REVENUE_SUSPEND_ABON,
              REVENUE_MONITORING_ABON, REVENUE_MSG_ABON, REVENUE_TOTAL
       FROM V_REVENUE_FROM_INVOICES
       WHERE IMEI = :imei
         AND TO_CHAR(TO_DATE(TO_CHAR(BILL_MONTH), 'YYYYMM'), 'YYYY-MM') = :period""",
    "1. V_REVENUE_FROM_INVOICES для IMEI=" + IMEI + " PERIOD=" + PERIOD_YYYYMM,
)

# 2) Услуги по этому IMEI (VSAT)
run(
    """SELECT SERVICE_ID, TYPE_ID, LOGIN, VSAT, ACCOUNT_ID, OPEN_DATE, CLOSE_DATE
       FROM SERVICES
       WHERE VSAT = :imei AND TYPE_ID IN (9002, 9004, 9005, 9008, 9010, 9013, 9014)
       ORDER BY TYPE_ID""",
    "2. SERVICES (VSAT=IMEI) TYPE_ID 9002,9004,9005,9008,9010,9013,9014",
    {"imei": IMEI},
)

# 3) PERIOD_ID для 2026-02
run(
    """SELECT PERIOD_ID, TO_CHAR(START_DATE,'YYYY-MM') AS YYYY_MM, START_DATE, STOP_DATE
       FROM BM_PERIOD
       WHERE TO_CHAR(START_DATE,'YYYY-MM') = :period""",
    "3. BM_PERIOD для периода " + PERIOD_YYYYMM,
    {"period": PERIOD_YYYYMM},
)

# 4) Строки BM_INVOICE_ITEM за этот период по сервисам с этим IMEI
run(
    """SELECT ii.INVOICE_ITEM_ID, ii.SERVICE_ID, ii.PERIOD_ID, ii.MONEY, s.TYPE_ID, s.VSAT, s.ACCOUNT_ID, s.LOGIN
       FROM BM_INVOICE_ITEM ii
       JOIN SERVICES s ON ii.SERVICE_ID = s.SERVICE_ID
       WHERE s.VSAT = :imei
         AND s.TYPE_ID IN (9002, 9004, 9005, 9008, 9010, 9013, 9014)
         AND ii.PERIOD_ID = (SELECT PERIOD_ID FROM BM_PERIOD WHERE TO_CHAR(START_DATE,'YYYY-MM') = :period)""",
    "4. BM_INVOICE_ITEM за период по сервисам с VSAT=IMEI",
)

# 5) Есть ли 9002/9014 для того же (VSAT, ACCOUNT_ID) что и 9008 (влияет на main_sbd_services)
run(
    """SELECT s.SERVICE_ID, s.TYPE_ID, s.ACCOUNT_ID, s.VSAT,
              (SELECT COUNT(*) FROM SERVICES m WHERE m.TYPE_ID IN (9002,9014) AND m.VSAT = s.VSAT AND m.ACCOUNT_ID = s.ACCOUNT_ID) AS cnt_main
       FROM SERVICES s
       WHERE s.VSAT = :imei AND s.TYPE_ID = 9008""",
    "5. Для каждой 9008: количество 9002/9014 с тем же VSAT+ACCOUNT_ID",
    {"imei": IMEI},
)

conn.close()
print("\nDone.")
