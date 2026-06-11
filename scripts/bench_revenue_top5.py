#!/usr/bin/env python3
"""Сравнение скорости top-5 IMEI: view vs BM_INVOICE_ITEM."""
import os
import sys
import time
from pathlib import Path

root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))
config_file = root / "config.env"
if config_file.exists():
    with open(config_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ[k.strip()] = v.strip().strip("'\"")

from utils.db_connection import get_db_connection

PERIOD = sys.argv[1] if len(sys.argv) > 1 else "2026-05"

VIEW_SQL = f"""
SELECT IMEI, REVENUE_TOTAL AS rev
FROM V_REVENUE_FROM_INVOICES
WHERE PERIOD_YYYYMM = '{PERIOD}' AND IMEI IS NOT NULL
ORDER BY REVENUE_TOTAL DESC
FETCH FIRST 5 ROWS ONLY
"""

FAST_SQL = f"""
SELECT imei, customer_name, rev FROM (
  SELECT TRIM(TO_CHAR(s.VSAT)) AS imei,
         MAX(vi.CUSTOMER_NAME) AS customer_name,
         SUM(ii.MONEY - NVL(ii.MONEY_REVERSED, 0)) AS rev
  FROM BM_INVOICE_ITEM ii
  JOIN BM_PERIOD p ON ii.PERIOD_ID = p.PERIOD_ID
  JOIN SERVICES s ON ii.SERVICE_ID = s.SERVICE_ID
  LEFT JOIN V_IRIDIUM_SERVICES_INFO vi
    ON vi.ACCOUNT_ID = s.ACCOUNT_ID
   AND TRIM(TO_CHAR(vi.IMEI)) = TRIM(TO_CHAR(s.VSAT))
  WHERE TO_CHAR(p.START_DATE, 'YYYY-MM') = '{PERIOD}'
    AND s.TYPE_ID IN (9002, 9004, 9005, 9008, 9010, 9013, 9014)
    AND s.VSAT IS NOT NULL
  GROUP BY TRIM(TO_CHAR(s.VSAT))
  ORDER BY rev DESC
  FETCH FIRST 5 ROWS ONLY
)
"""


def run(label, sql, limit_sec=90):
    conn = get_db_connection()
    cur = conn.cursor()
    t0 = time.time()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        print(f"{label}: {time.time() - t0:.1f}s, rows={len(rows)}")
        for r in rows:
            print(" ", r)
    except Exception as e:
        print(f"{label}: {time.time() - t0:.1f}s ERR {e}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    print("Period:", PERIOD)
    run("FAST (BM_INVOICE_ITEM)", FAST_SQL)
    run("VIEW (V_REVENUE_FROM_INVOICES)", VIEW_SQL, limit_sec=300)
