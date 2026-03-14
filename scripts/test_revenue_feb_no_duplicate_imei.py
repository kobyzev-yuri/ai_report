#!/usr/bin/env python3
"""
Тест: доходы за февраль без задвоения IMEI (фильтр по CLOSE_DATE сервиса).
Запуск из корня: python scripts/test_revenue_feb_no_duplicate_imei.py
На сервере (где есть Oracle): cd /usr/local/projects/ai_report && python3 scripts/test_revenue_feb_no_duplicate_imei.py
Либо выполнить SQL вручную: scripts/test_revenue_feb_no_duplicate_imei.sql
"""
import sys
from pathlib import Path

# Корень проекта
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Загрузка config до импорта db_connection (он сам подхватит utils/config.env)
config_env = ROOT / "config.env"
if not config_env.exists():
    config_env = ROOT / "utils" / "config.env"
if config_env.exists():
    with open(config_env) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip().strip("'\"")
                if k and not __import__("os").environ.get(k):
                    __import__("os").environ[k] = v

from utils.db_connection import get_db_connection

SQL = """
SELECT v.PERIOD_YYYYMM AS "Период", v.CONTRACT_ID, v.IMEI, v.CUSTOMER_NAME, v.CODE_1C,
  v.REVENUE_SBD_TRAFFIC, v.REVENUE_SBD_ABON, v.REVENUE_SBD_TOTAL, v.REVENUE_TOTAL
FROM V_REVENUE_FROM_INVOICES v
JOIN SERVICES s ON v.SERVICE_ID = s.SERVICE_ID
  AND (s.CLOSE_DATE IS NULL OR s.CLOSE_DATE > LAST_DAY(TO_DATE(v.PERIOD_YYYYMM||'-01','YYYY-MM-DD')))
WHERE v.PERIOD_YYYYMM = '2026-02'
ORDER BY v.IMEI, v.CONTRACT_ID
"""

def main():
    conn = get_db_connection()
    if not conn:
        print("Ошибка: не удалось подключиться к Oracle. Проверьте config.env / переменные ORACLE_*")
        return 2
    try:
        cur = conn.cursor()
        cur.execute(SQL)
        rows = cur.fetchall()
        col_names = [c[0] for c in cur.description]
        cur.close()

        # Проверка на дубли IMEI
        imei_counts = {}
        for row in rows:
            r = dict(zip(col_names, row))
            imei = r.get("IMEI") or ""
            imei_counts[imei] = imei_counts.get(imei, 0) + 1

        duplicates = [imei for imei, cnt in imei_counts.items() if cnt > 1]
        print(f"Период: 2026-02. Строк: {len(rows)}")
        if duplicates:
            print(f"ВНИМАНИЕ: найдены задвоенные IMEI: {duplicates[:10]}{'...' if len(duplicates) > 10 else ''} (всего {len(duplicates)})")
        else:
            print("OK: задвоенных IMEI нет, каждый IMEI встречается один раз.")

        # Вывод первых 15 строк
        print("\nПервые 15 строк:")
        print("-" * 100)
        for i, row in enumerate(rows[:15]):
            print(row)
        if len(rows) > 15:
            print(f"... и ещё {len(rows) - 15} строк(и)")
        return 0 if not duplicates else 1
    finally:
        conn.close()

if __name__ == "__main__":
    sys.exit(main())
