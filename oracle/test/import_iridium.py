import argparse
import csv
import logging
import re
import sys
from datetime import datetime
from typing import List, Tuple, Optional

import psycopg2
from psycopg2.extras import execute_batch

EXPECTED_FIELDS = 17
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")
INT32_MAX = 2147483647


def parse_args():
    p = argparse.ArgumentParser(description="Import Iridium TSV into PostgreSQL with validation and logging")
    p.add_argument("--input", required=True, help="Path to V_IRIDIUM_SERVICES_INFO.txt (TSV)")
    p.add_argument("--dsn", required=False, default="host=localhost dbname=billing user=cnn",
                   help='psycopg2 DSN, e.g. "host=localhost dbname=billing user=cnn password=..."')
    p.add_argument("--table", required=False, default="iridium_services_info",
                   help="Target table name (default: iridium_services_info)")
    p.add_argument("--batch", type=int, default=1000, help="Batch size for inserts")
    p.add_argument("--truncate", action="store_true", help="TRUNCATE target table before insert")
    p.add_argument("--log", default="import_iridium.log", help="Path to log file")
    return p.parse_args()


def setup_logging(path: str):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(path, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
    )


def btrim(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s2 = s.strip()
    return s2 if s2 != "" else None


def to_int32(s: Optional[str]) -> Optional[int]:
    s = btrim(s)
    if not s:
        return None
    neg = s.startswith("-")
    num = s[1:] if neg else s
    if not num.isdigit():
        return None
    try:
        val = int(s)
    except Exception:
        return None
    if abs(val) <= INT32_MAX:
        return val
    return None


def to_ts(s: Optional[str]) -> Optional[str]:
    s = btrim(s)
    if not s:
        return None
    if not DATE_RE.match(s):
        return None
    try:
        if len(s) == 10:
            datetime.strptime(s, "%Y-%m-%d")
        else:
            datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return s
    except Exception:
        return None


def validate_and_transform(row: List[str]) -> Tuple[Optional[Tuple], Optional[str]]:
    if len(row) != EXPECTED_FIELDS:
        return None, f"wrong_field_count={len(row)}"

    row = [None if (x.strip() == "") else x.strip() for x in row]

    service_id = to_int32(row[0])
    contract_id = (row[1] or "")[:50] if row[1] else None
    imei = (row[2] or "")[:50] if row[2] else None
    tariff_id = to_int32(row[3])
    agreement_number = btrim(row[4])
    order_number = btrim(row[5])
    status = to_int32(row[6])
    actual_status = to_int32(row[7])
    customer_id = to_int32(row[8])
    organization_name = btrim(row[9])
    person_name = btrim(row[10])
    customer_name = btrim(row[11])
    create_date = to_ts(row[12])
    start_date = to_ts(row[13])
    stop_date = to_ts(row[14])
    account_id = to_int32(row[15])
    code_1c = btrim(row[16])

    if service_id is None:
        return None, "service_id_null_or_invalid"

    return (
        (
            service_id, contract_id, imei, tariff_id, agreement_number, order_number,
            status, actual_status, customer_id, organization_name, person_name,
            customer_name, create_date, start_date, stop_date, account_id, code_1c
        ),
        None,
    )


def main():
    args = parse_args()
    setup_logging(args.log)
    logging.info("Starting import: input=%s table=%s", args.input, args.table)

    try:
        conn = psycopg2.connect(args.dsn)
    except Exception:
        logging.exception("DB connect failed: dsn=%s", args.dsn)
        sys.exit(1)

    conn.autocommit = False
    cur = conn.cursor()

    try:
        if args.truncate:
            logging.info("TRUNCATE %s", args.table)
            cur.execute(f"TRUNCATE TABLE {args.table}")

        insert_sql = f"""
            INSERT INTO {args.table} (
              service_id, contract_id, imei, tariff_id, agreement_number, order_number,
              status, actual_status, customer_id, organization_name, person_name,
              customer_name, create_date, start_date, stop_date, account_id, code_1c
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                      %s::timestamp, %s::timestamp, %s::timestamp, %s, %s)
        """

        good, bad, batch = 0, 0, []
        with open(args.input, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f, delimiter="\t", quotechar='"')
            for ln, row in enumerate(reader, start=1):
                if not row or (len(row) == 1 and (row[0].strip() == "")):
                    continue
                if len(row) != EXPECTED_FIELDS:
                    logging.warning("line=%d invalid field count=%d row=%r", ln, len(row), row)
                    bad += 1
                    continue

                rec, err = validate_and_transform(row)
                if err:
                    logging.warning("line=%d error=%s row=%r", ln, err, row)
                    bad += 1
                    continue

                batch.append(rec)
                if len(batch) >= args.batch:
                    execute_batch(cur, insert_sql, batch, page_size=args.batch)
                    conn.commit()
                    good += len(batch)
                    logging.info("Committed batch: +%d (total good=%d, bad=%d)", len(batch), good, bad)
                    batch.clear()

        if batch:
            execute_batch(cur, insert_sql, batch, page_size=args.batch)
            conn.commit()
            good += len(batch)
            logging.info("Committed final batch: +%d (total good=%d, bad=%d)", len(batch), good, bad)

        logging.info("DONE. Imported good=%d, skipped bad=%d", good, bad)

    except Exception:
        conn.rollback()
        logging.exception("Fatal error, transaction rolled back")
        sys.exit(1)
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()


