from datetime import datetime
import pytz

from config import Config
from sipuni_api import (
    fetch_operators_csv,
    fetch_calls_csv_export,
    fetch_calls_csv_export_all,
)
from utils_csv import parse_csv


def main():
    cfg = Config()
    tz = pytz.timezone(cfg.tz)
    now = datetime.now(tz)

    # 1) operators
    ops_csv, err = fetch_operators_csv(cfg.sipuni_user, cfg.sipuni_secret)
    if not ops_csv:
        print("operators ERROR:", err)
    else:
        with open("operators.csv", "w", encoding="utf-8") as f:
            f.write(ops_csv)
        h, rows = parse_csv(ops_csv)
        print("operators headers:", h)
        print("operators first row:", rows[0] if rows else "NO ROWS")

    # 2) export
    csv1, err1, st1, raw1 = fetch_calls_csv_export(
        user=cfg.sipuni_user,
        secret=cfg.sipuni_secret,
        day=now,
        time_from="00:00",
        time_to="23:59",
    )
    print("\nEXPORT:")
    print("export HTTP status:", st1)
    print("export err:", err1)
    print("export RAW first 300 chars:\n", (raw1 or "").strip()[:300])

    if csv1:
        with open("calls_export.csv", "w", encoding="utf-8") as f:
            f.write(csv1)
        h, rows = parse_csv(csv1)
        print("calls_export headers:", h)
        print("calls_export first row:", rows[0] if rows else "NO ROWS")

    # 3) export/all
    csv2, err2, st2, raw2 = fetch_calls_csv_export_all(
        user=cfg.sipuni_user,
        secret=cfg.sipuni_secret,
        limit=2000,
        order="desc",
        page=1,
    )
    print("\nEXPORT/ALL:")
    print("export/all HTTP status:", st2)
    print("export/all err:", err2)
    print("export/all RAW first 300 chars:\n", (raw2 or "").strip()[:300])

    if csv2:
        with open("calls_all.csv", "w", encoding="utf-8") as f:
            f.write(csv2)
        h, rows = parse_csv(csv2)
        print("calls_all headers:", h)
        print("calls_all first row:", rows[0] if rows else "NO ROWS")


if __name__ == "__main__":
    main()