import os
import json
import requests
from datetime import datetime, date, timedelta, timezone
import uuid
import psycopg2

# -----------------------------
# Config
# -----------------------------
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "./raw_data")
TIMEFRAME_MINUTES = int(os.getenv("TIMEFRAME_MINUTES", "5"))
TOLERANCE_BARS = int(os.getenv("TOLERANCE_BARS", "2"))

# DB
DB_HOST = os.getenv("DB_HOST", "localhost")          # docker: quant_db
DB_PORT = int(os.getenv("DB_PORT", "5432"))          # local docker mapping often 5433
DB_NAME = os.getenv("DB_NAME", "quant_data_db")
DB_USER = os.getenv("DB_USER", "mashooqrabbanishaik")
DB_PASS = os.getenv("DB_PASS", "quant@300")
DATA_TABLE = os.getenv("DB_TABLE", "intraday_data")

# DQ table name
DQ_TABLE = os.getenv("DQ_TABLE", "dq_intraday_calendar")

# Alpaca (paper is fine)
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

def _alpaca_headers():
    key = os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_API_KEY")
    secret = os.getenv("APCA_API_SECRET_KEY") or os.getenv("ALPACA_API_SECRET")
    if not key or not secret:
        raise ValueError(
            "Missing Alpaca credentials. Set APCA_API_KEY_ID & APCA_API_SECRET_KEY "
            "(or ALPACA_API_KEY & ALPACA_API_SECRET)."
        )
    return {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def parse_hhmm(hhmm: str) -> int:
    t = datetime.strptime(hhmm, "%H:%M")
    return t.hour * 60 + t.minute


def expected_bars(open_hhmm: str, close_hhmm: str, tf_min: int) -> int:
    o = parse_hhmm(open_hhmm)
    c = parse_hhmm(close_hhmm)
    minutes = c - o
    if minutes <= 0:
        return 0
    return minutes // tf_min


def fetch_alpaca_calendar(start: date, end: date) -> list[dict]:
    ensure_dir(RAW_DATA_DIR)
    cache_path = os.path.join(RAW_DATA_DIR, f"alpaca_calendar_{start.isoformat()}_{end.isoformat()}.json")

    if os.path.exists(cache_path):
        with open(cache_path, "r") as f:
            return json.load(f)

    url = f"{ALPACA_BASE_URL}/v2/calendar"
    params = {"start": start.isoformat(), "end": end.isoformat()}
    r = requests.get(url, params=params, headers=_alpaca_headers(), timeout=30)
    r.raise_for_status()
    cal = r.json()

    with open(cache_path, "w") as f:
        json.dump(cal, f, indent=2)

    return cal


def db_connect():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )


def get_db_date_range(conn) -> tuple[date, date]:
    with conn.cursor() as cur:
        cur.execute(f"select min(timestamp)::date, max(timestamp)::date from {DATA_TABLE};")
        row = cur.fetchone()
    if not row or not row[0] or not row[1]:
        raise RuntimeError("intraday_data has no timestamps to derive date range.")
    return row[0], row[1]


def get_daily_counts(conn) -> list[tuple[date, str, int]]:
    with conn.cursor() as cur:
        cur.execute(f"""
            select timestamp::date as d, symbol, count(*) as bars
            from {DATA_TABLE}
            group by 1,2
            order by 1,2;
        """)
        return cur.fetchall()


def ensure_dq_table(conn):
    ddl = f"""
    create table if not exists {DQ_TABLE} (
        run_id            uuid not null,
        run_ts_utc        timestamptz not null,
        timeframe_minutes int not null,
        tolerance_bars    int not null,

        trading_date      date not null,
        symbol            text not null,

        session_open_et   text null,
        session_close_et  text null,
        expected_bars     int null,
        actual_bars       int not null,

        status            text not null,   -- PASS / FAIL / WARN
        notes             text null,

        primary key (run_id, trading_date, symbol)
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def write_dq_rows(conn, rows: list[dict]):
    if not rows:
        return

    cols = (
        "run_id, run_ts_utc, timeframe_minutes, tolerance_bars, "
        "trading_date, symbol, session_open_et, session_close_et, "
        "expected_bars, actual_bars, status, notes"
    )

    values_sql = ", ".join(["%s"] * 12)
    sql = f"insert into {DQ_TABLE} ({cols}) values ({values_sql});"

    payload = []
    for r in rows:
        payload.append((
            r["run_id"],          # now a string UUID
            r["run_ts_utc"],
            r["timeframe_minutes"],
            r["tolerance_bars"],
            r["trading_date"],
            r["symbol"],
            r.get("session_open_et"),
            r.get("session_close_et"),
            r.get("expected_bars"),
            r["actual_bars"],
            r["status"],
            r.get("notes"),
    ))


    with conn.cursor() as cur:
        cur.executemany(sql, payload)
    conn.commit()


def main():
    run_id = str(uuid.uuid4())
    run_ts_utc = datetime.now(timezone.utc)

    conn = db_connect()
    try:
        ensure_dq_table(conn)

        start_d, end_d = get_db_date_range(conn)
        cal_start = start_d - timedelta(days=3)
        cal_end = end_d + timedelta(days=3)

        calendar = fetch_alpaca_calendar(cal_start, cal_end)

        cal_map = {}
        for day in calendar:
            d = datetime.strptime(day["date"], "%Y-%m-%d").date()
            cal_map[d] = (day["open"], day["close"])

        counts = get_daily_counts(conn)

        results = []
        bad = 0

        full_day_expected = 390 // TIMEFRAME_MINUTES  # 78 for 5-min

        print("\n--- DQ Report (Calendar-aware) ---")
        print(f"Timeframe: {TIMEFRAME_MINUTES}min | Tolerance: {TOLERANCE_BARS} bars")
        print("date       symbol  actual  expected  status   notes")

        for d, sym, actual in counts:
            row = {
                "run_id": run_id,
                "run_ts_utc": run_ts_utc,
                "timeframe_minutes": TIMEFRAME_MINUTES,
                "tolerance_bars": TOLERANCE_BARS,
                "trading_date": d,
                "symbol": sym,
                "actual_bars": actual,
            }

            if d not in cal_map:
                status = "WARN"
                notes = "not in Alpaca calendar (unexpected trading day)"
                row.update({
                    "session_open_et": None,
                    "session_close_et": None,
                    "expected_bars": None,
                    "status": status,
                    "notes": notes,
                })
                bad += 1
            else:
                open_et, close_et = cal_map[d]
                exp = expected_bars(open_et, close_et, TIMEFRAME_MINUTES)

                status = "PASS"
                notes = "full/near-full"

                if actual < exp - TOLERANCE_BARS:
                    status = "FAIL"
                    notes = "missing bars (feed gap or filter issue)"
                    bad += 1
                elif actual > exp:
                    status = "FAIL"
                    notes = "too many bars (extended hours leaked)"
                    bad += 1

                # annotate early close days
                if exp < full_day_expected:
                    notes += f" | early close ({open_et}-{close_et} ET)"

                row.update({
                    "session_open_et": open_et,
                    "session_close_et": close_et,
                    "expected_bars": exp,
                    "status": status,
                    "notes": notes,
                })

                print(f"{d}  {sym:<6} {actual:>6} {exp:>8}  {status:<6} {notes}")

            results.append(row)

        write_dq_rows(conn, results)

        print(f"\nSummary: {len(counts)} symbol-days checked | FAIL/WARN: {bad}")
        print(f"✅ Wrote {len(results)} rows into {DQ_TABLE} for run_id={run_id}")

        if bad > 0:
            raise SystemExit(1)

        print("✅ Calendar-aware validation passed.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
