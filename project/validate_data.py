import os
import sys
import psycopg2
from datetime import datetime, timezone, timedelta

# --- Config ---
BAR_MINUTES = int(os.getenv("BAR_MINUTES", "5"))

# Full-day expected bars (9:30–16:00 ET = 6.5h = 390 mins; 390/5 = 78)
EXPECTED_BARS_FULL = int(os.getenv("EXPECTED_BARS_FULL", "78"))

# If missing bars > this threshold => FAIL
FAIL_MISSING_BARS_GT = int(os.getenv("FAIL_MISSING_BARS_GT", "2"))

DB_TABLE = os.getenv("DB_TABLE", "intraday_data")

DB_HOST = os.getenv("DB_HOST", "quant_db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "quant_data_db")
DB_USER = os.getenv("DB_USER", "mashooqrabbanishaik")
DB_PASS = os.getenv("DB_PASS", "quant@300")

# Optional: list full-closure holidays to SKIP validation (comma-separated)
# Example: "2025-12-25,2025-01-01"
FULL_CLOSE_DATES = {d.strip() for d in os.getenv("FULL_CLOSE_DATES", "").split(",") if d.strip()}

# Detection rule:
# If the day's last timestamp is earlier than this many minutes before a full close,
# we treat it as an early-close/short session day and adjust expected bars.
EARLY_CLOSE_DETECT_MINUTES_BEFORE_FULL_CLOSE = int(
    os.getenv("EARLY_CLOSE_DETECT_MINUTES_BEFORE_FULL_CLOSE", "120")  # 2 hours
)

def ceil_div(a: int, b: int) -> int:
    return (a + b - 1) // b

def main():
    # store as naive UTC timestamp (consistent with your DB "timestamp without time zone")
    run_ts = datetime.now(timezone.utc).replace(tzinfo=None)

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )
    conn.autocommit = True

    with conn.cursor() as cur:
        # 1) Find latest trading day present
        cur.execute(f"SELECT MAX(timestamp::date) FROM {DB_TABLE};")
        latest_day = cur.fetchone()[0]
        if latest_day is None:
            print("FAIL: No data found to validate.")
            sys.exit(1)

        latest_day_str = str(latest_day)

        # If latest_day is a known full-close holiday (optional behavior)
        # You can choose: skip with OK(0) or fail(1). Here: SKIP with OK.
        if latest_day_str in FULL_CLOSE_DATES:
            print(f"RUN BADGE: OK | trading_date={latest_day} | notes=Holiday full close (skipped validation)")
            sys.exit(0)

        # 2) Ensure detail + summary tables exist
        cur.execute("""
        CREATE TABLE IF NOT EXISTS intraday_quality_report (
          run_ts_utc     TIMESTAMP NOT NULL DEFAULT NOW(),
          trading_date   DATE NOT NULL,
          symbol         TEXT NOT NULL,
          expected_bars  INT NOT NULL,
          actual_bars    INT NOT NULL,
          missing_bars   INT NOT NULL,
          status         TEXT NOT NULL,   -- OK / WARN / FAIL
          notes          TEXT
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS intraday_quality_run_summary (
          run_ts_utc        TIMESTAMP NOT NULL DEFAULT NOW(),
          trading_date      DATE NOT NULL,
          expected_bars     INT NOT NULL,
          symbols_total     INT NOT NULL,
          symbols_ok        INT NOT NULL,
          symbols_warn      INT NOT NULL,
          symbols_fail      INT NOT NULL,
          max_missing_bars  INT NOT NULL,
          overall_status    TEXT NOT NULL, -- OK / WARN / FAIL
          notes             TEXT
        );
        """)

        # 3) Detect if this looks like a short session day based on max timestamp for that date
        # We assume your DB timestamps are stored as UTC-naive (as your comment says).
        cur.execute(f"""
            SELECT MAX(timestamp) AS last_ts_utc
            FROM {DB_TABLE}
            WHERE timestamp::date = %s;
        """, (latest_day,))
        last_ts_utc = cur.fetchone()[0]

        if last_ts_utc is None:
            print(f"FAIL: No timestamps found for latest_day={latest_day}.")
            sys.exit(1)

        # Full close time in UTC for regular session end:
        # 16:00 ET is typically 21:00 UTC during Standard Time and 20:00 UTC during DST.
        # Instead of hardcoding ET/DST logic, we infer "short session" from your actual data:
        # - If the last timestamp is much earlier than a full day's end-of-session window,
        #   we compute expected bars from session length (from first to last bar).
        cur.execute(f"""
            SELECT MIN(timestamp) AS first_ts_utc
            FROM {DB_TABLE}
            WHERE timestamp::date = %s;
        """, (latest_day,))
        first_ts_utc = cur.fetchone()[0]

        # Compute observed session minutes from first to last bar, inclusive of the last bar slot.
        # Example: 9:30 to 13:00 in 5-min bars -> 42 bars.
        observed_minutes = int((last_ts_utc - first_ts_utc).total_seconds() // 60)
        # Inclusive bars approximation: from first bar time to last bar time, step=BAR_MINUTES
        expected_bars_dynamic = ceil_div(observed_minutes, BAR_MINUTES) + 1

        # Decide which expected bars to use:
        # If observed expected is close to full-day bars, keep full-day expected.
        # If clearly shorter (early close), use dynamic expected.
        # This avoids penalizing legitimate early-close days.
        if expected_bars_dynamic < (EXPECTED_BARS_FULL - ceil_div(EARLY_CLOSE_DETECT_MINUTES_BEFORE_FULL_CLOSE, BAR_MINUTES)):
            expected_bars = expected_bars_dynamic
            session_note = f"Short session detected; expected_bars={expected_bars} (dynamic)"
        else:
            expected_bars = EXPECTED_BARS_FULL
            session_note = f"Regular session assumed; expected_bars={expected_bars} (full)"

        # 4) Compute per-symbol bar counts for latest_day
        cur.execute(f"""
        WITH per_day AS (
          SELECT
            symbol,
            timestamp::date AS trading_date,
            COUNT(*) AS bars
          FROM {DB_TABLE}
          WHERE timestamp::date = %s
          GROUP BY symbol, timestamp::date
        )
        SELECT
          symbol,
          trading_date,
          %s AS expected_bars,
          bars AS actual_bars,
          GREATEST(0, (%s - bars)) AS missing_bars
        FROM per_day
        ORDER BY symbol;
        """, (latest_day, expected_bars, expected_bars))

        rows = cur.fetchall()
        if not rows:
            print(f"FAIL: No rows found for latest_day={latest_day}.")
            sys.exit(1)

        symbols_total = len(rows)
        symbols_ok = 0
        symbols_warn = 0
        symbols_fail = 0
        max_missing = 0

        # 5) Insert detail rows
        for symbol, trading_date, expected_bars_used, actual_bars, missing_bars in rows:
            missing_bars = int(missing_bars)
            max_missing = max(max_missing, missing_bars)

            if missing_bars == 0:
                status = "OK"
                notes = session_note
                symbols_ok += 1
            elif missing_bars <= FAIL_MISSING_BARS_GT:
                status = "WARN"
                notes = f"{session_note}; Missing {missing_bars} bars"
                symbols_warn += 1
            else:
                status = "FAIL"
                notes = f"{session_note}; Missing {missing_bars} bars"
                symbols_fail += 1

            cur.execute("""
              INSERT INTO intraday_quality_report
              (run_ts_utc, trading_date, symbol, expected_bars, actual_bars, missing_bars, status, notes)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """, (run_ts, trading_date, symbol, expected_bars_used, actual_bars, missing_bars, status, notes))

        # 6) Overall badge
        if symbols_fail > 0:
            overall_status = "FAIL"
            notes = f"{symbols_fail} symbols failed completeness. {session_note}"
        elif symbols_warn > 0:
            overall_status = "WARN"
            notes = f"{symbols_warn} symbols have missing bars (within threshold). {session_note}"
        else:
            overall_status = "OK"
            notes = f"All symbols complete. {session_note}"

        # 7) Run-level summary
        cur.execute("""
        INSERT INTO intraday_quality_run_summary (
          run_ts_utc, trading_date, expected_bars,
          symbols_total, symbols_ok, symbols_warn, symbols_fail,
          max_missing_bars, overall_status, notes
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """, (
            run_ts, latest_day, expected_bars,
            symbols_total, symbols_ok, symbols_warn, symbols_fail,
            max_missing, overall_status, notes
        ))

        print(
            f"RUN BADGE: {overall_status} | trading_date={latest_day} "
            f"| total={symbols_total} ok={symbols_ok} warn={symbols_warn} fail={symbols_fail} "
            f"| max_missing={max_missing} | {session_note}"
        )

        # Policy: fail DAG if severe missing bars
        if max_missing > FAIL_MISSING_BARS_GT:
            print(f"FAIL: max_missing_bars={max_missing} > threshold={FAIL_MISSING_BARS_GT}")
            sys.exit(1)

    conn.close()
    sys.exit(0)

if __name__ == "__main__":
    main()
