import os
import json
import requests
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, sequence, to_timestamp, broadcast, to_date, expr
)
from pyspark.sql.types import StructType, StructField, StringType

# -----------------------------
# DB Config (docker-safe)
# -----------------------------
DB_HOST = os.getenv("DB_HOST", "localhost")          # docker: quant_db
DB_PORT = int(os.getenv("DB_PORT", "5432"))          # local: 5433
DB_NAME = os.getenv("DB_NAME", "quant_data_db")
DB_USER = os.getenv("DB_USER", "mashooqrabbanishaik")
DB_PASS = os.getenv("DB_PASS", "quant@300")
DATA_TABLE = os.getenv("DB_TABLE", "intraday_data")

DB_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Output table for missing bars
MISSING_TABLE = os.getenv("MISSING_TABLE", "dq_missing_bars")

# -----------------------------
# Calendar Config (Alpaca)
# -----------------------------
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "./raw_data")
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
TIMEFRAME_MINUTES = int(os.getenv("TIMEFRAME_MINUTES", "5"))

# We keep canonical UTC in DB. Alpaca calendar returns open/close in ET.
MARKET_TZ = "America/New_York"


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def alpaca_headers():
    key = os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_API_KEY")
    secret = os.getenv("APCA_API_SECRET_KEY") or os.getenv("ALPACA_API_SECRET")
    if not key or not secret:
        raise ValueError("Missing Alpaca credentials (APCA_API_KEY_ID / APCA_API_SECRET_KEY).")
    return {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}


def fetch_alpaca_calendar(start_date: str, end_date: str) -> list[dict]:
    """
    Fetch Alpaca trading calendar between [start_date, end_date] inclusive.
    Caches JSON locally to RAW_DATA_DIR.
    """
    ensure_dir(RAW_DATA_DIR)
    cache_path = os.path.join(RAW_DATA_DIR, f"alpaca_calendar_{start_date}_{end_date}.json")

    if os.path.exists(cache_path):
        with open(cache_path, "r") as f:
            return json.load(f)

    url = f"{ALPACA_BASE_URL}/v2/calendar"
    r = requests.get(url, params={"start": start_date, "end": end_date}, headers=alpaca_headers(), timeout=30)
    r.raise_for_status()
    cal = r.json()

    with open(cache_path, "w") as f:
        json.dump(cal, f, indent=2)

    return cal


def initialize_spark_session():
    print("Initializing Spark session for audit...")
    spark = (
        SparkSession.builder
        .appName("QuantAuditCalendarAware")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        # Keep Spark timestamp behavior consistent (we store/expect UTC)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    spark = initialize_spark_session()

    print("Loading actual timestamps from PostgreSQL...")
    actual_df = (
        spark.read.format("jdbc")
        .option("url", DB_URL)
        .option("dbtable", DATA_TABLE)
        .option("user", DB_USER)
        .option("password", DB_PASS)
        .option("driver", "org.postgresql.Driver")
        .load()
        .select(col("symbol"), col("timestamp"))
        .where(col("symbol").isNotNull() & col("timestamp").isNotNull())
    )

    # Determine overall date range from DB (UTC)
    min_max = actual_df.selectExpr("min(timestamp) as min_ts", "max(timestamp) as max_ts").first()
    if not min_max or not min_max["min_ts"]:
        print("No data found. Exiting audit.")
        spark.stop()
        return

    start_d = min_max["min_ts"].date().isoformat()
    end_d = min_max["max_ts"].date().isoformat()

    print(f"Audit range from DB: {start_d} → {end_d}")

    # Fetch trading calendar days in that range
    cal = fetch_alpaca_calendar(start_d, end_d)
    if not cal:
        print("No calendar days returned. Exiting audit.")
        spark.stop()
        return

    # Build session bounds per day in UTC using Python tz conversion (DST + early close safe)
    try:
        from zoneinfo import ZoneInfo
        ny_tz = ZoneInfo(MARKET_TZ)
    except Exception:
        raise RuntimeError(
            "Python zoneinfo not available. Use Python 3.9+ or install tzdata in your env/container."
        )

    session_rows = []
    for day in cal:
        d = day["date"]       # YYYY-MM-DD
        open_et = day["open"]   # HH:MM (ET)
        close_et = day["close"] # HH:MM (ET)

        open_dt_et = datetime.fromisoformat(f"{d}T{open_et}:00").replace(tzinfo=ny_tz)
        close_dt_et = datetime.fromisoformat(f"{d}T{close_et}:00").replace(tzinfo=ny_tz)

        open_dt_utc = open_dt_et.astimezone(timezone.utc)
        close_dt_utc = close_dt_et.astimezone(timezone.utc)

        # For 5-min bars where timestamps represent bar START times:
        # last bar starts at (close - 5min)
        last_bar_utc = close_dt_utc - timedelta(minutes=TIMEFRAME_MINUTES)

        session_rows.append({
            "date": d,
            "session_open_utc": open_dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
            "last_bar_utc": last_bar_utc.strftime("%Y-%m-%d %H:%M:%S"),
        })

    schema = StructType([
        StructField("date", StringType(), False),
        StructField("session_open_utc", StringType(), False),
        StructField("last_bar_utc", StringType(), False),
    ])

    sessions_df = spark.createDataFrame(session_rows, schema=schema)

    print("Building expected 5-min grid per trading session (DST + early close safe)...")

    # IMPORTANT FIXES:
    # 1) Create start_ts / end_ts columns from your string fields.
    # 2) sequence() must use step as 3rd argument (or step=), NOT expr=.
    # 3) You must import expr from pyspark.sql.functions.
    grid_df = (
        sessions_df
        .withColumn("start_ts", to_timestamp(col("session_open_utc")))  # parsed as UTC due to session timezone
        .withColumn("end_ts", to_timestamp(col("last_bar_utc")))
        .select(
            col("date"),
            explode(
                sequence(
                    col("start_ts"),
                    col("end_ts"),
                    expr(f"INTERVAL {TIMEFRAME_MINUTES} MINUTES")
                )
            ).alias("timestamp")
        )
    )

    # Add all symbols
    symbols_df = actual_df.select("symbol").distinct()
    expected_df = grid_df.crossJoin(broadcast(symbols_df)).select("symbol", "timestamp", "date")

    # Anti-join to find missing bars
    print("Finding missing bars via anti-join...")
    missing_df = (
        expected_df.join(
            actual_df.select("symbol", "timestamp"),
            ["symbol", "timestamp"],
            "anti"
        )
        .withColumn("trading_date", to_date(col("timestamp")))
        .orderBy("timestamp", "symbol")
    )

    missing_count = missing_df.count()
    print(f"\n--- AUDIT RESULT ---\nMissing bars found: {missing_count}\n")

    # Persist to Postgres for Metabase
    print(f"Writing missing bars to Postgres table: {MISSING_TABLE} ...")
    (
        missing_df.select(
            col("symbol"),
            col("timestamp"),
            col("trading_date")
        )
        .write.format("jdbc")
        .option("url", DB_URL)
        .option("dbtable", MISSING_TABLE)
        .option("user", DB_USER)
        .option("password", DB_PASS)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    print("✅ Missing bars audit table written.")
    spark.stop()


if __name__ == "__main__":
    main()
