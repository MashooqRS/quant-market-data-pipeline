import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, explode, mean, stddev, to_timestamp, lit,
    dayofweek, hour, minute, count, when, log,
    from_utc_timestamp
)
from pyspark.sql.types import DecimalType

RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "/opt/airflow/raw_data")

DB_HOST = os.getenv("DB_HOST", "localhost")  # in docker: quant_db
DB_NAME = os.getenv("DB_NAME", "quant_data_db")
DB_USER = os.getenv("DB_USER", "mashooqrabbanishaik")
DB_PASS = os.getenv("DB_PASS", "quant@300")
DB_TABLE = os.getenv("DB_TABLE", "intraday_data")
DB_URL = f"jdbc:postgresql://{DB_HOST}/{DB_NAME}"

STOCK_PAIRS = [("MSFT", "AAPL"), ("WMT", "TGT"), ("KO", "PEP")]

# Exchange timezone (handles DST automatically)
EXCHANGE_TZ = "America/New_York"


def initialize_spark_session():
    print("Initializing Spark session...")
    return (
        SparkSession.builder
        .appName("QuantPipeline")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )


def read_alpaca_json(spark, path):
    # Important: multiline JSON to keep `bars` from becoming NULL
    return (
        spark.read
        .option("multiline", "true")
        .option("mode", "PERMISSIVE")
        .json(path)
    )


def clean_data(raw_df, symbol):
    raw_df.selectExpr("size(bars) as bars_size").show(1, truncate=False)

    exploded = raw_df.select(explode(col("bars")).alias("bar"))
    print(f"[DEBUG] {symbol} exploded rows:", exploded.count())

    # Alpaca timestamp example: 2025-10-20T13:30:00+00:00
    ts_utc = to_timestamp(col("bar.timestamp"), "yyyy-MM-dd'T'HH:mm:ssXXX")

    df = (
        exploded.select(
            lit(symbol).alias("symbol"),
            ts_utc.alias("timestamp_utc"),
            col("bar.open").cast(DecimalType(15, 4)).alias("open"),
            col("bar.high").cast(DecimalType(15, 4)).alias("high"),
            col("bar.low").cast(DecimalType(15, 4)).alias("low"),
            col("bar.close").cast(DecimalType(15, 4)).alias("close"),
            col("bar.volume").cast("integer").alias("volume"),
        )
        .na.drop(subset=["close", "timestamp_utc"])
    )

    print(f"[DEBUG] {symbol} clean rows:", df.count())
    return df


def filter_market_hours_keep_utc(df):
    """
    Professional approach:
    - Convert UTC -> Exchange local time ONLY for filtering (DST-safe)
    - Keep canonical timestamp in UTC for storage and joins
    """
    print("Filtering RTH using exchange TZ (DST-safe), keeping UTC canonical...")

    df_et = df.withColumn("ts_et", from_utc_timestamp(col("timestamp_utc"), EXCHANGE_TZ))

    df_parts = (
        df_et.withColumn("dow", dayofweek(col("ts_et")))
             .withColumn("hh", hour(col("ts_et")))
             .withColumn("mm", minute(col("ts_et")))
    )

    # Mon-Fri
    weekday = col("dow").isin([2, 3, 4, 5, 6])

    # 09:30 <= time <= 15:55 (last 5-min bar starts at 15:55)
    start_ok = ((col("hh") > 9) | ((col("hh") == 9) & (col("mm") >= 30)))
    end_ok   = ((col("hh") < 15) | ((col("hh") == 15) & (col("mm") <= 55)))

    out = df_parts.filter(weekday & start_ok & end_ok).drop("ts_et", "dow", "hh", "mm")

    # Keep UTC as the stored timestamp column
    return out.withColumnRenamed("timestamp_utc", "timestamp")


def process_pair_data(spark, symbol1, symbol2):
    print(f"Processing pair: {symbol1} / {symbol2}")

    p1 = os.path.join(RAW_DATA_DIR, f"{symbol1}_intraday_5min.json")
    p2 = os.path.join(RAW_DATA_DIR, f"{symbol2}_intraday_5min.json")

    if not os.path.exists(p1):
        raise FileNotFoundError(f"Missing input file for {symbol1}: {p1}")
    if not os.path.exists(p2):
        raise FileNotFoundError(f"Missing input file for {symbol2}: {p2}")

    df1_raw = read_alpaca_json(spark, p1)
    df2_raw = read_alpaca_json(spark, p2)

    df1 = filter_market_hours_keep_utc(clean_data(df1_raw, symbol1))
    df2 = filter_market_hours_keep_utc(clean_data(df2_raw, symbol2))

    print(f"[DEBUG] {symbol1} RTH rows:", df1.count())
    print(f"[DEBUG] {symbol2} RTH rows:", df2.count())

    pair_df = (
        df1.alias("df1")
        .join(df2.alias("df2"), col("df1.timestamp") == col("df2.timestamp"), "inner")
        .select(
            col("df1.timestamp"),
            lit(f"{symbol1}-{symbol2}").alias("pair_name"),
            col("df1.symbol").alias("symbol1"),
            col("df2.symbol").alias("symbol2"),
            col("df1.close").alias("close1"),
            col("df2.close").alias("close2"),
            col("df1.open").alias("open1"),
            col("df1.high").alias("high1"),
            col("df1.low").alias("low1"),
            col("df1.volume").alias("volume1"),
            col("df2.open").alias("open2"),
            col("df2.high").alias("high2"),
            col("df2.low").alias("low2"),
            col("df2.volume").alias("volume2"),
            (log(col("df1.close")) - log(col("df2.close"))).alias("spread"),
        )
    )

    print(f"[DEBUG] Joined pair rows ({symbol1}-{symbol2}):", pair_df.count())

    rolling_window = Window.partitionBy("pair_name").orderBy("timestamp").rowsBetween(-59, 0)
    MIN_WINDOW_SIZE = 30

    final_pair_df = (
        pair_df.withColumn("rolling_mean", mean("spread").over(rolling_window))
               .withColumn("rolling_stddev", stddev("spread").over(rolling_window))
               .withColumn("window_size", count("*").over(rolling_window))
               .withColumn("z_score_raw", (col("spread") - col("rolling_mean")) / col("rolling_stddev"))
               .withColumn("z_score", when(col("window_size") >= MIN_WINDOW_SIZE, col("z_score_raw")).otherwise(None))
    )

    df1_final = final_pair_df.select(
        col("symbol1").alias("symbol"),
        col("timestamp"),
        col("open1").alias("open"),
        col("high1").alias("high"),
        col("low1").alias("low"),
        col("close1").alias("close"),
        col("volume1").alias("volume"),
        col("spread"),
        col("z_score"),
    )

    df2_final = final_pair_df.select(
        col("symbol2").alias("symbol"),
        col("timestamp"),
        col("open2").alias("open"),
        col("high2").alias("high"),
        col("low2").alias("low"),
        col("close2").alias("close"),
        col("volume2").alias("volume"),
        (col("spread") * -1).alias("spread"),
        (col("z_score") * -1).alias("z_score"),
    )

    return df1_final.unionByName(df2_final)


def main():
    spark = initialize_spark_session()

    frames = []
    for s1, s2 in STOCK_PAIRS:
        frames.append(process_pair_data(spark, s1, s2))

    final_df = frames[0]
    for df in frames[1:]:
        final_df = final_df.unionByName(df)

    total = final_df.count()
    print("[DEBUG] Final DF rows:", total)
    if total == 0:
        raise RuntimeError("Final DF is empty. Stop before overwriting Postgres.")

    print("Writing data to PostgreSQL...")
    (final_df.write
        .format("jdbc")
        .option("url", DB_URL)
        .option("dbtable", DB_TABLE)
        .option("user", DB_USER)
        .option("password", DB_PASS)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    print("Data transformation and loading complete.")
    spark.stop()


if __name__ == "__main__":
    main()
