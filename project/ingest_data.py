import os
import json
from datetime import datetime, timedelta, timezone

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

# We'll use these highly correlated stock pairs for our strategy
STOCK_PAIRS = [("MSFT", "AAPL"), ("WMT", "TGT"), ("KO", "PEP")]

# Unique list of 6 symbols
SYMBOLS = sorted({s for pair in STOCK_PAIRS for s in pair})

# ✅ FIX: use an absolute path so Airflow always writes to the same location
RAW_DIR = os.getenv("RAW_DIR", "/opt/airflow/raw_data")


def get_alpaca_client() -> StockHistoricalDataClient:
    # Support both naming styles (yours + common Alpaca docs)
    api_key = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
    api_secret = os.getenv("ALPACA_API_SECRET") or os.getenv("APCA_API_SECRET_KEY")

    if not api_key or not api_secret:
        raise RuntimeError(
            "Missing Alpaca credentials. Set ALPACA_API_KEY + ALPACA_API_SECRET "
            "(or APCA_API_KEY_ID + APCA_API_SECRET_KEY)."
        )

    return StockHistoricalDataClient(api_key, api_secret)


def fetch_intraday_data_5min(
    client: StockHistoricalDataClient,
    symbol: str,
    lookback_days: int = 60
):
    """
    Fetch intraday 5-minute OHLCV bars for a given stock symbol from Alpaca.
    Returns a raw JSON-like dict that we save to disk.
    """
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=lookback_days)

    req = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame(5, TimeFrameUnit.Minute),
        start=start,
        end=end,
        feed="iex",          # free-tier feed
        adjustment="all"
    )

    print(f"Fetching 5-min bars for {symbol} from Alpaca...")
    bars = client.get_stock_bars(req)
    df = bars.df

    if df is None or df.empty:
        print(f"No 5-min bars returned for {symbol}. Skipping.")
        return None

    # Normalize index -> columns
    df = df.reset_index()

    # Convert to a simple list of bar dicts (raw JSON)
    bar_list = []
    for row in df.to_dict(orient="records"):
        ts = row.get("timestamp")
        ts_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)

        bar_list.append({
            "timestamp": ts_str,          # UTC timestamp
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": int(row["volume"]),
        })

    payload = {
        "symbol": symbol,
        "timeframe": "5Min",
        "source": "alpaca",
        "feed": "iex",
        "start_utc": start.isoformat(),
        "end_utc": end.isoformat(),
        "bars": bar_list
    }

    return payload


def main():
    # ✅ FIX: create raw dir in a stable absolute location
    os.makedirs(RAW_DIR, exist_ok=True)
    print(f"Raw data directory: {RAW_DIR}")

    client = get_alpaca_client()

    for symbol in SYMBOLS:
        data = fetch_intraday_data_5min(client, symbol, lookback_days=60)

        if data:
            file_path = os.path.join(RAW_DIR, f"{symbol}_intraday_5min.json")
            with open(file_path, "w") as f:
                json.dump(data, f, indent=2)

            print(f"✅ Successfully saved raw data for {symbol} to {file_path}")

    print("Done.")


if __name__ == "__main__":
    main()
