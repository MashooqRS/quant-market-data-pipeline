import os
import pandas as pd
import psycopg2
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots

st.set_page_config(page_title="Quant Signal Explorer", layout="wide")

# ---- DB Config (matches your Docker port mapping) ----
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = os.getenv("DB_NAME", "quant_data_db")
DB_USER = os.getenv("DB_USER", "mashooqrabbanishaik")
DB_PASS = os.getenv("DB_PASS", "quant@300")

conn_string = (
    f"dbname='{DB_NAME}' user='{DB_USER}' host='{DB_HOST}' "
    f"port='{DB_PORT}' password='{DB_PASS}'"
)

ET_TZ = "America/New_York"

@st.cache_data(ttl=300)
def get_symbols():
    q = "SELECT DISTINCT symbol FROM intraday_data ORDER BY symbol;"
    with psycopg2.connect(conn_string) as conn:
        df = pd.read_sql_query(q, conn)
    return df["symbol"].tolist()

@st.cache_data(ttl=300)
def load_symbol_data(symbol, start_ts=None, end_ts=None):
    base = """
    SELECT timestamp, symbol, open, high, low, close, volume, spread, z_score
    FROM intraday_data
    WHERE symbol = %s
    """
    params = [symbol]

    if start_ts is not None:
        base += " AND timestamp >= %s"
        params.append(start_ts)
    if end_ts is not None:
        base += " AND timestamp <= %s"
        params.append(end_ts)

    base += " ORDER BY timestamp;"

    with psycopg2.connect(conn_string) as conn:
        df = pd.read_sql_query(base, conn, params=tuple(params))

    if df.empty:
        return df

    # --- IMPORTANT: Alpaca timestamps are UTC; convert to US/Eastern ---
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(ET_TZ)

    # Plotly rangebreaks are most reliable with tz-naive datetimes
    df["timestamp"] = df["timestamp"].dt.tz_localize(None)

    return df

st.title("Quant Signal Explorer (OHLC + Z-Score)")

# ---- Sidebar controls ----
symbols = get_symbols()
if not symbols:
    st.error("No symbols found in intraday_data.")
    st.stop()

symbol = st.sidebar.selectbox("Select symbol", symbols, index=0)

# Load full data first to set date bounds quickly
df_full = load_symbol_data(symbol)
if df_full.empty:
    st.warning(f"No data found for {symbol}.")
    st.stop()

min_dt = df_full["timestamp"].min()
max_dt = df_full["timestamp"].max()

st.sidebar.caption(f"Data range (ET): {min_dt} → {max_dt}")

use_date_filter = st.sidebar.checkbox("Filter by date range", value=False)
start_dt, end_dt = None, None

if use_date_filter:
    start_dt = st.sidebar.date_input("Start date", value=min_dt.date())
    end_dt = st.sidebar.date_input("End date", value=max_dt.date())
    # These are date-only (tz-naive) bounds; OK because df["timestamp"] is tz-naive ET too
    start_dt = pd.Timestamp(start_dt)
    end_dt = pd.Timestamp(end_dt) + pd.Timedelta(hours=23, minutes=59, seconds=59)

df = load_symbol_data(symbol, start_dt, end_dt)

# ---- Plot ----
fig = make_subplots(
    rows=2, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.05,
    subplot_titles=(f"{symbol} Price (OHLC)", "Z-Score Signal"),
    row_heights=[0.7, 0.3]
)

fig.add_trace(
    go.Candlestick(
        x=df["timestamp"],
        open=df["open"],
        high=df["high"],
        low=df["low"],
        close=df["close"],
        name=f"{symbol} OHLC",
    ),
    row=1, col=1
)

fig.add_trace(
    go.Scatter(
        x=df["timestamp"],
        y=df["z_score"],
        mode="lines",
        name="Z-Score",
    ),
    row=2, col=1
)

fig.add_trace(
    go.Scatter(
        x=df["timestamp"],
        y=[2.0] * len(df),
        mode="lines",
        name="Sell Signal (+2)",
        line=dict(dash="dash"),
    ),
    row=2, col=1
)

fig.add_trace(
    go.Scatter(
        x=df["timestamp"],
        y=[-2.0] * len(df),
        mode="lines",
        name="Buy Signal (-2)",
        line=dict(dash="dash"),
    ),
    row=2, col=1
)

fig.update_layout(
    height=800,
    xaxis_rangeslider_visible=False,
    showlegend=True
)

# ✅ Remove non-trading gaps: weekends + 4:00pm->9:30am ET
fig.update_xaxes(
    rangebreaks=[
        dict(bounds=["sat", "mon"]),            # hide weekends
        dict(bounds=[16, 9.5], pattern="hour")  # hide non-trading hours (ET)
    ]
)

st.plotly_chart(fig, use_container_width=True)

st.subheader("Data Preview")
st.dataframe(df.tail(200), use_container_width=True)

st.caption(f"Rows loaded: {len(df)}")
