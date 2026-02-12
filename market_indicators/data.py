import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, Union, Callable
import streamlit as st

from exchanges.binance import get_ohlcv as fetch_binance_OHLCV
from binance.client import Client 

# ----------------------
# Data access & caching
# ----------------------


@st.cache_data(ttl=60*15, show_spinner=False)
def get_OHLCV(symbol: str,
    interval: str = Client.KLINE_INTERVAL_1DAY,
    start_date: Union[str, int, datetime] = "1 Jan, 2010",
    end_date: Optional[Union[str, int, datetime]] = None,
    max_retries: int = 3,
    backoff_sec: float = 1.5
) -> pd.DataFrame:
    """Fetch OHLCV data. Replace this with your real source (e.g., your DB, CCXT, yfinance).
    Returns DataFrame with columns: [timestamp, open, high, low, close, volume]
    """
    df = fetch_binance_OHLCV(symbol, interval, start_date, end_date)
    if df.empty:
        return df

    if isinstance(df.index, pd.DatetimeIndex):
        df = df.rename_axis("timestamp").reset_index()

    df = df.rename(columns={
        "Time": "timestamp",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
    })
    return df

def _ensure_timestamp_column(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with a 'timestamp' column in UTC, regardless of input layout."""
    df = df.copy()

    # Already provided as column?
    if "timestamp" in df.columns:
        ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    elif "Time" in df.columns:  # common alternative
        ts = pd.to_datetime(df["Time"], utc=True, errors="coerce")
    else:
        # Try to use the index
        idx = df.index
        if isinstance(idx, pd.DatetimeIndex):
            ts = idx.tz_convert("UTC") if idx.tz is not None else idx.tz_localize("UTC")
        else:
            # last resort: parse whatever is there
            ts = pd.to_datetime(idx, utc=True, errors="coerce")

    out = df.copy()
    out["timestamp"] = ts
    # Drop any rows where timestamp failed to parse
    out = out.dropna(subset=["timestamp"])
    return out

# ----------------------
# Indicator: Pi Cycle Top (example)
# ----------------------

def _sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=window).mean()

def compute_pi_cycle_top(ohlc_df: pd.DataFrame, *, ma_short: int = 111, ma_long: int = 350, long_multiplier: float = 2.0):
    # 1) Ensure we have a 'timestamp' column regardless of input format
    df = _ensure_timestamp_column(ohlc_df)

    # 2) Sort by time and keep the expected columns
    keep = ["timestamp", "open", "high", "low", "close", "volume"]
    cols = [c for c in keep if c in df.columns]
    df = df[cols].sort_values("timestamp").reset_index(drop=True)

    # 3) Compute MAs
    df["MA_short"] = _sma(df["close"], ma_short)
    df["MA_long"] = _sma(df["close"], ma_long) * long_multiplier


    # 4) Signal when MA_short crosses above MA_long (potential cycle top zone)
    df["signal"] = (df["MA_short"].shift(1) < df["MA_long"].shift(1)) & (df["MA_short"] >= df["MA_long"])

    # 5) Status/score (simple heuristic)
    last_row = df.iloc[-1]
    status = "NEUTRAL"
    score = 0.5
    if pd.notna(last_row["MA_short"]) and pd.notna(last_row["MA_long"]):
        if last_row["MA_short"] > last_row["MA_long"]:
            status = "RISK" # i.e., in the risk/top zone
            score = 0.8
        else:
            status = "SAFE" # i.e., below risk zone
            score = 0.2

    # 6) Last signal date
    last_signal_date = df.loc[df["signal"], "timestamp"].max() if df["signal"].any() else None


    payload = {
    "df": df, # enriched DF with MAs & signal
    "status": status, # string badge
    "score": float(score), # 0..1 risk score
    "last_signal": last_signal_date,
    "last_updated": datetime.utcnow(),
    "params": {
        "ma_short": ma_short,
        "ma_long": ma_long,
        "long_multiplier": long_multiplier,
    },
    "method": "111D SMA vs 350D SMA * 2 crossover on close price.",
    }
    return payload
