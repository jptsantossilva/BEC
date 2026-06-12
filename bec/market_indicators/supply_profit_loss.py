import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

# Allow running this file directly from the package path.
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import bec.utils.database as database
import bec.utils.telegram as telegram


SIGNAL_NAME = "BTC Supply Profit/Loss"
BITVIEW_API_BASE_URL = "https://bitview.space/api"
BITVIEW_TIMEOUT = 15
DEFAULT_BACKFILL_DAYS = 365
DEFAULT_UPDATE_DAYS = 10
DEFAULT_CROSS_TOLERANCE = 1.0

BITVIEW_REQUIRED_SERIES = (
    "price",
    "supply_in_profit_share",
    "supply_in_loss_share",
)
BITVIEW_OPTIONAL_SERIES = (
    "supply_in_profit",
    "supply_in_loss",
)
BITVIEW_ALLOWED_SERIES = frozenset(BITVIEW_REQUIRED_SERIES + BITVIEW_OPTIONAL_SERIES)

EVENT_TOP_ZONE = "SUPPLY_PROFIT_TOP_ZONE"
EVENT_EXTREME_TOP_ZONE = "SUPPLY_PROFIT_EXTREME_TOP_ZONE"
EVENT_BOTTOM_ZONE = "SUPPLY_PROFIT_BOTTOM_ZONE"
EVENT_CROSS_50 = "SUPPLY_PROFIT_LOSS_CROSS_50"

EVENT_MESSAGES = {
    EVENT_TOP_ZONE: (
        "BTC Supply in Profit is above 95%. Historically this is an "
        "euphoria/distribution zone. It is not an automatic sell signal."
    ),
    EVENT_EXTREME_TOP_ZONE: (
        "BTC Supply in Profit is above 98%. Extreme euphoria increases "
        "correction risk, but it can persist during strong bull markets."
    ),
    EVENT_BOTTOM_ZONE: (
        "BTC Supply in Profit is below 5%. Historically this is a "
        "capitulation/macro-bottom zone. It is not an automatic buy signal."
    ),
    EVENT_CROSS_50: (
        "BTC Supply in Loss crossed above Supply in Profit. Historically this "
        "marks an important macro stress/capitulation regime transition."
    ),
}


class BitviewError(RuntimeError):
    pass


class BitviewRateLimitError(BitviewError):
    pass


def _empty_supply_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "date",
            "btc_price",
            "percent_supply_in_profit",
            "percent_supply_in_loss",
            "supply_in_profit_btc",
            "supply_in_loss_btc",
            "source",
            "retrieved_at",
            "updated_at",
        ]
    )


def _read_setting(name: str, default):
    try:
        return database.get_setting(name)
    except Exception:
        return default


def _read_int_setting(name: str, default: int) -> int:
    try:
        return int(_read_setting(name, default))
    except (TypeError, ValueError):
        return int(default)


def _read_float_setting(name: str, default: float) -> float:
    try:
        return float(_read_setting(name, default))
    except (TypeError, ValueError):
        return float(default)


def _read_bool_setting(name: str, default: bool) -> bool:
    value = _read_setting(name, default)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def fetch_bitview_series(
    series_name: str,
    days: int = DEFAULT_BACKFILL_DAYS,
    session=None,
) -> dict:
    """Fetch one Bitview daily series."""
    series_name = str(series_name or "").strip()
    if not series_name:
        raise ValueError("series_name is required")
    if series_name not in BITVIEW_ALLOWED_SERIES:
        raise ValueError(f"Unsupported Bitview series: {series_name}")

    client = session or requests
    days = int(days)
    start = "0" if days == 0 else f"-{abs(days)}"
    response = client.get(
        f"{BITVIEW_API_BASE_URL}/series/{series_name}/day",
        params={"start": start},
        timeout=BITVIEW_TIMEOUT,
    )
    if response.status_code == 429:
        raise BitviewRateLimitError(f"Bitview rate limit exceeded for {series_name}.")
    if response.status_code >= 400:
        raise BitviewError(
            f"Bitview request failed for {series_name}: HTTP {response.status_code}"
        )

    try:
        payload = response.json()
    except ValueError as exc:
        raise BitviewError(f"Bitview returned invalid JSON for {series_name}.") from exc

    if not isinstance(payload, dict):
        raise BitviewError(f"Bitview payload for {series_name} is not an object.")
    if payload.get("error"):
        raise BitviewError(f"Bitview error for {series_name}: {payload['error']}")
    data = payload.get("data")
    if not isinstance(data, list) or not data:
        raise BitviewError(f"Bitview payload for {series_name} has no data.")
    if not payload.get("stamp"):
        raise BitviewError(f"Bitview payload for {series_name} has no stamp.")
    return payload


def dates_from_bitview_payload(payload: dict, now_utc: datetime | None = None) -> list:
    """Build daily dates from a Bitview payload, preferring the last complete UTC day."""
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, list) or not data:
        raise BitviewError("Bitview payload has no data.")

    stamp = pd.to_datetime(payload.get("stamp"), utc=True, errors="coerce")
    if pd.isna(stamp):
        raise BitviewError("Bitview payload has invalid stamp.")

    last_complete_utc_day = _last_complete_utc_day(now_utc=now_utc)
    stamp_date = stamp.date()
    end_date = min(stamp_date, last_complete_utc_day)
    return list(pd.date_range(end=end_date, periods=len(data), freq="D").date)


def _last_complete_utc_day(now_utc: datetime | None = None):
    now_utc = now_utc or datetime.now(timezone.utc)
    return (now_utc - timedelta(days=1)).date()


def _series_to_numeric(payload: dict, series_name: str) -> pd.Series:
    data = payload.get("data")
    values = pd.to_numeric(pd.Series(data), errors="coerce")
    if values.notna().sum() == 0:
        raise BitviewError(f"Bitview series {series_name} contains no numeric data.")
    return values


def normalize_bitview_series(
    payloads: dict[str, dict],
    now_utc: datetime | None = None,
) -> pd.DataFrame:
    """Merge Bitview series into normalized supply profit/loss rows."""
    missing = [name for name in BITVIEW_REQUIRED_SERIES if name not in payloads]
    if missing:
        raise BitviewError(f"Missing required Bitview series: {', '.join(missing)}")

    required_lengths = {
        name: len(payloads[name].get("data", [])) for name in BITVIEW_REQUIRED_SERIES
    }
    if len(set(required_lengths.values())) != 1:
        raise BitviewError(f"Required Bitview series lengths differ: {required_lengths}")

    dates = dates_from_bitview_payload(payloads["price"], now_utc=now_utc)
    if len(dates) != required_lengths["price"]:
        raise BitviewError("Bitview date count does not match required series length.")

    df = pd.DataFrame(
        {
            "date": pd.to_datetime(dates, utc=True),
            "btc_price": _series_to_numeric(payloads["price"], "price"),
            "percent_supply_in_profit": _series_to_numeric(
                payloads["supply_in_profit_share"], "supply_in_profit_share"
            ),
            "percent_supply_in_loss": _series_to_numeric(
                payloads["supply_in_loss_share"], "supply_in_loss_share"
            ),
        }
    )

    for series_name, column_name in (
        ("supply_in_profit", "supply_in_profit_btc"),
        ("supply_in_loss", "supply_in_loss_btc"),
    ):
        payload = payloads.get(series_name)
        if payload and len(payload.get("data", [])) == len(df):
            df[column_name] = _series_to_numeric(payload, series_name)
        else:
            df[column_name] = pd.NA

    retrieved_at = pd.to_datetime(
        payloads["price"].get("stamp"), utc=True, errors="coerce"
    )
    df["source"] = "bitview"
    df["retrieved_at"] = retrieved_at.isoformat() if pd.notna(retrieved_at) else ""
    return normalize_supply_profit_loss(df, source="bitview")


def fetch_bitview_supply_profit_loss(
    days: int = DEFAULT_BACKFILL_DAYS,
    session=None,
) -> pd.DataFrame:
    payloads = {}
    for series_name in BITVIEW_REQUIRED_SERIES:
        payloads[series_name] = fetch_bitview_series(
            series_name, days=days, session=session
        )

    for series_name in BITVIEW_OPTIONAL_SERIES:
        try:
            payloads[series_name] = fetch_bitview_series(
                series_name, days=days, session=session
            )
        except BitviewError as exc:
            print(f"Optional Bitview series skipped ({series_name}): {exc}")

    return normalize_bitview_series(payloads)


def normalize_supply_profit_loss(raw: pd.DataFrame, source: str = "bitview") -> pd.DataFrame:
    """Return normalized BTC supply profit/loss rows sorted by date."""
    if raw is None or raw.empty:
        return _empty_supply_frame()

    required_columns = [
        "date",
        "btc_price",
        "percent_supply_in_profit",
        "percent_supply_in_loss",
    ]
    if any(column not in raw.columns for column in required_columns):
        return _empty_supply_frame()

    out = raw.copy()
    out["date"] = pd.to_datetime(out["date"], utc=True, errors="coerce").dt.normalize()
    out["btc_price"] = pd.to_numeric(out["btc_price"], errors="coerce")
    out["percent_supply_in_profit"] = pd.to_numeric(
        out["percent_supply_in_profit"], errors="coerce"
    )
    out["percent_supply_in_loss"] = pd.to_numeric(
        out["percent_supply_in_loss"], errors="coerce"
    )
    if "supply_in_profit_btc" not in out.columns:
        out["supply_in_profit_btc"] = pd.NA
    if "supply_in_loss_btc" not in out.columns:
        out["supply_in_loss_btc"] = pd.NA
    if "source" not in out.columns:
        out["source"] = source
    if "retrieved_at" not in out.columns:
        out["retrieved_at"] = ""
    if "updated_at" not in out.columns:
        out["updated_at"] = ""

    out["supply_in_profit_btc"] = pd.to_numeric(
        out["supply_in_profit_btc"], errors="coerce"
    )
    out["supply_in_loss_btc"] = pd.to_numeric(
        out["supply_in_loss_btc"], errors="coerce"
    )
    out["source"] = out["source"].fillna(source).astype(str)
    out["retrieved_at"] = out["retrieved_at"].fillna("").astype(str)
    out["updated_at"] = out["updated_at"].fillna("").astype(str)

    out = out.dropna(
        subset=[
            "date",
            "btc_price",
            "percent_supply_in_profit",
            "percent_supply_in_loss",
        ]
    )
    out = out[
        (out["btc_price"] > 0)
        & out["percent_supply_in_profit"].between(0, 100)
        & out["percent_supply_in_loss"].between(0, 100)
    ]
    out = out.sort_values("date").drop_duplicates("date", keep="last")
    return out[
        [
            "date",
            "btc_price",
            "percent_supply_in_profit",
            "percent_supply_in_loss",
            "supply_in_profit_btc",
            "supply_in_loss_btc",
            "source",
            "retrieved_at",
            "updated_at",
        ]
    ].reset_index(drop=True)


def load_cached_supply_profit_loss() -> pd.DataFrame:
    return normalize_supply_profit_loss(
        database.get_onchain_btc_supply_profit_loss(), source="cache"
    )


def save_cached_supply_profit_loss(df: pd.DataFrame) -> None:
    normalized = normalize_supply_profit_loss(df, source="bitview")
    if normalized.empty:
        return
    database.upsert_onchain_btc_supply_profit_loss(normalized)


def backfill_last_365_days(days: int | None = None, session=None) -> int:
    if days is None:
        days = _read_int_setting(
            "onchain_supply_profit_loss_backfill_days", DEFAULT_BACKFILL_DAYS
        )
    days = int(days)
    df = fetch_bitview_supply_profit_loss(days=days, session=session)
    save_cached_supply_profit_loss(df)
    print(f"BTC Supply Profit/Loss: backfilled {len(df)} Bitview rows.")
    return len(df)


def update_latest_day(days: int | None = None, session=None) -> int:
    if days is None:
        days = _read_int_setting(
            "onchain_supply_profit_loss_update_days", DEFAULT_UPDATE_DAYS
        )
    days = int(days)
    df = fetch_bitview_supply_profit_loss(days=days, session=session)
    save_cached_supply_profit_loss(df)
    print(f"BTC Supply Profit/Loss: updated {len(df)} recent Bitview rows.")
    return len(df)


def update_missing_days(session=None, now_utc: datetime | None = None) -> int:
    cached = load_cached_supply_profit_loss()
    if cached.empty:
        return backfill_last_365_days(days=0, session=session)

    last_cached_date = pd.to_datetime(cached["date"], utc=True, errors="coerce").max()
    if pd.isna(last_cached_date):
        return backfill_last_365_days(days=0, session=session)

    missing_days = (_last_complete_utc_day(now_utc=now_utc) - last_cached_date.date()).days
    if missing_days <= 0:
        print("BTC Supply Profit/Loss: local table is already up to date.")
        return 0

    df = fetch_bitview_supply_profit_loss(days=missing_days, session=session)
    save_cached_supply_profit_loss(df)
    print(f"BTC Supply Profit/Loss: inserted/updated {len(df)} missing Bitview rows.")
    return len(df)


def _continuous_starts(mask: pd.Series) -> pd.Series:
    mask = mask.fillna(False).astype(bool)
    return mask & ~mask.shift(fill_value=False)


def _event_row(row, event_type: str, severity: str) -> dict:
    return {
        "date": row["date"],
        "event_type": event_type,
        "severity": severity,
        "btc_price": float(row["btc_price"]),
        "percent_supply_in_profit": float(row["percent_supply_in_profit"]),
        "percent_supply_in_loss": float(row["percent_supply_in_loss"]),
        "message": EVENT_MESSAGES[event_type],
        "source": str(row.get("source", "bitview")),
    }


def detect_supply_profit_loss_events(
    df: pd.DataFrame,
    top_threshold: float = 95.0,
    extreme_top_threshold: float = 98.0,
    bottom_threshold: float = 5.0,
    cross_tolerance: float = DEFAULT_CROSS_TOLERANCE,
) -> pd.DataFrame:
    normalized = normalize_supply_profit_loss(df, source="events")
    if normalized.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "event_type",
                "severity",
                "btc_price",
                "percent_supply_in_profit",
                "percent_supply_in_loss",
                "message",
                "source",
            ]
        )

    profit = normalized["percent_supply_in_profit"]
    loss = normalized["percent_supply_in_loss"]
    previous_profit = profit.shift()
    previous_loss = loss.shift()
    definitions = [
        (EVENT_TOP_ZONE, "warning", profit >= float(top_threshold)),
        (EVENT_EXTREME_TOP_ZONE, "risk", profit >= float(extreme_top_threshold)),
        (
            EVENT_BOTTOM_ZONE,
            "opportunity",
            (profit <= float(bottom_threshold))
            | (loss >= 100.0 - float(bottom_threshold)),
        ),
        (
            EVENT_CROSS_50,
            "stress",
            (loss > profit) & (previous_loss <= previous_profit),
        ),
    ]

    events = []
    for event_type, severity, mask in definitions:
        for _, row in normalized.loc[_continuous_starts(mask)].iterrows():
            events.append(_event_row(row, event_type, severity))

    if not events:
        return pd.DataFrame(
            columns=[
                "date",
                "event_type",
                "severity",
                "btc_price",
                "percent_supply_in_profit",
                "percent_supply_in_loss",
                "message",
                "source",
            ]
        )
    return pd.DataFrame(events).sort_values(["date", "event_type"]).reset_index(drop=True)


def format_supply_profit_loss_alert(event: pd.Series | dict) -> str:
    value = event.to_dict() if hasattr(event, "to_dict") else dict(event)
    date_value = pd.to_datetime(value["date"]).strftime("%Y-%m-%d")
    return "\n".join(
        [
            "BTC Supply Profit/Loss Alert",
            f"Event: {value['event_type']}",
            f"Date: {date_value}",
            f"BTC Price: ${float(value['btc_price']):,.0f}",
            f"Supply in Profit: {float(value['percent_supply_in_profit']):.2f}%",
            f"Supply in Loss: {float(value['percent_supply_in_loss']):.2f}%",
            "",
            f"Interpretation: {value['message']}",
            "",
            "Macro/on-chain signal only. This is not an automatic buy/sell order.",
        ]
    )


def send_supply_profit_loss_telegram_alert(event: pd.Series | dict) -> None:
    value = event.to_dict() if hasattr(event, "to_dict") else dict(event)
    message = telegram.telegram_prefix_signals_sl + format_supply_profit_loss_alert(value)
    telegram.send_telegram_message(
        telegram.telegram_token_signals,
        telegram.EMOJI_INFORMATION,
        message,
    )
    database.add_signal_log(
        date=datetime.now(),
        signal=SIGNAL_NAME,
        signal_message=str(value["event_type"]),
        symbol="BTC",
        notes=str(value["message"]),
    )


def send_new_supply_profit_loss_alerts(events: pd.DataFrame) -> int:
    if events is None or events.empty:
        return 0

    sent_count = 0
    for _, event in events.iterrows():
        event_date = pd.to_datetime(event["date"]).strftime("%Y-%m-%d")
        event_type = str(event["event_type"])
        if database.onchain_signal_alert_sent(SIGNAL_NAME, event_type, event_date):
            continue
        send_supply_profit_loss_telegram_alert(event)
        database.record_onchain_signal_alert_sent(SIGNAL_NAME, event_type, event_date)
        sent_count += 1
    return sent_count


def run_btc_supply_profit_loss_update_job(session=None) -> int:
    try:
        update_missing_days(session=session)
    except BitviewError as exc:
        print(f"BTC Supply Profit/Loss: Bitview update skipped: {exc}")
        return 0
    except requests.RequestException as exc:
        print(f"BTC Supply Profit/Loss: Bitview network error: {exc}")
        return 0

    df = load_cached_supply_profit_loss()
    if df.empty:
        print("BTC Supply Profit/Loss: no cached Bitview data available.")
        return 0

    tolerance = _read_float_setting(
        "onchain_supply_profit_loss_cross_tolerance", DEFAULT_CROSS_TOLERANCE
    )
    top_threshold = _read_float_setting("onchain_supply_profit_loss_top_threshold", 95.0)
    extreme_top_threshold = _read_float_setting(
        "onchain_supply_profit_loss_extreme_top_threshold", 98.0
    )
    bottom_threshold = _read_float_setting(
        "onchain_supply_profit_loss_bottom_threshold", 5.0
    )
    events = detect_supply_profit_loss_events(
        df,
        top_threshold=top_threshold,
        extreme_top_threshold=extreme_top_threshold,
        bottom_threshold=bottom_threshold,
        cross_tolerance=tolerance,
    )
    if events.empty:
        print("BTC Supply Profit/Loss: no events detected.")
        return 0

    latest_date = pd.to_datetime(df["date"]).max().normalize()
    latest_events = events[pd.to_datetime(events["date"]).dt.normalize() == latest_date]
    if not _read_bool_setting("onchain_supply_profit_loss_send_telegram_alerts", True):
        print("BTC Supply Profit/Loss: Telegram alerts disabled.")
        return 0
    sent_count = send_new_supply_profit_loss_alerts(latest_events)
    print(f"BTC Supply Profit/Loss: sent {sent_count} new alerts.")
    return sent_count


def run_btc_supply_profit_loss_signal_check() -> int:
    return run_btc_supply_profit_loss_update_job()


def run() -> None:
    run_btc_supply_profit_loss_update_job()


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(description="BTC Supply Profit/Loss Bitview jobs")
    parser.add_argument("--backfill", type=int, help="Backfill N days from Bitview")
    parser.add_argument(
        "--update-latest",
        action="store_true",
        help="Update recent Bitview rows and send latest alerts",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = _parse_args(argv)
    if args.backfill:
        backfill_last_365_days(days=args.backfill)
        return
    if args.update_latest:
        run_btc_supply_profit_loss_update_job()
        return
    run()


if __name__ == "__main__":
    main()
