"""Exchange adapter registry resolved by the app-wide active exchange."""

from __future__ import annotations

from bec.exchanges.base import ExchangeAdapter

_default_adapter: ExchangeAdapter | None = None


def get_default_adapter() -> ExchangeAdapter:
    global _default_adapter
    if _default_adapter is not None:
        return _default_adapter

    from bec.utils import database

    exchange = database.get_active_exchange(required=True)
    if exchange["code"] != "binance":
        raise RuntimeError(
            f"No adapter is available for active exchange: {exchange['code']}"
        )
    # Keep adapter imports lazy. Resetting the registry during exchange
    # selection must not initialize Binance, Telegram, or database settings.
    from bec.exchanges.binance_adapter import BinanceAdapter

    _default_adapter = BinanceAdapter()
    return _default_adapter


def set_default_adapter(adapter: ExchangeAdapter | None) -> None:
    """Override the process adapter, primarily for isolated tests."""
    global _default_adapter
    _default_adapter = adapter
