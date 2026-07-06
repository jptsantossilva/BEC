"""Exchange adapter registry resolved by the app-wide active exchange."""

from __future__ import annotations

from decimal import Decimal

from bec.exchanges.base import ExchangeAdapter

_default_adapter: ExchangeAdapter | None = None


def get_adapter_for_code(
    code: str, *, sizing_buffer_pct: Decimal = Decimal("1")
) -> ExchangeAdapter:
    code = str(code or "").strip().lower()
    if code == "binance":
        from bec.exchanges.binance_adapter import BinanceAdapter

        return BinanceAdapter()
    if code == "kraken":
        from bec.exchanges.kraken_adapter import KrakenAdapter

        return KrakenAdapter(sizing_buffer_pct=sizing_buffer_pct)
    raise RuntimeError(f"No adapter is available for exchange: {code}")


def get_default_adapter() -> ExchangeAdapter:
    global _default_adapter
    if _default_adapter is not None:
        return _default_adapter

    from bec.utils import database

    exchange = database.get_active_exchange(required=True)
    code = str(exchange["code"])
    # Keep adapter imports lazy. Resetting the registry during exchange
    # selection must not initialize an exchange client or database settings.
    _default_adapter = get_adapter_for_code(
        code,
        sizing_buffer_pct=Decimal(str(exchange.get("sizing_buffer_pct", 1.0))),
    )
    return _default_adapter


def set_default_adapter(adapter: ExchangeAdapter | None) -> None:
    """Override the process adapter, primarily for isolated tests."""
    global _default_adapter
    _default_adapter = adapter
