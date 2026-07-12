"""Exchange adapter registry resolved by the app-wide active exchange."""

from __future__ import annotations

from decimal import Decimal

from bec.exchanges.base import ExchangeAdapter, ExchangeCapabilities

_default_adapter: ExchangeAdapter | None = None


def get_registered_exchange_codes() -> tuple[str, ...]:
    """Return adapters that the current application build can resolve."""
    return ("binance", "kraken", "okx", "okx_demo")


def get_adapter_capabilities_for_code(code: str) -> ExchangeCapabilities:
    """Read static adapter behavior without creating an exchange client."""
    code = str(code or "").strip().lower()
    if code == "binance":
        from bec.exchanges.binance_adapter import BinanceAdapter

        return BinanceAdapter.capabilities
    if code == "kraken":
        from bec.exchanges.kraken_adapter import KrakenAdapter

        return KrakenAdapter.capabilities
    if code in {"okx", "okx_demo"}:
        from bec.exchanges.okx_adapter import OkxAdapter

        return OkxAdapter.capabilities
    raise RuntimeError(f"No adapter is available for exchange: {code}")


def get_adapter_for_code(
    code: str,
    *,
    sizing_buffer_pct: Decimal = Decimal("1"),
    adapter_id: str | None = None,
) -> ExchangeAdapter:
    code = str(code or "").strip().lower()
    if code == "binance":
        from bec.exchanges.binance_adapter import BinanceAdapter

        return BinanceAdapter()
    if code == "kraken":
        from bec.exchanges.kraken_adapter import KrakenAdapter

        return KrakenAdapter(sizing_buffer_pct=sizing_buffer_pct)
    if code in {"okx", "okx_demo"}:
        from bec.exchanges.okx_adapter import OkxAdapter

        demo = code == "okx_demo"
        return OkxAdapter(
            adapter_id=adapter_id or "myokx",
            execution_environment="demo" if demo else "production",
            execution_code=code,
            private_enabled=demo,
            sizing_buffer_pct=sizing_buffer_pct,
        )
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
        adapter_id=str(exchange.get("adapter_id") or ""),
    )
    return _default_adapter


def set_default_adapter(adapter: ExchangeAdapter | None) -> None:
    """Override the process adapter, primarily for isolated tests."""
    global _default_adapter
    _default_adapter = adapter
