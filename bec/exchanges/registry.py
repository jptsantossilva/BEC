"""Exchange adapter registry.

Phase 1 intentionally resolves Binance unconditionally. Database-backed exchange
selection is introduced only by the exchange-aware runtime PR.
"""

from __future__ import annotations

from bec.exchanges.base import ExchangeAdapter
from bec.exchanges.binance_adapter import BinanceAdapter

_default_adapter: ExchangeAdapter | None = None


def get_default_adapter() -> ExchangeAdapter:
    global _default_adapter
    if _default_adapter is None:
        _default_adapter = BinanceAdapter()
    return _default_adapter


def set_default_adapter(adapter: ExchangeAdapter | None) -> None:
    """Override the process adapter, primarily for isolated tests."""
    global _default_adapter
    _default_adapter = adapter

