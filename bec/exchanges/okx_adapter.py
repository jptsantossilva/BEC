"""Credential-free OKX spot public-data adapter."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from bec.exchanges.base import ExchangeCapabilities
from bec.exchanges.ccxt_adapter import CcxtExchangeAdapter


class OkxAdapter(CcxtExchangeAdapter):
    """OKX public spot data through either the EEA or global CCXT variant."""

    code = "okx"
    name = "OKX"
    capabilities = ExchangeCapabilities(
        supports_backtesting=False,
        supports_live_trading=False,
        requires_explicit_live_flags=True,
        supports_signal_schedules=False,
        supports_reconciliation=False,
        uses_gated_live_execution=False,
    )

    def __init__(
        self,
        *,
        adapter_id: str = "myokx",
        client: Any | None = None,
        market_cache_ttl_seconds: float = 900,
        clock=None,
    ):
        variant = str(adapter_id or "").strip().lower()
        if variant not in {"myokx", "okx"}:
            raise ValueError("OKX adapter variant must be myokx or okx")
        kwargs = {
            "client": client,
            "name": self.name,
            "market_cache_ttl_seconds": market_cache_ttl_seconds,
            # PR 4 deliberately has no private API boundary at all.
            "private_enabled": False,
            "sizing_buffer_pct": Decimal("1"),
        }
        if clock is not None:
            kwargs["clock"] = clock
        super().__init__(variant, **kwargs)
        self.code = "okx"
        self.adapter_id = variant
