"""Kraken public market-data adapter backed by CCXT."""

from __future__ import annotations

from typing import Any

from bec.exchanges.ccxt_adapter import CcxtExchangeAdapter


class KrakenAdapter(CcxtExchangeAdapter):
    code = "kraken"
    name = "Kraken"
    asset_aliases = {
        "XBT": "BTC",
        "XDG": "DOGE",
    }

    def __init__(
        self,
        *,
        client: Any | None = None,
        market_cache_ttl_seconds: float = 900,
        clock=None,
    ):
        kwargs = {
            "client": client,
            "name": self.name,
            "market_cache_ttl_seconds": market_cache_ttl_seconds,
        }
        if clock is not None:
            kwargs["clock"] = clock
        super().__init__(self.code, **kwargs)
