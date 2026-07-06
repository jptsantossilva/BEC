"""Kraken public market-data adapter backed by CCXT."""

from __future__ import annotations

import os
from decimal import Decimal
from typing import Any

from bec.exchanges.ccxt_adapter import CcxtExchangeAdapter
from bec.utils.env_loader import load_env_file


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
        api_key: str | None = None,
        api_secret: str | None = None,
        private_enabled: bool | None = None,
        sizing_buffer_pct: Decimal = Decimal("1"),
        clock=None,
    ):
        load_env_file()
        api_key = os.getenv("KRAKEN_API_KEY", "") if api_key is None else api_key
        api_secret = (
            os.getenv("KRAKEN_API_SECRET", "")
            if api_secret is None
            else api_secret
        )
        if private_enabled is None:
            private_enabled = bool(api_key and api_secret)
        kwargs = {
            "client": client,
            "name": self.name,
            "market_cache_ttl_seconds": market_cache_ttl_seconds,
            "api_key": api_key,
            "api_secret": api_secret,
            "private_enabled": private_enabled,
            "sizing_buffer_pct": sizing_buffer_pct,
        }
        if clock is not None:
            kwargs["clock"] = clock
        super().__init__(self.code, **kwargs)
