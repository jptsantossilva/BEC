"""Kraken public market-data adapter backed by CCXT."""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path
from typing import Any

from bec.exchanges.ccxt_adapter import CcxtExchangeAdapter
from bec.exchanges.public_rate_limit import SharedPublicRequestThrottle
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
        public_request_throttle: SharedPublicRequestThrottle | None = None,
        sleeper=None,
        random_uniform=None,
    ):
        # An injected client is a test/integration boundary. Do not silently
        # import process credentials into it unless the caller explicitly asks.
        use_environment_credentials = client is None
        if use_environment_credentials:
            load_env_file()
        api_key = (
            os.getenv("KRAKEN_API_KEY", "")
            if api_key is None and use_environment_credentials
            else (api_key or "")
        )
        api_secret = (
            os.getenv("KRAKEN_API_SECRET", "")
            if api_secret is None and use_environment_credentials
            else (api_secret or "")
        )
        if private_enabled is None:
            private_enabled = bool(api_key and api_secret)
        if public_request_throttle is None and client is None:
            project_root = Path(__file__).resolve().parents[2]
            public_request_throttle = SharedPublicRequestThrottle(
                project_root
                / "static"
                / "backtest_results"
                / "rate_limits"
                / "kraken-public.lock",
                min_interval_seconds=1.1,
            )
        kwargs = {
            "client": client,
            "name": self.name,
            "market_cache_ttl_seconds": market_cache_ttl_seconds,
            "api_key": api_key,
            "api_secret": api_secret,
            "private_enabled": private_enabled,
            "sizing_buffer_pct": sizing_buffer_pct,
            "public_request_throttle": public_request_throttle,
            "public_read_retry_enabled": True,
        }
        if clock is not None:
            kwargs["clock"] = clock
        if sleeper is not None:
            kwargs["sleeper"] = sleeper
        if random_uniform is not None:
            kwargs["random_uniform"] = random_uniform
        super().__init__(self.code, **kwargs)
