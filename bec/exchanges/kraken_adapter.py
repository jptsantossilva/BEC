"""Kraken public market-data adapter backed by CCXT."""

from __future__ import annotations

import os
import re
from decimal import Decimal
from pathlib import Path
from typing import Any

import ccxt

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
    _INVALID_CLIENT_ORDER_ID_ERROR = "egeneral:invalid arguments:cl_ord_id"
    _REGIONAL_TRADING_RESTRICTION = re.compile(
        r"eaccount:invalid permissions:"
        r"(?P<asset>[a-z0-9._-]+)\s+trading restricted for\s+"
        r"(?P<region>[a-z0-9._-]+)",
        re.IGNORECASE,
    )

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

    def validate_client_order_id(self, client_order_id: str) -> str:
        """Apply Kraken's free-text and UUID ``cl_ord_id`` constraints."""
        value = str(client_order_id or "").strip()
        is_free_text = bool(re.fullmatch(r"[A-Za-z0-9_-]{1,18}", value))
        is_short_uuid = bool(re.fullmatch(r"[0-9A-Fa-f]{32}", value))
        is_long_uuid = bool(
            re.fullmatch(
                r"[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-"
                r"[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}",
                value,
            )
        )
        if not (is_free_text or is_short_uuid or is_long_uuid):
            raise ValueError(
                "Kraken client order ID must be up to 18 safe ASCII characters "
                "or a 32/36-character UUID"
            )
        return value

    def is_known_submission_rejection(self, exc: Exception) -> bool:
        if super().is_known_submission_rejection(exc):
            return True
        if not isinstance(exc, ccxt.ExchangeError):
            return False
        return (
            self._INVALID_CLIENT_ORDER_ID_ERROR in str(exc).lower()
            or self.regional_trading_restriction(exc) is not None
        )

    def is_known_persisted_submission_rejection(self, error: str) -> bool:
        return (
            self._INVALID_CLIENT_ORDER_ID_ERROR in str(error).lower()
            or self.regional_trading_restriction(error) is not None
        )

    def regional_trading_restriction(
        self, error: Exception | str
    ) -> dict[str, str] | None:
        """Return the asset and region from a definitive Kraken restriction."""
        match = self._REGIONAL_TRADING_RESTRICTION.search(str(error))
        if match is None:
            return None
        asset = match.group("asset").upper()
        return {
            "asset": self.asset_aliases.get(asset, asset),
            "region": match.group("region").rstrip(".\"']}").upper(),
        }
