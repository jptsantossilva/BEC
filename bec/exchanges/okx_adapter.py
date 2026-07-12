"""OKX spot adapter with an explicit read-only private boundary."""

from __future__ import annotations

import os
from decimal import Decimal
from typing import Any

from bec.exchanges.base import ExchangeCapabilities
from bec.exchanges.ccxt_adapter import (
    CcxtExchangeAdapter,
    PrivateExchangeOperationDisabled,
)
from bec.utils.env_loader import load_env_file


class OkxAdapter(CcxtExchangeAdapter):
    """OKX spot data with opt-in, balance-only private access.

    Constructing the public adapter never reads credentials.  Callers must set
    ``private_enabled=True`` to opt into loading a complete credential triplet
    for an explicitly requested read-only status check.
    """

    code = "okx"
    name = "OKX"
    capabilities = ExchangeCapabilities(
        supports_backtesting=True,
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
        execution_environment: str = "production",
        api_key: str | None = None,
        api_secret: str | None = None,
        api_passphrase: str | None = None,
        private_enabled: bool = False,
        clock=None,
    ):
        variant = str(adapter_id or "").strip().lower()
        if variant not in {"myokx", "okx"}:
            raise ValueError("OKX adapter variant must be myokx or okx")
        environment = str(execution_environment or "").strip().lower()
        if environment not in {"production", "demo"}:
            raise ValueError("OKX execution environment must be production or demo")

        # A public adapter must remain credential-free even when process
        # variables exist.  An injected client is also a strict test boundary.
        use_environment_credentials = bool(private_enabled) and client is None
        if use_environment_credentials:
            load_env_file()
        prefix = "OKX_DEMO" if environment == "demo" else "OKX"
        api_key = (
            os.getenv(f"{prefix}_API_KEY", "")
            if api_key is None and use_environment_credentials
            else (api_key or "")
        )
        api_secret = (
            os.getenv(f"{prefix}_API_SECRET", "")
            if api_secret is None and use_environment_credentials
            else (api_secret or "")
        )
        api_passphrase = (
            os.getenv(f"{prefix}_API_PASSPHRASE", "")
            if api_passphrase is None and use_environment_credentials
            else (api_passphrase or "")
        )
        self.execution_environment = environment
        self.missing_private_credentials = tuple(
            name
            for name, value in (
                ("API key", api_key),
                ("API secret", api_secret),
                ("API passphrase", api_passphrase),
            )
            if not value
        )
        private_access_ready = (
            bool(private_enabled) and not self.missing_private_credentials
        )
        kwargs = {
            "client": client,
            "name": self.name,
            "market_cache_ttl_seconds": market_cache_ttl_seconds,
            "api_key": api_key,
            "api_secret": api_secret,
            "api_password": api_passphrase,
            "private_enabled": private_access_ready,
            "sizing_buffer_pct": Decimal("1"),
        }
        if clock is not None:
            kwargs["clock"] = clock
        super().__init__(variant, **kwargs)
        self.code = "okx"
        self.adapter_id = variant
        if private_access_ready and environment == "demo":
            self._enable_demo_sandbox()

    def _enable_demo_sandbox(self) -> None:
        """Select CCXT demo mode before any private API call is possible."""
        set_sandbox_mode = getattr(self.client, "set_sandbox_mode", None)
        if not callable(set_sandbox_mode):
            raise PrivateExchangeOperationDisabled(
                "OKX demo read-only checks require a CCXT client with sandbox support"
            )
        set_sandbox_mode(True)

    def _orders_disabled(self, operation: str) -> None:
        raise PrivateExchangeOperationDisabled(
            f"OKX {operation} is unavailable until the dedicated execution PR"
        )

    def create_market_buy(self, *args, **kwargs):
        self._orders_disabled("market buys")

    def create_market_sell(self, *args, **kwargs):
        self._orders_disabled("market sells")

    def fetch_order(self, *args, **kwargs):
        self._orders_disabled("order queries")

    def fetch_order_by_client_id(self, *args, **kwargs):
        self._orders_disabled("client-order queries")

    def cancel_order(self, *args, **kwargs):
        self._orders_disabled("order cancellation")
