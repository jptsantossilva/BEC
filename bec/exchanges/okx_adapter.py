"""OKX spot adapter with explicitly gated private spot operations."""

from __future__ import annotations

import os
import re
from decimal import Decimal
from typing import Any

from bec.exchanges.base import Balance, ExchangeCapabilities, OrderRequest
from bec.exchanges.ccxt_adapter import (
    CcxtExchangeAdapter,
    PrivateExchangeOperationDisabled,
    _decimal,
)
from bec.utils.env_loader import load_env_file


class OkxAdapter(CcxtExchangeAdapter):
    """OKX spot data with opt-in private access.

    Constructing the public adapter never reads credentials.  Callers must set
    ``private_enabled=True`` to opt into loading a complete credential triplet.
    The application-level production gate remains responsible for deciding
    whether an authenticated production adapter may submit an order.
    """

    code = "okx"
    name = "OKX"
    capabilities = ExchangeCapabilities(
        supports_backtesting=True,
        supports_live_trading=True,
        requires_explicit_live_flags=True,
        supports_signal_schedules=False,
        supports_reconciliation=True,
        uses_gated_live_execution=True,
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
        execution_code: str = "okx",
        sizing_buffer_pct: Decimal = Decimal("5"),
        clock=None,
    ):
        variant = str(adapter_id or "").strip().lower()
        if variant not in {"myokx", "okx"}:
            raise ValueError("OKX adapter variant must be myokx or okx")
        environment = str(execution_environment or "").strip().lower()
        if environment not in {"production", "demo"}:
            raise ValueError("OKX execution environment must be production or demo")
        code = str(execution_code or "okx").strip().lower()
        if code not in {"okx", "okx_demo"}:
            raise ValueError("OKX execution code must be okx or okx_demo")
        if code == "okx_demo" and environment != "demo":
            raise ValueError("OKX demo execution code requires the demo environment")

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
            "sizing_buffer_pct": sizing_buffer_pct,
        }
        if clock is not None:
            kwargs["clock"] = clock
        super().__init__(variant, **kwargs)
        self.code = code
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

    def _require_private_execution(self, operation: str) -> None:
        expected_environment = "demo" if self.code == "okx_demo" else "production"
        if self.code not in {"okx", "okx_demo"} or self.execution_environment != expected_environment:
            raise PrivateExchangeOperationDisabled(
                f"OKX {operation} is unavailable for the configured execution identity"
            )
        self._private_disabled(operation)

    def validate_client_order_id(self, client_order_id: str) -> str:
        """Apply OKX ``clOrdId``'s 32-character alphanumeric constraint."""
        value = str(client_order_id or "").strip()
        if not re.fullmatch(r"[A-Za-z0-9]{1,32}", value):
            raise ValueError(
                "OKX client order ID must be 1-32 ASCII letters or digits"
            )
        return value

    def fetch_balance(self, asset: str | None = None):
        """Read OKX cash availability without losing ``availBal`` in CCXT parsing.

        For a spot order with ``tdMode=cash``, OKX's authoritative available
        amount is ``availBal`` in the trading-account balance details.  Some
        unified-account responses also include ``availEq=0``; CCXT then exposes
        that equity value as ``free`` even when the cash balance is available.
        Preserve the generic result for every other asset/response, but prefer
        the explicit cash fields when OKX provided them.
        """
        self._private_disabled("balances")
        raw = self.client.fetch_balance()
        balances = self._parse_balance_response(raw)
        info = raw.get("info") if isinstance(raw, dict) else None
        data = info.get("data") if isinstance(info, dict) else None
        account = data[0] if isinstance(data, list) and data else None
        details = account.get("details") if isinstance(account, dict) else None
        if isinstance(details, list):
            for detail in details:
                if not isinstance(detail, dict) or detail.get("availBal") in (None, ""):
                    continue
                canonical = self._canonical_asset(detail.get("ccy"))
                if not canonical:
                    continue
                balances[canonical] = Balance(
                    asset=canonical,
                    free=_decimal(detail.get("availBal")),
                    locked=_decimal(detail.get("frozenBal")),
                )
        if asset is not None:
            canonical = self._canonical_asset(asset)
            return balances.get(canonical, Balance(canonical, Decimal("0")))
        return balances

    def fetch_funding_balance(self, asset: str) -> Balance:
        """Read a funding balance for diagnostics; it is never used for orders."""
        self._private_disabled("funding balances")
        raw = self.client.fetch_balance({"type": "funding"})
        balances = self._parse_balance_response(raw)
        canonical = self._canonical_asset(asset)
        return balances.get(canonical, Balance(canonical, Decimal("0")))

    @staticmethod
    def _okx_order_params(client_order_id: str | None = None) -> dict[str, str]:
        params = {"tdMode": "cash"}
        if client_order_id:
            params["clOrdId"] = str(client_order_id)
        return params

    def _parse_order(self, raw, canonical: str):
        """Accept OKX's native client id when CCXT has not normalized it yet."""
        normalized = dict(raw)
        info = normalized.get("info") or {}
        if not normalized.get("clientOrderId"):
            normalized["clientOrderId"] = (
                normalized.get("clOrdId") or info.get("clOrdId") or None
            )
        return super()._parse_order(normalized, canonical)

    def validate_order(self, request: OrderRequest):
        validation = super().validate_order(request)
        market = self._market(request.symbol)
        if market.contract or not market.spot or market.market_type != "spot":
            return type(validation)(
                False,
                (*validation.errors, "OKX demo execution requires an active spot cash market"),
                validation.normalized_amount,
                validation.normalized_price,
                validation.estimated_cost,
            )
        return validation

    def create_market_buy(
        self,
        symbol: str,
        *,
        amount: Decimal | None = None,
        quote_amount: Decimal | None = None,
        client_order_id: str | None = None,
    ):
        self._require_private_execution("market buys")
        if amount is not None or quote_amount is None:
            raise ValueError("OKX market buys require an explicit quote_amount only")
        canonical = self.normalize_symbol(symbol)
        quote_amount = Decimal(str(quote_amount))
        validation = self.validate_order(
            OrderRequest(canonical, "buy", quote_amount=quote_amount)
        )
        if not validation.valid:
            raise ValueError("; ".join(validation.errors))
        create_with_cost = getattr(self.client, "create_market_buy_order_with_cost", None)
        if not callable(create_with_cost):
            raise PrivateExchangeOperationDisabled(
                "OKX demo client does not support quote-cost market buys"
            )
        raw = create_with_cost(
            canonical,
            float(quote_amount),
            self._okx_order_params(client_order_id),
        )
        return self._parse_order(raw, canonical)

    def create_market_sell(
        self,
        symbol: str,
        amount: Decimal,
        *,
        client_order_id: str | None = None,
    ):
        self._require_private_execution("market sells")
        canonical = self.normalize_symbol(symbol)
        amount = self.normalize_amount(canonical, Decimal(str(amount)))
        ticker = self.fetch_ticker(canonical)
        validation = self.validate_order(
            OrderRequest(canonical, "sell", amount=amount, price=ticker.bid or ticker.last)
        )
        if not validation.valid:
            raise ValueError("; ".join(validation.errors))
        raw = self.client.create_order(
            canonical,
            "market",
            "sell",
            float(amount),
            None,
            self._okx_order_params(client_order_id),
        )
        return self._parse_order(raw, canonical)

    def create_limit_sell_ioc(
        self,
        symbol: str,
        amount: Decimal,
        price: Decimal,
        *,
        client_order_id: str | None = None,
    ):
        """Submit a demo-only spot limit sell with immediate-or-cancel semantics.

        CCXT maps ``optimal_limit_ioc`` to a market order for spot instruments,
        so it must not be used here.  A regular limit order plus ``IOC`` is
        retained as a genuine price-bounded spot submission.
        """
        self._require_private_execution("limit IOC sells")
        canonical = self.normalize_symbol(symbol)
        amount = self.normalize_amount(canonical, Decimal(str(amount)))
        price = self.normalize_price(canonical, Decimal(str(price)))
        validation = self.validate_order(
            OrderRequest(canonical, "sell", amount=amount, price=price)
        )
        if not validation.valid:
            raise ValueError("; ".join(validation.errors))
        params = self._okx_order_params(client_order_id)
        params["timeInForce"] = "IOC"
        raw = self.client.create_order(
            canonical,
            "limit",
            "sell",
            float(amount),
            float(price),
            params,
        )
        return self._parse_order(raw, canonical)

    def fetch_order(self, exchange_order_id: str, symbol: str):
        self._require_private_execution("order lookup")
        canonical = self.normalize_symbol(symbol)
        raw = self.client.fetch_order(
            str(exchange_order_id), canonical, self._okx_order_params()
        )
        return self._parse_order(raw, canonical)

    def fetch_order_by_client_id(self, client_order_id: str, symbol: str):
        self._require_private_execution("order lookup")
        canonical = self.normalize_symbol(symbol)
        client_order_id = self.validate_client_order_id(client_order_id)
        params = self._okx_order_params(client_order_id)
        direct_lookup = getattr(self.client, "fetch_order_by_client_order_id", None)
        if callable(direct_lookup):
            raw = direct_lookup(client_order_id, canonical, params)
            if raw:
                return self._parse_order(raw, canonical)
        methods = [
            getattr(self.client, "fetch_open_orders", None),
            getattr(self.client, "fetch_closed_orders", None),
        ]
        if not any(callable(method) for method in methods):
            methods.append(getattr(self.client, "fetch_orders", None))
        for method in methods:
            if not callable(method):
                continue
            try:
                orders = method(canonical, params=params)
            except ccxt.NotSupported:
                continue
            for raw in orders or ():
                if str(raw.get("clientOrderId") or raw.get("clOrdId") or "") == client_order_id:
                    return self._parse_order(raw, canonical)
        return None

    def cancel_order(self, exchange_order_id: str, symbol: str):
        self._require_private_execution("order cancellation")
        canonical = self.normalize_symbol(symbol)
        raw = self.client.cancel_order(
            str(exchange_order_id), canonical, self._okx_order_params()
        )
        return self._parse_order(raw, canonical)
