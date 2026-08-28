"""Read-only OKX authentication and reconciliation-access checks."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import ccxt

from bec.exchanges.okx_adapter import OkxAdapter


@dataclass(frozen=True)
class OkxPrivateStatus:
    available: bool
    message: str


def check_okx_private_access(
    exchange: Mapping[str, Any],
    *,
    run_mode: str,
    adapter_factory: Callable[..., OkxAdapter] = OkxAdapter,
) -> OkxPrivateStatus:
    """Authenticate OKX by reading balances and order history.

    This helper deliberately does not use the app-wide adapter or active
    exchange.  It cannot enable an exchange, arm order flags, create an order,
    amend an order, cancel an order, or change application settings. Because
    the local adapter never escapes this function, the read-only check is safe
    in every app run mode and does not need to pause trading.
    """
    del run_mode  # Read-only balance access is independent from order run modes.
    code = str(exchange.get("Code") or exchange.get("code") or "").lower()
    if code != "okx":
        return OkxPrivateStatus(False, "The selected configuration is not OKX production.")

    environment = str(
        exchange.get("Execution_Environment")
        or exchange.get("execution_environment")
        or "production"
    ).lower()
    if environment != "production":
        return OkxPrivateStatus(False, "Only the OKX production identity is supported.")
    adapter = adapter_factory(
        adapter_id=str(exchange.get("Adapter_Id") or exchange.get("adapter_id") or "myokx"),
        private_enabled=True,
    )
    if not adapter.private_enabled:
        missing = ", ".join(adapter.missing_private_credentials)
        return OkxPrivateStatus(
            False,
            f"OKX {environment} credentials are incomplete; configure {missing}.",
        )

    quote_asset = str(
        exchange.get("Quote_Asset") or exchange.get("quote_asset") or "USDC"
    ).upper()
    try:
        balance = adapter.fetch_balance(quote_asset)
        funding_balance = adapter.fetch_funding_balance(quote_asset)
        adapter.check_reconciliation_access()
    except ccxt.InvalidNonce:
        return OkxPrivateStatus(
            False,
            "OKX rejected the request because of a clock or nonce error. Verify system time "
            "synchronization and retry the read-only check.",
        )
    except (ccxt.AuthenticationError, ccxt.PermissionDenied):
        return OkxPrivateStatus(
            False,
            "OKX authentication or permission check failed. Verify the key, secret, "
            "passphrase, read permission, IP allowlist, and regional API variant.",
        )
    except (
        ccxt.NetworkError,
        ccxt.RequestTimeout,
        ccxt.ExchangeNotAvailable,
        ccxt.DDoSProtection,
    ):
        return OkxPrivateStatus(
            False,
            "OKX is unreachable for the read-only private check. Verify network access, "
            "service availability, and the selected regional API variant.",
        )
    except Exception as exc:
        # Do not surface exchange exception text: it may contain request context.
        return OkxPrivateStatus(
            False,
            f"OKX read-only private check failed ({type(exc).__name__}).",
        )

    return OkxPrivateStatus(
        True,
        f"OKX production ({adapter.adapter_id}) balance and order-history access succeeded for "
        f"{quote_asset} (free={balance.free}, locked={balance.locked}). Funding balance is "
        f"free={funding_balance.free}, locked={funding_balance.locked}; it is diagnostic only "
        f"and cannot fund an order until manually transferred by the operator. No order was "
        f"submitted, amended, or canceled.",
    )
