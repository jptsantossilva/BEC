"""Read-only OKX authentication checks with no order capability."""

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
    """Authenticate an explicitly selected OKX identity by reading balances only.

    This helper deliberately does not use the app-wide adapter or active
    exchange.  It cannot enable an exchange, arm order flags, create an order,
    query an order, or change application settings.
    """
    code = str(exchange.get("Code") or exchange.get("code") or "").lower()
    if code not in {"okx", "okx_demo"}:
        return OkxPrivateStatus(False, "The selected configuration is not an OKX identity.")
    mode = str(run_mode or "").strip().lower()
    # The demo identity can be checked while it is armed for its controlled
    # demo workflow.  This operation remains balance-only in either mode.
    allowed_modes = {"test", "demo"} if code == "okx_demo" else {"test"}
    if mode not in allowed_modes:
        required_mode = "run_mode=test or run_mode=demo" if code == "okx_demo" else "run_mode=test"
        return OkxPrivateStatus(
            False,
            f"OKX read-only checks require {required_mode}; no setting was changed.",
        )

    environment = str(
        exchange.get("Execution_Environment")
        or exchange.get("execution_environment")
        or ("demo" if code == "okx_demo" else "production")
    ).lower()
    adapter = adapter_factory(
        adapter_id=str(exchange.get("Adapter_Id") or exchange.get("adapter_id") or "myokx"),
        execution_environment=environment,
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
            "OKX is unreachable for the read-only balance check. Verify network access, "
            "service availability, and the selected regional API variant.",
        )
    except Exception as exc:
        # Do not surface exchange exception text: it may contain request context.
        return OkxPrivateStatus(
            False,
            f"OKX read-only balance check failed ({type(exc).__name__}).",
        )

    return OkxPrivateStatus(
        True,
        f"OKX {environment} ({adapter.adapter_id}) trading/cash balance access succeeded for "
        f"{quote_asset} (free={balance.free}, locked={balance.locked}). Funding balance is "
        f"free={funding_balance.free}, locked={funding_balance.locked}; it is diagnostic only "
        f"and cannot fund an order until manually transferred by the operator. No order endpoint "
        f"was used.",
    )
