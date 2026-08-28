from decimal import Decimal

import ccxt
import pytest

import bec.exchanges.okx_adapter as okx_adapter_module
from bec.exchanges.ccxt_adapter import CcxtExchangeAdapter
from bec.exchanges.ccxt_adapter import PrivateExchangeOperationDisabled
from bec.exchanges.okx_adapter import OkxAdapter
from bec.exchanges.okx_private_status import check_okx_private_access


class ReadOnlyOkxClient:
    def __init__(self, *, fail_with=None):
        self.calls = []
        self.fail_with = fail_with

    def fetch_balance(self, params=None):
        self.calls.append(("balance", params))
        if self.fail_with:
            raise self.fail_with
        return {"free": {"USDC": "12.5"}, "used": {"USDC": "1.5"}}

    def fetch_open_orders(self, limit=None):
        self.calls.append(("open_orders", limit))
        return []

    def fetch_closed_orders(self, limit=None):
        self.calls.append(("closed_orders", limit))
        return []


def _private_adapter(*, client=None, **kwargs):
    return OkxAdapter(
        client=client or ReadOnlyOkxClient(),
        adapter_id="myokx",
        api_key="key-value",
        api_secret="secret-value",
        api_passphrase="passphrase-value",
        private_enabled=True,
        **kwargs,
    )


def _exchange():
    return {
        "Code": "okx",
        "Adapter_Id": "myokx",
        "Execution_Environment": "production",
        "Quote_Asset": "USDC",
    }


def test_okx_private_balance_access_requires_a_complete_credential_triplet():
    client = ReadOnlyOkxClient()
    adapter = OkxAdapter(
        client=client,
        api_key="key-value",
        api_secret="",
        api_passphrase="passphrase-value",
        private_enabled=True,
    )

    assert adapter.private_enabled is False
    assert adapter.missing_private_credentials == ("API secret",)
    with pytest.raises(PrivateExchangeOperationDisabled):
        adapter.fetch_balance("USDC")
    assert client.calls == []


def test_okx_maps_the_passphrase_to_ccxt_password(monkeypatch):
    captured = {}

    class FakeExchange:
        def __init__(self, config):
            captured.update(config)

    monkeypatch.setattr(ccxt, "okx", FakeExchange)

    CcxtExchangeAdapter._create_client(
        "okx",
        api_key="key-value",
        api_secret="secret-value",
        api_password="passphrase-value",
    )

    assert captured["apiKey"] == "key-value"
    assert captured["secret"] == "secret-value"
    assert captured["password"] == "passphrase-value"


def test_okx_private_credentials_use_production_environment_only(monkeypatch):
    monkeypatch.setattr(okx_adapter_module, "load_env_file", lambda: None)
    monkeypatch.setenv("OKX_API_KEY", "selected-key")
    monkeypatch.setenv("OKX_API_SECRET", "selected-secret")
    monkeypatch.setenv("OKX_API_PASSPHRASE", "selected-passphrase")
    monkeypatch.setenv("OKX_DEMO_API_KEY", "ignored-key")
    monkeypatch.setenv("OKX_DEMO_API_SECRET", "ignored-secret")
    monkeypatch.setenv("OKX_DEMO_API_PASSPHRASE", "ignored-passphrase")
    client = ReadOnlyOkxClient()
    client_config = {}

    def create_client(*args, **kwargs):
        client_config.update(kwargs)
        return client

    monkeypatch.setattr(
        OkxAdapter,
        "_create_client",
        staticmethod(create_client),
    )

    adapter = OkxAdapter(private_enabled=True)

    assert adapter.private_enabled is True
    assert client_config == {
        "api_key": "selected-key",
        "api_secret": "selected-secret",
        "api_password": "selected-passphrase",
    }
    assert client.calls == []


def test_okx_uses_cash_availability_when_ccxt_reports_zero_unified_equity():
    class UnifiedAccountClient(ReadOnlyOkxClient):
        def fetch_balance(self, params=None):
            self.calls.append(("balance", params))
            return {
                "free": {"USDC": "0"},
                "used": {"USDC": "0"},
                "info": {
                    "data": [
                        {
                            "details": [
                                {
                                    "ccy": "USDC",
                                    "availEq": "0",
                                    "availBal": "5000",
                                    "frozenBal": "0",
                                }
                            ]
                        }
                    ]
                },
            }

    client = UnifiedAccountClient()
    adapter = _private_adapter(client=client)

    balance = adapter.fetch_balance("USDC")

    assert balance.free == Decimal("5000")
    assert balance.locked == Decimal("0")
    assert client.calls == [("balance", None)]


def test_okx_private_status_is_read_only_and_never_exposes_credential_values():
    client = ReadOnlyOkxClient()
    adapter = _private_adapter(client=client)

    status = check_okx_private_access(
        _exchange(),
        run_mode="test",
        adapter_factory=lambda **kwargs: adapter,
    )

    assert status.available is True
    assert "balance and order-history access succeeded" in status.message
    assert client.calls == [
        ("balance", None),
        ("balance", {"type": "funding"}),
        ("open_orders", 1),
        ("closed_orders", 1),
    ]
    assert "key-value" not in status.message
    assert "secret-value" not in status.message
    assert "passphrase-value" not in status.message


@pytest.mark.parametrize("run_mode", ["prod", "live"])
def test_okx_production_private_status_is_read_only_in_every_run_mode(run_mode):
    client = ReadOnlyOkxClient()
    adapter = _private_adapter(client=client)
    status = check_okx_private_access(
        _exchange(),
        run_mode=run_mode,
        adapter_factory=lambda **kwargs: adapter,
    )

    assert status.available is True
    assert "balance and order-history access succeeded" in status.message
    assert client.calls == [
        ("balance", None),
        ("balance", {"type": "funding"}),
        ("open_orders", 1),
        ("closed_orders", 1),
    ]


def test_okx_private_status_sanitizes_authentication_errors():
    client = ReadOnlyOkxClient(
        fail_with=ccxt.AuthenticationError("secret-value must never be shown")
    )
    adapter = _private_adapter(client=client)

    status = check_okx_private_access(
        _exchange(),
        run_mode="test",
        adapter_factory=lambda **kwargs: adapter,
    )

    assert status.available is False
    assert "authentication or permission" in status.message.lower()
    assert "secret-value" not in status.message
    assert client.calls == [("balance", None)]


def test_okx_private_status_reports_clock_and_network_errors_without_request_details():
    clock_adapter = _private_adapter(
        client=ReadOnlyOkxClient(fail_with=ccxt.InvalidNonce("request context"))
    )
    network_adapter = _private_adapter(
        client=ReadOnlyOkxClient(fail_with=ccxt.NetworkError("request context"))
    )

    clock_status = check_okx_private_access(
        _exchange(), run_mode="test", adapter_factory=lambda **kwargs: clock_adapter
    )
    network_status = check_okx_private_access(
        _exchange(), run_mode="test", adapter_factory=lambda **kwargs: network_adapter
    )

    assert clock_status.available is False
    assert "clock or nonce" in clock_status.message
    assert network_status.available is False
    assert "unreachable" in network_status.message
    assert "request context" not in clock_status.message + network_status.message


def test_okx_public_adapter_keeps_all_private_order_operations_disabled():
    adapter = OkxAdapter(
        client=ReadOnlyOkxClient(),
        adapter_id="myokx",
        private_enabled=False,
    )

    for operation in (
        lambda: adapter.create_market_buy("BTC/USDC", quote_amount=Decimal("10")),
        lambda: adapter.create_market_sell("BTC/USDC", Decimal("0.001")),
        lambda: adapter.fetch_order("1", "BTC/USDC"),
        lambda: adapter.fetch_order_by_client_id("client-1", "BTC/USDC"),
        lambda: adapter.cancel_order("1", "BTC/USDC"),
    ):
        with pytest.raises(PrivateExchangeOperationDisabled, match="requires configured private credentials"):
            operation()
