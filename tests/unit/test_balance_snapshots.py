import sqlite3
from decimal import Decimal

import ccxt
import pandas as pd
import pytest

from bec.balance_snapshots import create_daily_balance_snapshot
from bec.exchanges.base import Balance, ExchangeCapabilities, MarketInfo, Ticker
import bec.utils.database as database
from bec.db.exchange_schema import apply_exchange_aware_schema


def _use_temp_balances_db(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.execute(database.sql_create_balances_table)
    conn.execute("BEGIN IMMEDIATE")
    apply_exchange_aware_schema(conn, upgraded_install=True)
    conn.commit()
    database._ensure_balances_unique_index(conn)
    monkeypatch.setattr(database._thread_local, "conn", conn, raising=False)
    return conn


def test_add_balances_replaces_existing_snapshot_for_date(monkeypatch):
    conn = _use_temp_balances_db(monkeypatch)
    first_snapshot = pd.DataFrame(
        [
            ["2025-03-25", "BTC", 0.35, 87500.0, 87500.0, 30625.0, 0.35, 0.37],
            ["2025-03-25", "USDC", 1500.0, 1.0, 87500.0, 1500.0, 0.01714285, 0.37],
        ],
        columns=[
            "Date",
            "Asset",
            "Balance",
            "USD_Price",
            "BTC_Price",
            "Balance_USD",
            "Balance_BTC",
            "Total_Balance_BTC",
        ],
    )
    second_snapshot = pd.DataFrame(
        [
            ["2025-03-25", "BTC", 0.36, 88000.0, 88000.0, 31680.0, 0.36, 0.38],
            ["2025-03-25", "USDC", 1600.0, 1.0, 88000.0, 1600.0, 0.01818181, 0.38],
        ],
        columns=first_snapshot.columns,
    )

    database.add_balances(first_snapshot, exchange_id=1)
    database.add_balances(second_snapshot, exchange_id=1)

    rows = conn.execute(
        "SELECT Date, Asset, Balance_USD FROM Balances ORDER BY Asset"
    ).fetchall()
    assert rows == [
        ("2025-03-25", "BTC", 31680.0),
        ("2025-03-25", "USDC", 1600.0),
    ]


def test_add_balances_replaces_only_the_requested_exchange(monkeypatch):
    conn = _use_temp_balances_db(monkeypatch)
    columns = [
        "Date",
        "Asset",
        "Balance",
        "USD_Price",
        "BTC_Price",
        "Balance_USD",
        "Balance_BTC",
        "Total_Balance_BTC",
    ]
    binance = pd.DataFrame(
        [["2026-08-20", "USDC", 100, 1, 60000, 100, 1 / 600, 1 / 600]],
        columns=columns,
    )
    kraken = pd.DataFrame(
        [["2026-08-20", "USDC", 200, 1, 60000, 200, 1 / 300, 1 / 300]],
        columns=columns,
    )
    updated_kraken = pd.DataFrame(
        [["2026-08-20", "USDC", 250, 1, 60000, 250, 1 / 240, 1 / 240]],
        columns=columns,
    )

    database.add_balances(binance, exchange_id=1)
    database.add_balances(kraken, exchange_id=2)
    database.add_balances(updated_kraken, exchange_id=2)

    assert conn.execute(
        "SELECT Exchange_Id, Balance_USD FROM Balances ORDER BY Exchange_Id"
    ).fetchall() == [(1, 100.0), (2, 250.0)]


class _FakeSnapshotAdapter:
    capabilities = ExchangeCapabilities(supports_private_balance_reads=True)
    name = "Fake exchange"

    def __init__(self, code):
        self.code = code
        self.balance_calls = 0
        self.ticker_calls = []
        self.balances = {
            "BTC": Balance("BTC", Decimal("0.1"), Decimal("0.9")),
            "ETH": Balance("ETH", Decimal("2")),
            "USDC": Balance("USDC", Decimal("100")),
            "DOGE": Balance("DOGE", Decimal("20")),
            "UNPRICED": Balance("UNPRICED", Decimal("5")),
            "ZERO": Balance("ZERO", Decimal("0")),
        }
        self.markets = {
            "BTC/USDC": MarketInfo("BTC/USDC", "BTCUSDC", "BTC", "USDC", True),
            "ETH/BTC": MarketInfo("ETH/BTC", "ETHBTC", "ETH", "BTC", True),
            "USDC/DOGE": MarketInfo("USDC/DOGE", "USDCDOGE", "USDC", "DOGE", True),
        }
        self.prices = {
            "BTC/USDC": Decimal("60000"),
            "ETH/BTC": Decimal("0.05"),
            "USDC/DOGE": Decimal("10"),
        }

    def fetch_balance(self):
        self.balance_calls += 1
        return self.balances

    def load_markets(self):
        return self.markets

    def fetch_ticker(self, symbol):
        self.ticker_calls.append(symbol)
        return Ticker(symbol, self.prices[symbol])


@pytest.mark.parametrize(
    "exchange_code,exchange_id",
    [("binance", 1), ("kraken", 2), ("okx", 3), ("okx_demo", 4)],
)
def test_daily_snapshot_values_and_persists_active_exchange_balances(
    monkeypatch, exchange_code, exchange_id
):
    conn = _use_temp_balances_db(monkeypatch)
    adapter = _FakeSnapshotAdapter(exchange_code)

    result = create_daily_balance_snapshot(
        exchange={"id": exchange_id, "code": exchange_code, "quote_asset": "USDC"},
        adapter=adapter,
        snapshot_date="2026-08-20",
    )

    assert result.assets_received == 5
    assert result.assets_stored == 4
    assert result.skipped_assets == ("UNPRICED",)
    assert result.total_usd == Decimal("12102.0")
    assert result.total_btc == Decimal("0.2017")
    assert adapter.ticker_calls.count("BTC/USDC") == 1
    rows = conn.execute(
        """
        SELECT Exchange_Id, Asset, Balance, Balance_USD, Total_Balance_Of_BTC
        FROM Balances ORDER BY Balance_USD DESC
        """
    ).fetchall()
    assert rows == [
        (exchange_id, "BTC", 0.1, 6000.0, pytest.approx(0.2017)),
        (exchange_id, "ETH", 2.0, 6000.0, pytest.approx(0.2017)),
        (exchange_id, "USDC", 100.0, 100.0, pytest.approx(0.2017)),
        (exchange_id, "DOGE", 20.0, 2.0, pytest.approx(0.2017)),
    ]


def test_empty_snapshot_does_not_delete_an_existing_snapshot(monkeypatch):
    conn = _use_temp_balances_db(monkeypatch)
    existing = pd.DataFrame(
        [["2026-08-20", "USDC", 100, 1, 60000, 100, 1 / 600, 1 / 600]],
        columns=[
            "Date",
            "Asset",
            "Balance",
            "USD_Price",
            "BTC_Price",
            "Balance_USD",
            "Balance_BTC",
            "Total_Balance_BTC",
        ],
    )
    database.add_balances(existing, exchange_id=2)
    adapter = _FakeSnapshotAdapter("kraken")
    adapter.balances = {}

    result = create_daily_balance_snapshot(
        exchange={"id": 2, "code": "kraken", "quote_asset": "USDC"},
        adapter=adapter,
        snapshot_date="2026-08-20",
    )

    assert result.assets_stored == 0
    assert conn.execute(
        "SELECT Balance_USD FROM Balances WHERE Exchange_Id=2"
    ).fetchone() == (100.0,)


def test_balance_read_retries_only_transient_failures(monkeypatch):
    _use_temp_balances_db(monkeypatch)
    adapter = _FakeSnapshotAdapter("kraken")
    sleeps = []
    responses = [
        ccxt.RequestTimeout("first"),
        ccxt.ExchangeNotAvailable("second"),
        adapter.balances,
    ]

    def fetch_balance():
        response = responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    adapter.fetch_balance = fetch_balance

    result = create_daily_balance_snapshot(
        exchange={"id": 2, "code": "kraken", "quote_asset": "USDC"},
        adapter=adapter,
        snapshot_date="2026-08-20",
        sleeper=sleeps.append,
    )

    assert result.assets_stored == 4
    assert sleeps == [1.0, 2.0]


def test_ensure_balances_unique_index_deduplicates_existing_rows(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE Balances (
            Id INTEGER PRIMARY KEY,
            Date TEXT,
            Asset TEXT,
            Balance REAL,
            USD_Price REAL,
            BTC_Price REAL,
            Balance_USD REAL,
            Balance_BTC REAL,
            Total_Balance_Of_BTC REAL
        );
        """
    )
    conn.executemany(
        """
        INSERT INTO Balances
        (Date, Asset, Balance, USD_Price, BTC_Price, Balance_USD, Balance_BTC, Total_Balance_Of_BTC)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("2025-03-25", "BTC", 0.35, 87500.0, 87500.0, 30625.0, 0.35, 0.37),
            ("2025-03-25", "BTC", 0.36, 88000.0, 88000.0, 31680.0, 0.36, 0.38),
            ("2025-03-25", "USDC", 1600.0, 1.0, 88000.0, 1600.0, 0.01818181, 0.38),
        ],
    )
    monkeypatch.setattr(database._thread_local, "conn", conn, raising=False)

    database._ensure_balances_unique_index(conn)

    rows = conn.execute(
        "SELECT Date, Asset, Balance_USD FROM Balances ORDER BY Asset"
    ).fetchall()
    assert rows == [
        ("2025-03-25", "BTC", 31680.0),
        ("2025-03-25", "USDC", 1600.0),
    ]
