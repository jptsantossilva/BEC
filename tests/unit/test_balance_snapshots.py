import sqlite3

import pandas as pd

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

    database.add_balances(first_snapshot)
    database.add_balances(second_snapshot)

    rows = conn.execute(
        "SELECT Date, Asset, Balance_USD FROM Balances ORDER BY Asset"
    ).fetchall()
    assert rows == [
        ("2025-03-25", "BTC", 31680.0),
        ("2025-03-25", "USDC", 1600.0),
    ]


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
