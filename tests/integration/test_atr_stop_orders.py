import sqlite3

import bec.utils.database as database


def _orders_columns(db_path: str) -> set[str]:
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute("PRAGMA table_info(Orders)")
        return {row[1] for row in cur.fetchall()}


def _positions_columns(db_path: str) -> set[str]:
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute("PRAGMA table_info(Positions)")
        return {row[1] for row in cur.fetchall()}


def test_positions_has_minimum_atr_runtime_columns(db_path):
    cols = _positions_columns(db_path)
    assert "Highest_Price_Since_Entry" in cols
    assert "Trail_Stop_ATR" in cols
    assert "Atr_Period" not in cols
    assert "Atr_Multiplier" not in cols
    assert "Atr_Activation_PnL" not in cols


def test_add_order_sell_persists_atr_stop_metadata(db_path, test_ids, monkeypatch):
    cols = _orders_columns(db_path)
    required = {
        "Stop_Type",
        "Stop_Trigger_Price",
        "Trail_Stop_ATR_At_Exit",
        "Highest_Price_Since_Entry_At_Exit",
        "Atr_Params_At_Exit",
    }
    assert required.issubset(cols)
    exchange_id = database.get_active_exchange_id(required=True)
    monkeypatch.setattr(
        database,
        "_exchange_symbol_metadata",
        lambda symbol: (
            exchange_id,
            symbol,
            symbol,
            symbol.removesuffix("USDC"),
            "USDC",
        ),
    )

    database.add_order_buy(
        exchange_order_id=test_ids["buy_exchange_order_id"],
        date="2026-01-01 00:00:00",
        bot=test_ids["bot"],
        symbol=test_ids["symbol"],
        price=100.0,
        qty=1.0,
    )

    database.add_order_sell(
        sell_order_id=test_ids["sell_exchange_order_id"],
        buy_order_id=test_ids["buy_exchange_order_id"],
        date="2026-01-01 01:00:00",
        bot=test_ids["bot"],
        symbol=test_ids["symbol"],
        price=95.0,
        qty=1.0,
        exit_reason="ATR Trailing Stop - test",
        sell_percentage=100,
        stop_type="atr_trailing",
        stop_trigger_price=96.5,
        trail_stop_atr_at_exit=96.5,
        highest_price_since_entry_at_exit=104.2,
        atr_params_at_exit='{"period":14,"multiplier":1.8,"activation_pnl":2.0}',
    )

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT Stop_Type, Stop_Trigger_Price, Trail_Stop_ATR_At_Exit,
                   Highest_Price_Since_Entry_At_Exit, Atr_Params_At_Exit
            FROM Orders
            WHERE Exchange_Order_Id = ?
            """,
            (test_ids["sell_exchange_order_id"],),
        ).fetchone()

    assert row is not None
    assert row[0] == "atr_trailing"
    assert abs(float(row[1]) - 96.5) < 1e-12
    assert abs(float(row[2]) - 96.5) < 1e-12
    assert abs(float(row[3]) - 104.2) < 1e-12
    assert '"period":14' in (row[4] or "")
