import sqlite3

import pytest

import bec.utils.database as database
from bec.db.exchange_schema import apply_exchange_aware_schema


def _runtime_database(tmp_path):
    connection = sqlite3.connect(tmp_path / "runtime.db", check_same_thread=False)
    connection.executescript(
        f"""
        {database.create_orders_table}
        {database.sql_create_positions_table}
        {database.sql_create_backtesting_results_table}
        {database.sql_create_backtesting_trades_table}
        {database.sql_create_balances_table}
        {database.sql_create_symbols_by_market_phase_table}
        {database.sql_create_locked_values_table}
        {database.sql_create_job_schedules_table}
        {database.sql_create_backtesting_jobs_table}
        {database.sql_create_monte_carlo_jobs_table}
        {database.sql_create_monte_carlo_results_table}
        """
    )
    connection.execute("BEGIN IMMEDIATE")
    apply_exchange_aware_schema(connection, upgraded_install=False)
    connection.execute(
        """
        INSERT INTO Exchanges (Id, Code, Name, Enabled, Is_Default, Trading_Mode)
        VALUES (2, 'second', 'Second', 0, 0, 'spot')
        """
    )
    for name in database.EXCHANGE_DEPENDENT_JOBS:
        connection.execute(
            """
            INSERT INTO Job_Schedules (name, script, cadence, enabled)
            VALUES (?, 'main.py', '1h', 0)
            """,
            (name,),
        )
    connection.commit()
    return connection


def _use_connection(connection):
    original = getattr(database._thread_local, "conn", None)
    database._thread_local.conn = connection
    return original


def _restore_connection(original):
    if original is None:
        try:
            delattr(database._thread_local, "conn")
        except AttributeError:
            pass
    else:
        database._thread_local.conn = original


def test_new_install_activation_enables_jobs_and_candidates_do_not_block(tmp_path):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        connection.execute(
            "INSERT INTO Positions (Symbol, Position, Exchange_Id) VALUES ('BTCUSDC', 0, 1)"
        )
        selected = database.set_active_exchange(1)

        assert selected["code"] == "binance"
        assert connection.execute(
            "SELECT COUNT(*) FROM Job_Schedules WHERE enabled=1"
        ).fetchone()[0] == len(database.EXCHANGE_DEPENDENT_JOBS)
    finally:
        connection.close()
        _restore_connection(original)


@pytest.mark.parametrize("status", ["pending", "open", "partially_filled", "unknown"])
def test_exchange_switch_is_blocked_by_unsettled_orders(tmp_path, status):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(1)
        connection.execute(
            "INSERT INTO Orders (Symbol, Order_Status, Exchange_Id) VALUES ('BTCUSDC', ?, 1)",
            (status,),
        )
        connection.commit()

        with pytest.raises(ValueError, match="unsettled orders"):
            database.set_active_exchange(2)
    finally:
        connection.close()
        _restore_connection(original)


def test_exchange_switch_is_blocked_by_open_position(tmp_path):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(1)
        connection.execute(
            "INSERT INTO Positions (Symbol, Position, Exchange_Id) VALUES ('BTCUSDC', 1, 1)"
        )
        connection.commit()

        with pytest.raises(ValueError, match="open_positions=1"):
            database.set_active_exchange(2)
    finally:
        connection.close()
        _restore_connection(original)


def test_runtime_queries_are_filtered_by_active_exchange(tmp_path):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(1)
        connection.executemany(
            "INSERT INTO Positions (Bot, Symbol, Position, Exchange_Id) VALUES ('1h', ?, 0, ?)",
            [("BTCUSDC", 1), ("ETHEUR", 2)],
        )
        connection.executemany(
            "INSERT INTO Orders (Symbol, Side, Order_Status, Exchange_Id) VALUES (?, 'BUY', 'filled', ?)",
            [("BTCUSDC", 1), ("ETHEUR", 2)],
        )
        connection.commit()

        assert database.get_all_positions_by_bot("1h")["Symbol"].tolist() == [
            "BTCUSDC"
        ]
        assert database.get_all_orders()["Symbol"].tolist() == ["BTCUSDC"]

        database.set_active_exchange(2)
        assert database.get_all_positions_by_bot("1h")["Symbol"].tolist() == [
            "ETHEUR"
        ]
        assert database.get_all_orders()["Symbol"].tolist() == ["ETHEUR"]
    finally:
        connection.close()
        _restore_connection(original)


def test_rankings_balances_backtests_and_queues_are_exchange_isolated(tmp_path):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(1)
        connection.execute(database.sql_create_strategies_table)
        connection.execute(
            "INSERT INTO Strategies (Id, Name) VALUES ('ema', 'EMA')"
        )
        connection.executemany(
            "INSERT INTO Symbols_By_Market_Phase "
            "(Symbol, Rank, Exchange_Id) VALUES (?, 1, ?)",
            [("BTCUSDC", 1), ("ETHEUR", 2)],
        )
        connection.executemany(
            "INSERT INTO Backtesting_Results "
            "(Symbol, Time_Frame, Strategy_Id, Exchange_Id) "
            "VALUES (?, '1h', 'ema', ?)",
            [("BTCUSDC", 1), ("ETHEUR", 2)],
        )
        connection.executemany(
            "INSERT INTO Balances "
            "(Date, Asset, Balance_USD, Exchange_Id) "
            "VALUES ('2026-07-04', ?, 100, ?)",
            [("USDC", 1), ("EUR", 2)],
        )
        connection.executemany(
            "INSERT INTO Backtesting_Jobs "
            "(batch_id, strategy_id, symbol, timeframe, created_at, Exchange_Id) "
            "VALUES ('batch', 'ema', ?, '1h', '2026-07-04', ?)",
            [("BTCUSDC", 1), ("ETHEUR", 2)],
        )
        connection.commit()

        assert database.get_all_symbols_by_market_phase()["Symbol"].tolist() == [
            "BTCUSDC"
        ]
        assert database.get_all_backtesting_results()["Symbol"].tolist() == [
            "BTCUSDC"
        ]
        assert database.get_asset_balances_all_time()["Asset"].tolist() == ["USDC"]
        assert int(database.get_backtesting_job_counts()["count"].sum()) == 1

        database.set_active_exchange(2)
        assert database.get_all_symbols_by_market_phase()["Symbol"].tolist() == [
            "ETHEUR"
        ]
        assert database.get_all_backtesting_results()["Symbol"].tolist() == [
            "ETHEUR"
        ]
        assert database.get_asset_balances_all_time()["Asset"].tolist() == ["EUR"]
        assert int(database.get_backtesting_job_counts()["count"].sum()) == 1
    finally:
        connection.close()
        _restore_connection(original)


def test_candidate_cleanup_does_not_use_other_exchange_rankings(tmp_path):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(1)
        connection.execute(
            "INSERT INTO Positions (Symbol, Position, Exchange_Id) "
            "VALUES ('ONLYOTHER', 0, 1)"
        )
        connection.execute(
            "INSERT INTO Symbols_By_Market_Phase (Symbol, Exchange_Id) "
            "VALUES ('ONLYOTHER', 2)"
        )
        connection.commit()

        database.delete_positions_not_top_rank()

        assert connection.execute(
            "SELECT COUNT(*) FROM Positions "
            "WHERE Symbol='ONLYOTHER' AND Exchange_Id=1"
        ).fetchone()[0] == 0
    finally:
        connection.close()
        _restore_connection(original)


def test_runtime_writes_persist_active_exchange_metadata(tmp_path):
    connection = _runtime_database(tmp_path)
    original = _use_connection(connection)
    try:
        database.set_active_exchange(1)
        database.add_order_buy("order-1", "2026-07-03", "1h", "BTCUSDC", 10, 2)
        database.insert_position("1h", "BTCUSDC")
        database.add_backtesting_results(
            "1h", "BTCUSDC", 1, 1, "2026-01-01", "2026-07-01", -1,
            10, 50, 2, -1, 0.5, "1d", "1h", 1.2, 0.1, 1.0, 0.2, "ema",
        )
        database.add_backtesting_trade(
            "BTCUSDC", "1h", "ema", 1, 2, 10, 11, 1, 10,
            "2026-01-01", "2026-01-02", "1d",
        )
        database.enqueue_backtesting_jobs(
            [{"strategy_id": "ema", "symbol": "BTCUSDC", "timeframe": "1h"}]
        )

        for table in (
            "Orders", "Positions", "Backtesting_Results", "Backtesting_Trades",
            "Backtesting_Jobs",
        ):
            assert connection.execute(
                f'SELECT Exchange_Id, Symbol_Normalized FROM "{table}" LIMIT 1'
            ).fetchone() == (1, "BTC/USDC")
    finally:
        connection.close()
        _restore_connection(original)
