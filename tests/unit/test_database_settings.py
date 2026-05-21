import sqlite3

import bec.utils.database as database


def test_builtin_strategy_template_seed_updates_existing_builtin_rows(tmp_path):
    original_conn = getattr(database._thread_local, "conn", None)
    test_conn = sqlite3.connect(tmp_path / "data.db", check_same_thread=False)
    test_conn.execute(database.sql_create_strategies_table)
    test_conn.execute(
        """
        INSERT INTO Strategies (Id, Name, Type, Status, Definition_JSON)
        VALUES ('ema_cross', 'EMA Cross', 'builtin', 'approved', NULL)
        """
    )
    test_conn.commit()

    database._thread_local.conn = test_conn
    try:
        database.seed_builtin_strategy_templates(test_conn)
        row = test_conn.execute(
            "SELECT Type, Status, Definition_JSON FROM Strategies WHERE Id = 'ema_cross'"
        ).fetchone()
        assert row[0] == "builtin"
        assert row[1] == "approved"
        assert '"engine":"bec_strategy_ast_v2"' in row[2]
    finally:
        test_conn.close()
        if original_conn is None:
            try:
                delattr(database._thread_local, "conn")
            except AttributeError:
                pass
        else:
            database._thread_local.conn = original_conn


def test_builtin_strategy_seed_creates_weekly_btc_strategies(tmp_path):
    original_conn = getattr(database._thread_local, "conn", None)
    test_conn = sqlite3.connect(tmp_path / "data.db", check_same_thread=False)
    test_conn.execute(database.sql_create_strategies_table)
    for statement in database.sql_strategies_add_default_strategies.split(";"):
        if statement.strip():
            test_conn.execute(statement)
    test_conn.commit()

    database._thread_local.conn = test_conn
    try:
        database.seed_builtin_strategy_templates(test_conn)
        rows = test_conn.execute(
            """
            SELECT Id, Type, Status, Backtest_Optimize, Main_Strategy, BTC_Strategy, Definition_JSON
            FROM Strategies
            WHERE Id IN ('bullmarketsupportband', 'wema20')
            ORDER BY Id
            """
        ).fetchall()

        assert [row[0] for row in rows] == ["bullmarketsupportband", "wema20"]
        for row in rows:
            assert row[1] == "builtin"
            assert row[2] == "approved"
            assert row[3] == 0
            assert row[4] == 0
            assert row[5] == 1
            assert '"timeframe":"1w"' in row[6]
    finally:
        test_conn.close()
        if original_conn is None:
            try:
                delattr(database._thread_local, "conn")
            except AttributeError:
                pass
        else:
            database._thread_local.conn = original_conn


def test_builtin_position_snapshot_migration_preserves_executed_take_profits(tmp_path):
    original_conn = getattr(database._thread_local, "conn", None)
    test_conn = sqlite3.connect(tmp_path / "data.db", check_same_thread=False)
    test_conn.execute(database.sql_create_settings_table)
    test_conn.executemany(
        "INSERT INTO Settings (name, value) VALUES (?, ?)",
        [
            ("stop_loss", "7.5"),
            ("atr_trailing_enabled", "false"),
            ("take_profit_enabled", "true"),
            ("take_profits_json", '[{"level":1,"pnl_pct":8,"amount_pct":25},{"level":2,"pnl_pct":12,"amount_pct":50}]'),
        ],
    )
    test_conn.execute(database.sql_create_strategies_table)
    for statement in database.sql_strategies_add_default_strategies.split(";"):
        if statement.strip():
            test_conn.execute(statement)
    database.seed_builtin_strategy_templates(test_conn)
    test_conn.execute(database.sql_create_positions_table)
    test_conn.execute(
        """
        INSERT INTO Positions (
            Bot, Symbol, Position, Strategy_Id, Strategy_Name,
            Strategy_Params_JSON, Take_Profits_JSON
        )
        VALUES ('1h', 'TESTUSDC', 1, 'ema_cross', 'EMA Cross',
                '{"ema_fast":30,"ema_slow":90}', '[{"level":1}]')
        """
    )
    test_conn.commit()

    database._thread_local.conn = test_conn
    try:
        database._ensure_positions_columns(test_conn)
        strategy_params_json, take_profits_json = test_conn.execute(
            """
            SELECT Strategy_Params_JSON, Take_Profits_JSON
            FROM Positions
            WHERE Symbol = 'TESTUSDC'
            """
        ).fetchone()
        params = database.parse_strategy_params(strategy_params_json)
        assert params["engine"] == "bec_strategy_ast_v2"
        assert params["parameters"]["ema_fast"] == 30
        assert params["parameters"]["ema_slow"] == 90
        assert params["definition"]["engine"] == "bec_strategy_ast_v2"
        assert params["risk"] == database.get_strategy_risk("ema_cross")
        assert take_profits_json == '[{"level":1}]'
    finally:
        test_conn.close()
        if original_conn is None:
            try:
                delattr(database._thread_local, "conn")
            except AttributeError:
                pass
        else:
            database._thread_local.conn = original_conn


def test_positions_migration_moves_legacy_take_profit_flags_to_json(tmp_path):
    original_conn = getattr(database._thread_local, "conn", None)
    test_conn = sqlite3.connect(tmp_path / "data.db", check_same_thread=False)
    test_conn.execute(database.sql_create_settings_table)
    test_conn.execute(database.sql_create_strategies_table)
    test_conn.execute(database.sql_create_positions_table)
    for level in range(1, 5):
        test_conn.execute(
            f"ALTER TABLE Positions ADD COLUMN Take_Profit_{level} INTEGER NOT NULL DEFAULT 0"
        )
    test_conn.execute(
        """
        INSERT INTO Positions (
            Bot, Symbol, Position, Strategy_Id, Strategy_Name,
            Take_Profits_JSON, Take_Profit_1, Take_Profit_2,
            Take_Profit_3, Take_Profit_4
        )
        VALUES ('1h', 'LEGACYUSDC', 1, 'ema_cross', 'EMA Cross',
                '[]', 1, 0, 1, 0)
        """
    )
    test_conn.execute(
        """
        INSERT INTO Positions (
            Bot, Symbol, Position, Strategy_Id, Strategy_Name,
            Take_Profits_JSON, Take_Profit_1
        )
        VALUES ('1h', 'JSONUSDC', 1, 'ema_cross', 'EMA Cross',
                '[{"level":2}]', 1)
        """
    )
    test_conn.commit()

    database._thread_local.conn = test_conn
    try:
        database._ensure_positions_columns(test_conn)
        rows = dict(
            test_conn.execute(
                "SELECT Symbol, Take_Profits_JSON FROM Positions"
            ).fetchall()
        )
        assert rows["LEGACYUSDC"] == "[1,3]"
        assert rows["JSONUSDC"] == '[{"level":2}]'

        columns = {
            row[1] for row in test_conn.execute("PRAGMA table_info(Positions)")
        }
        assert "Take_Profit_1" not in columns
        assert "Take_Profit_2" not in columns
        assert "Take_Profit_3" not in columns
        assert "Take_Profit_4" not in columns
    finally:
        test_conn.close()
        if original_conn is None:
            try:
                delattr(database._thread_local, "conn")
            except AttributeError:
                pass
        else:
            database._thread_local.conn = original_conn
