from bec.utils import take_profit


def test_normalize_take_profit_levels_accepts_dynamic_json():
    levels = take_profit.normalize_take_profit_levels(
        '[{"level": 1, "pnl_pct": 5, "amount_pct": 20}, {"level": 7, "pct": 30, "size_pct": 40}]'
    )

    assert levels == [
        {"level": 1, "pnl_pct": 5.0, "amount_pct": 20.0},
        {"level": 7, "pnl_pct": 30.0, "amount_pct": 40.0},
    ]


def test_executed_take_profit_levels_round_trip():
    payload = take_profit.dumps_executed_take_profit_levels([3, "1", 3, 0, "bad"])

    assert payload == "[1,3]"
    assert take_profit.parse_executed_take_profit_levels(payload) == {1, 3}


def test_remaining_position_pct_uses_remaining_position():
    levels = [
        {"level": 1, "pnl_pct": 5, "amount_pct": 20},
        {"level": 2, "pnl_pct": 10, "amount_pct": 25},
        {"level": 3, "pnl_pct": 15, "amount_pct": 50},
    ]

    assert take_profit.remaining_position_pct(levels) == [80.0, 60.0, 30.0]
