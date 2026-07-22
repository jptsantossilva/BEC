import pandas as pd

from pages.trading import _format_realized_detail


def test_realized_detail_keeps_cancelled_order_null_numbers_displayable():
    detail = _format_realized_detail(
        pd.DataFrame(
            [
                {
                    "Bot": "manual_demo",
                    "Symbol": "BTC/USDC",
                    "Buy_Price": 60177.6,
                    "Sell_Price": None,
                    "Buy_Position_Value": 20.0,
                    "Sell_Position_Value": None,
                    "PnL_Perc": None,
                    "PnL_Value": None,
                }
            ]
        )
    )

    assert detail.loc[0, "Buy_Price"] == "60177.60000000"
    assert detail.loc[0, "Sell_Price"] == ""
    assert detail.loc[0, "Sell_Position_Value"] == ""
    assert detail.loc[0, "PnL_Perc"] == ""
    assert detail.loc[0, "PnL_Value"] == ""
