from datetime import datetime

import utils.database as database
import utils.telegram as telegram


def delete_position(symbol, bot, unit_price: float = 0.0, reason: str = "Symbol delisted from exchange"):
    telegram_token = telegram.get_telegram_token()

    df_pos = database.get_positions_by_bot_symbol_position(bot=bot, symbol=symbol, position=1)

    # get Buy_Order_Id from positions table
    if not df_pos.empty:
        buy_order_id = str(df_pos["Buy_Order_Id"].iloc[0])
        qty = df_pos["Qty"].iloc[0]
    else:
        buy_order_id = str(0)
        qty = 0

    # Get the current date and time
    current_datetime = datetime.now()
    # Format the date and time as 'YYYY-MM-DD HH:MM:SS'
    order_sell_date = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

    fast_ema = 0
    slow_ema = 0

    # add to orders database table
    pnl_value, pnl_perc = database.add_order_sell(
        sell_order_id=0,
        buy_order_id=buy_order_id,
        date=str(order_sell_date),
        bot=bot,
        symbol=symbol,
        price=unit_price,
        qty=qty,
        ema_fast=fast_ema,
        ema_slow=slow_ema,
        exit_reason=reason,
    )

    # update position as closed position
    database.set_position_sell(bot=bot, symbol=symbol)

    # release all locked values from position
    if not df_pos.empty:
        database.release_value(df_pos["Id"].iloc[0])

    # determine the alert type based on the value of pnl_value
    if pnl_value > 0:
        alert_type = telegram.EMOJI_TRADE_WITH_PROFIT
    else:
        alert_type = telegram.EMOJI_TRADE_WITH_LOSS

    telegram_prefix = telegram.get_telegram_prefix(bot)

    order_side = "SELL"
    order_avg_price = unit_price

    # call send_telegram_alert with the appropriate alert type
    telegram.send_telegram_alert(
        telegram_token=telegram_token,
        telegram_prefix=telegram_prefix,
        emoji=alert_type,
        date=current_datetime,
        symbol=symbol,
        timeframe=bot,
        strategy="",
        ordertype=order_side,
        unitValue=order_avg_price,
        amount=qty,
        trade_against_value=order_avg_price * qty,
        pnlPerc=pnl_perc,
        pnl_trade_against=pnl_value,
        exit_reason=reason,
    )
