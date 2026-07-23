import requests
import os
import sys
import logging
from html import escape as _html_escape

from bec.utils.env_loader import load_env_file
import bec.utils.database as database
# from bec.utils.database import get_setting

load_env_file(override=True)

# log file to store error messages
log_filename = "main.log"
class _ExchangeLogFilter(logging.Filter):
    def filter(self, record):
        record.exchange_id = database.get_active_exchange_log_identity()
        return True


logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s [exchange_id=%(exchange_id)s] %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p -')
for _handler in logging.getLogger().handlers:
    _handler.addFilter(_ExchangeLogFilter())

# get settings
# get trade_against to know which telegram bots to use (USDT/USDC or BTC)
# trade_against = get_setting("trade_against")
trade_against = database.get_setting( "trade_against")
    
# Check if bot_prefix exists in config, otherwise assign default value
bot_prefix = database.get_setting( "bot_prefix")

# emoji
EMOJI_START = u'\U000025B6'
EMOJI_STOP = u'\U000023F9'
EMOJI_WARNING = u'\U000026A0'
EMOJI_ENTER_TRADE = u'\U0001F91E' # crossfingers
EMOJI_EXIT_TRADE = u'\U0001F91E' # crossfingers
EMOJI_TRADE_WITH_PROFIT = u'\U0001F44D' # thumbs up
EMOJI_TRADE_WITH_LOSS = u'\U0001F44E' # thumbs down
# EMOJI_INFORMATION = u'\U00002139'
EMOJI_INFORMATION = u'\U0001F4E2'
EMOJI_BULL = u'\U0001F402' # bull market
EMOJI_BEAR = u'\U0001F43B' # bear market
EMOJI_PASSWORD_RESET = u'\U0001F511' # key

telegram_chat_id = ""
telegram_token_closed_position = ""
telegram_token_errors = ""
telegram_token_main = ""
telegram_token_signals = ""

# telegram timeout 5 seg
telegram_timeout = 5

# telegram messages prefix to identify the process sending the message
telegram_prefix_market_phases_sl = "MKT - "
telegram_prefix_market_phases_ml = "MKT\n"

telegram_prefix_signals_sl = "SGN - "
telegram_prefix_signals_ml = "SGN\n"

telegram_prefix_errors_sl = "ERR - "
telegram_prefix_errors_ml = "ERR\n"

telegram_prefix_bot_1d_sl = "1D "
telegram_prefix_bot_1d_ml = "1D\n"

telegram_prefix_bot_4h_sl = "4h "
telegram_prefix_bot_4h_ml = "4h\n"

telegram_prefix_bot_1h_sl = "1h "
telegram_prefix_bot_1h_ml = "1h\n"

# Controlled OKX demo validations are not strategy timeframes. Keep their
# notifications explicit without pretending they came from a scheduled bot.
telegram_prefix_bot_manual_demo_sl = "DEMO "
telegram_prefix_bot_manual_demo_ml = "DEMO\n"

# Message classes used by callers to keep Telegram channels focused:
# routine: per-cycle status and summaries, action: order attempts/executions,
# warning/error: exceptional conditions, signal: actionable signal alerts,
# closed_position: realized PnL events mirrored to the closed-position bot.
MESSAGE_CLASS_ROUTINE = "routine"
MESSAGE_CLASS_ACTION = "action"
MESSAGE_CLASS_WARNING = "warning"
MESSAGE_CLASS_ERROR = "error"
MESSAGE_CLASS_SIGNAL = "signal"
MESSAGE_CLASS_CLOSED_POSITION = "closed_position"

def read_env_var():
    # environment variables
    
    global telegram_chat_id
    global telegram_token_closed_position
    global telegram_token_errors
    global telegram_token_main
    global telegram_token_signals

    try:
        telegram_chat_id = os.environ.get('telegram_chat_id')
        telegram_token_closed_position = os.environ.get('telegram_token_closed_positions') 
        telegram_token_errors = os.environ.get('telegram_token_errors')
        telegram_token_main = os.environ.get('telegram_token_main')
        telegram_token_signals = os.environ.get('telegram_token_signals')

    except KeyError as e: 
        msg = sys._getframe(  ).f_code.co_name+" - "+repr(e)
        print(msg)
        logging.exception(msg)

# fulfill telegram vars
read_env_var()

def remove_chars_exceptions(string, parse_mode: str | None = "HTML"):
    """Sanitize outgoing text according to parse mode."""
    if parse_mode == "HTML":
        return _html_escape(string)
    return string

def get_telegram_token() -> str:
    telegram_token = telegram_token_main
    return telegram_token

def get_telegram_prefix(bot, multi_line=False):
    if bot == "1h":
        result = telegram_prefix_bot_1h_ml if multi_line else telegram_prefix_bot_1h_sl
    elif bot == "4h":
        result = telegram_prefix_bot_4h_ml if multi_line else telegram_prefix_bot_4h_sl
    elif bot == "1d":
        result = telegram_prefix_bot_1d_ml if multi_line else telegram_prefix_bot_1d_sl
    elif bot == "manual_demo":
        result = (
            telegram_prefix_bot_manual_demo_ml
            if multi_line
            else telegram_prefix_bot_manual_demo_sl
        )
    else:
        raise ValueError(f"Invalid bot type: {bot}")
    
    return result

def _format_optional_lines(fields: list[tuple[str, object]]) -> list[str]:
    lines = []
    for label, value in fields:
        if value in (None, ""):
            continue
        lines.append(f"{label}: {value}")
    return lines

def format_trade_event(
    *,
    action: str,
    symbol: str,
    timeframe: str,
    strategy: str,
    unit_price,
    quantity,
    notional_value,
    reason: str = "",
    pnl_perc=None,
    pnl_value=None,
    entry_price=None,
    duration: str = "",
    open_positions: str = "",
    exchange: str = "",
    environment: str = "",
    client_order_id: str = "",
    order_status: str = "",
    fees: str = "",
    reconciliation: str = "",
):
    action = str(action or "").upper()
    lines = [
        f"{timeframe} {action} {symbol}",
        f"Strategy: {strategy}",
    ]
    lines.extend(
        _format_optional_lines(
            [
                ("Reason", reason),
                ("Entry", entry_price),
                ("Price", unit_price),
                ("Qty", quantity),
                (trade_against, notional_value),
                ("PnL%", pnl_perc),
                (f"PnL {trade_against}", pnl_value),
                ("Held", duration),
                ("Open positions", open_positions),
                ("Exchange", exchange),
                ("Environment", environment),
                ("Client order", client_order_id),
                ("Order status", order_status),
                ("Fees", fees),
                ("Reconciliation", reconciliation),
            ]
        )
    )
    return "\n".join(lines)

def send_trade_event(
    *,
    telegram_token,
    telegram_prefix: str,
    emoji,
    action: str,
    symbol: str,
    timeframe: str,
    strategy: str,
    unit_price,
    quantity,
    notional_value,
    reason: str = "",
    pnl_perc=None,
    pnl_value=None,
    entry_price=None,
    duration: str = "",
    open_positions: str = "",
    exchange: str = "",
    environment: str = "",
    client_order_id: str = "",
    order_status: str = "",
    fees: str = "",
    reconciliation: str = "",
):
    prefix_label = str(telegram_prefix or "").strip()
    timeframe_label = str(timeframe or "").strip()
    effective_prefix = "" if prefix_label == timeframe_label else telegram_prefix
    msg = effective_prefix + format_trade_event(
        action=action,
        symbol=symbol,
        timeframe=timeframe,
        strategy=strategy,
        unit_price=unit_price,
        quantity=quantity,
        notional_value=notional_value,
        reason=reason,
        pnl_perc=pnl_perc,
        pnl_value=pnl_value,
        entry_price=entry_price,
        duration=duration,
        open_positions=open_positions,
        exchange=exchange,
        environment=environment,
        client_order_id=client_order_id,
        order_status=order_status,
        fees=fees,
        reconciliation=reconciliation,
    )
    print(msg)
    send_telegram_message(telegram_token, emoji, msg)
    if emoji in [EMOJI_TRADE_WITH_PROFIT, EMOJI_TRADE_WITH_LOSS]:
        send_telegram_message(telegram_token_closed_position, emoji, msg)

def format_error_event(
    *,
    action: str,
    symbol: str = "",
    timeframe: str = "",
    strategy: str = "",
    reason: str = "",
    impact: str = "",
    next_step: str = "",
    exception=None,
):
    lines = ["Operational issue"]
    lines.extend(
        _format_optional_lines(
            [
                ("Action", action),
                ("Symbol", symbol),
                ("Timeframe", timeframe),
                ("Strategy", strategy),
                ("Reason", reason),
                ("Impact", impact),
                ("Next", next_step),
                ("Exception", repr(exception) if exception is not None else ""),
            ]
        )
    )
    return "\n".join(lines)

def send_error_event(
    *,
    action: str,
    symbol: str = "",
    timeframe: str = "",
    strategy: str = "",
    reason: str = "",
    impact: str = "",
    next_step: str = "",
    exception=None,
    main_token=None,
    main_prefix: str = "",
    notify_main: bool = True,
):
    detail = format_error_event(
        action=action,
        symbol=symbol,
        timeframe=timeframe,
        strategy=strategy,
        reason=reason,
        impact=impact,
        next_step=next_step,
        exception=exception,
    )
    print(detail)
    send_telegram_message(telegram_token_errors, "", telegram_prefix_errors_sl + detail)
    if notify_main and main_token:
        short = f"{main_prefix}{timeframe + ' ' if timeframe else ''}{action} warning. See Errors channel."
        send_telegram_message(main_token, EMOJI_INFORMATION, short)


def _log_telegram_request_error(
    operation: str,
    category: str,
    error: requests.exceptions.RequestException,
) -> None:
    """Log a Telegram request failure without exposing the token-bearing URL."""
    error_type = type(error).__name__
    status_code = getattr(getattr(error, "response", None), "status_code", None)
    status_detail = f", status={status_code}" if status_code is not None else ""
    message = f"{operation} - {category}: {error_type}{status_detail}"
    print(message)
    logging.error(message)


def _send_telegram_request(
    url: str,
    *,
    operation: str,
    params=None,
    data=None,
    files=None,
):
    """POST to Telegram with consistent, token-safe request error handling."""
    request_kwargs = {"timeout": telegram_timeout}
    if params is not None:
        request_kwargs["params"] = params
    if data is not None:
        request_kwargs["data"] = data
    if files is not None:
        request_kwargs["files"] = files

    try:
        response = requests.post(url, **request_kwargs)
        response.raise_for_status()
        return response
    except requests.exceptions.HTTPError as error:
        _log_telegram_request_error(operation, "HTTP error", error)
    except requests.exceptions.ConnectionError as error:
        _log_telegram_request_error(operation, "Connection error", error)
    except requests.exceptions.Timeout as error:
        _log_telegram_request_error(operation, "Timeout", error)
    except requests.exceptions.RequestException as error:
        _log_telegram_request_error(operation, "Request error", error)
    return None


def send_telegram_message(
    telegram_token,
    emoji,
    msg,
    *,
    include_prefix: bool = True,
    sanitize: bool = True,
    parse_mode: str | None = "HTML"
):
    """
    Send a message to a Telegram chat.

    Args:
        telegram_token (str): The bot token to use for sending the message.
        emoji (str|None): Emoji prefix for the message (e.g., warning or info icon).
        msg (str): The message body.
        include_prefix (bool): If True, prepend bot_prefix to the message.
                               If False, send the message exactly as provided.
        sanitize (bool): If True, sanitize the text for the selected parse mode.
                         If False, send raw text (important for passwords).
        parse_mode (str|None): Telegram parse mode (e.g., "HTML", "Markdown").
                               If None, sends plain text without formatting.

    Behavior:
        - If the message exceeds Telegram’s 4096 char limit, it is split into parts.
        - By default, messages are wrapped in <pre> tags for monospace formatting.
        - Prefix and sanitization can be disabled for sensitive content like passwords.
    """

    if include_prefix:
        exchange_name = database.get_active_exchange_display_name()
        msg = f"{exchange_name} {msg}"

    if sanitize:
        msg = remove_chars_exceptions(msg, parse_mode=parse_mode)

    prefix_str = (bot_prefix + " - ") if include_prefix else ""

    if emoji:
        msg = emoji + " - " + msg

    max_limit = 4096
    if emoji:
        additional_characters = f"{EMOJI_WARNING} {prefix_str} <pre> </pre>Part [10/99]"
    else:
        additional_characters = f"{prefix_str} <pre> </pre>Part [10/99]"

    num_additional_characters = len(additional_characters)
    max_limit = 4096 - num_additional_characters 

    if len(msg+additional_characters) > max_limit:
        # Split the message into multiple parts
        message_parts = [msg[i:i+max_limit] for i in range(0, len(msg), max_limit)]
        n_parts = len(message_parts)
        for i, part in enumerate(message_parts):
            # print(f"Part [{i+1}/{n_parts}]\n{part}")
        
            lmsg = "<pre>"+prefix_str+" - "+"Part ["+str(i+1)+"/"+str(n_parts)+"]\n"+part+"</pre>"
            
            params = {
            "chat_id": telegram_chat_id,
            "text": lmsg,
            "parse_mode": "HTML",
            }

            # Warnings are mirrored to Errors, but one failed destination must
            # not prevent delivery to the requested destination.
            if emoji == EMOJI_WARNING and telegram_token != telegram_token_errors:
                _send_telegram_request(
                    f"https://api.telegram.org/bot{telegram_token_errors}/sendMessage",
                    operation="send_telegram_message.errors",
                    params=params,
                )

            _send_telegram_request(
                f"https://api.telegram.org/bot{telegram_token}/sendMessage",
                operation="send_telegram_message",
                params=params,
            )
            
    else: # message size < max size 4096
        # Short message fits in one send
        lmsg_plain = (prefix_str + msg) if include_prefix else msg
        lmsg = f"<pre>{lmsg_plain}</pre>" if parse_mode == "HTML" else lmsg_plain

        params = {"chat_id": telegram_chat_id, "text": lmsg}
        if parse_mode:
            params["parse_mode"] = parse_mode

        # Warnings are considered operational issues and are mirrored to Errors.
        # Routine status/debug messages must not use EMOJI_WARNING.
        if emoji == EMOJI_WARNING and telegram_token != telegram_token_errors:
            _send_telegram_request(
                f"https://api.telegram.org/bot{telegram_token_errors}/sendMessage",
                operation="send_telegram_message.errors",
                params=params,
            )

        _send_telegram_request(
            f"https://api.telegram.org/bot{telegram_token}/sendMessage",
            operation="send_telegram_message",
            params=params,
        )

def send_password_only(telegram_token: str, password: str):
    """
    Send a password-only message to Telegram.

    This helper ensures:
      - No bot prefix, no emoji
      - No sanitization (password is sent exactly as generated)
      - No formatting (<pre>, HTML, etc.)
      - User receives a clean message that can be copy-pasted directly

    Args:
        telegram_token (str): The bot token used to send the message.
        password (str): The password to send.
    """

    # Escape only for HTML safety;
    pw_html = _html_escape(password)

    send_telegram_message(
        telegram_token,
        emoji=None,
        msg=pw_html,
        include_prefix=False,
        sanitize=False,
        parse_mode="HTML"
    )

def send_telegram_alert(telegram_token, telegram_prefix, emoji, date, symbol, timeframe, strategy, ordertype, unitValue, amount, trade_against_value, pnlPerc = '', pnl_trade_against = '', exit_reason = ''):
    
    # Convert datetime object to string and truncate milliseconds
    datetime_str = date.strftime('%Y-%m-%d %H:%M:%S')

    lmsg = telegram_prefix + emoji + " " + datetime_str + "\n" + symbol + "\n" + strategy + "\n" + timeframe + "\n" + ordertype + "\n" + "UnitPrice: " + str(unitValue) + "\n" + "Qty: " + str(amount)+ "\n" + trade_against + ": " + str(trade_against_value)
    if pnlPerc != '':
        lmsg = lmsg + "\n"+"PnL%: "+str(round(float(pnlPerc),2)) + "\n"+"PnL "+trade_against+": "+str(float(pnl_trade_against))
    if exit_reason != '':
        lmsg = lmsg + "\n"+"Exit Reason: "+exit_reason

    print(lmsg)

    # To fix the issues with dataframes alignments, the message is sent as HTML and wraped with <pre> tag
    # Text in a <pre> element is displayed in a fixed-width font, and the text preserves both spaces and line breaks
    # lmsg = "<pre>"+lmsg+"</pre>"
    lmsg = "<pre>"+bot_prefix+" - "+lmsg+"</pre>"

    params = {
    "chat_id": telegram_chat_id,
    "text": lmsg,
    "parse_mode": "HTML",
    }
    
    _send_telegram_request(
        f"https://api.telegram.org/bot{telegram_token}/sendMessage",
        operation="send_telegram_alert",
        params=params,
    )

    # if is a closed position send also to telegram of closed positions
    if emoji in [EMOJI_TRADE_WITH_PROFIT, EMOJI_TRADE_WITH_LOSS]:
        
        params = {
        "chat_id": telegram_chat_id,
        "text": lmsg,
        "parse_mode": "HTML",
        }

        _send_telegram_request(
            f"https://api.telegram.org/bot{telegram_token_closed_position}/sendMessage",
            operation="send_telegram_alert.closed_position",
            params=params,
        )

def send_telegram_photo(telegram_token, file_name):
    
    # get current dir
    cwd = os.getcwd()
    limg = cwd+"/"+file_name
    url = f"https://api.telegram.org/bot{telegram_token}/sendPhoto?chat_id={telegram_chat_id}"

    with open(limg, "rb") as image:
        _send_telegram_request(
            url,
            operation="send_telegram_photo",
            files={"photo": image},
        )

def send_telegram_file(telegram_token, file_name):
    
    # get current dir
    cwd = os.getcwd()
    file = cwd+"/"+file_name
    # print(limg)
    url = f"https://api.telegram.org/bot{telegram_token}/sendDocument"
    
    with open(file, "rb") as document:
        _send_telegram_request(
            url,
            operation="send_telegram_file",
            data={"chat_id": telegram_chat_id},
            files={"document": document},
        )
