import logging
import sys

import bec.utils.config as config
import bec.utils.telegram as telegram
from bec.utils import telegram_reporting


def run():
    settings = config.load_settings(refresh=True)
    msg = telegram_reporting.format_daily_summary(settings=settings)
    print(msg)
    telegram.send_telegram_message(telegram.telegram_token_main, "", msg)


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logging.exception("Unhandled exception during Telegram daily summary")
        try:
            telegram.send_error_event(
                action="telegram daily summary",
                reason="Unhandled exception",
                impact="Daily summary was not sent.",
                next_step="Check scheduler logs and database availability.",
                exception=e,
                main_token=telegram.telegram_token_main,
            )
        except Exception:
            pass
        sys.exit(1)
