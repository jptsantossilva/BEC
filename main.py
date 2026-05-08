from bec.main import *

if __name__ == "__main__":
    time_frame = read_arguments()
    run_mode = config.load_settings(refresh=True).run_mode

    try:
        apply_arguments(time_frame)
    except ValueError as e:
        logging.error(str(e))
        try:
            telegram.send_telegram_message(
                telegram_token, telegram.EMOJI_WARNING, f"[FATAL] {e}"
            )
        except Exception:
            pass
        sys.exit(2)

    try:
        run(timeframe=time_frame, run_mode=run_mode)
    except Exception as e:
        logging.exception("Unhandled exception during bot run")
        try:
            telegram.send_telegram_message(
                telegram_token,
                telegram.EMOJI_WARNING,
                f"[FATAL] Unhandled exception: {e}",
            )
        except Exception:
            pass
        sys.exit(1)
