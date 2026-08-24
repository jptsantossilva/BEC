"""Batch entrypoint for due live-trading timeframes."""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import asdict
from datetime import datetime, timezone

import bec.main as live_main
from bec.exchanges.market_data_batch import (
    fetch_recent_ohlcv_batch,
    merge_requirements,
)
from bec.utils import config, database


TIMEFRAME_ORDER = ("1h", "4h", "1d")


def normalize_timeframes(values: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    requested = {str(value).strip() for value in values}
    invalid = sorted(requested - set(TIMEFRAME_ORDER))
    if invalid:
        raise ValueError(
            f"Incorrect time frame: {', '.join(invalid)}. Use one of: "
            + ", ".join(TIMEFRAME_ORDER)
        )
    if not requested:
        raise ValueError("At least one live-trading timeframe is required")
    return tuple(timeframe for timeframe in TIMEFRAME_ORDER if timeframe in requested)


def run_batch(
    timeframes: list[str] | tuple[str, ...],
    *,
    market_data_only: bool = False,
    now_utc: datetime | None = None,
):
    ordered_timeframes = normalize_timeframes(timeframes)
    started = time.perf_counter()
    settings = config.load_settings(refresh=True)

    # The benchmark path must be strictly read-only.  Candidate cleanup is a
    # trading write and therefore only belongs to the normal execution path.
    if not market_data_only:
        database.delete_positions_not_top_rank()

    prepared_cycles = [
        live_main.prepare_trade_cycle(timeframe, settings)
        for timeframe in ordered_timeframes
    ]
    requirements = merge_requirements(
        *(cycle.requirements for cycle in prepared_cycles)
    )
    batch_result = fetch_recent_ohlcv_batch(
        requirements,
        now_utc=now_utc or datetime.now(timezone.utc),
    )

    if market_data_only:
        payload = asdict(batch_result.metrics) if batch_result.metrics else {}
        payload["timeframes"] = list(ordered_timeframes)
        print(json.dumps(payload, sort_keys=True))
        return {
            "timeframes": ordered_timeframes,
            "market_data": batch_result,
            "summaries": {},
        }

    market_data = live_main._TradeCycleMarketData(
        requirements,
        frames=batch_result.frames,
        failures=batch_result.failures,
        allow_network=False,
    )

    # Only public reads happened concurrently.  Every decision, database write
    # and private exchange call below is deliberately sequential.
    for cycle in prepared_cycles:
        live_main.apply_arguments(cycle.timeframe)
        live_main.trade(
            cycle.timeframe,
            settings.run_mode,
            settings=settings,
            send_summary=False,
            prepared_cycle=cycle,
            market_data=market_data,
            execute_sells=True,
            execute_buys=False,
            finalize_market_data=False,
        )
    for cycle in prepared_cycles:
        live_main.apply_arguments(cycle.timeframe)
        live_main.trade(
            cycle.timeframe,
            settings.run_mode,
            settings=settings,
            send_summary=False,
            prepared_cycle=cycle,
            market_data=market_data,
            execute_sells=False,
            execute_buys=True,
            finalize_market_data=False,
        )

    if market_data.failures:
        live_main.apply_arguments(ordered_timeframes[0])
        market_data.send_failure_summary("/".join(ordered_timeframes))
    for cycle in prepared_cycles:
        relevant = set(cycle.requirements) & set(market_data.failures)
        cycle.summary["warnings"] += len(relevant)

    total_seconds = time.perf_counter() - started
    for cycle in prepared_cycles:
        live_main.apply_arguments(cycle.timeframe)
        live_main.send_trade_report(
            cycle.timeframe,
            cycle.summary,
            settings,
            total_seconds,
        )

    logging.info(
        "Live trade batch timeframes=%s total=%.3fs fetch=%.3fs requests=%s "
        "workers=%s failures=%s",
        ",".join(ordered_timeframes),
        total_seconds,
        batch_result.metrics.elapsed_seconds if batch_result.metrics else 0.0,
        batch_result.metrics.logical_requests if batch_result.metrics else 0,
        batch_result.metrics.workers if batch_result.metrics else 0,
        len(market_data.failures),
    )
    return {
        "timeframes": ordered_timeframes,
        "market_data": batch_result,
        "summaries": {
            cycle.timeframe: cycle.summary for cycle in prepared_cycles
        },
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--market-data-only",
        action="store_true",
        help="Fetch and measure public OHLCV without trading writes or orders.",
    )
    parser.add_argument("timeframes", nargs="+")
    return parser


def cli_main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        run_batch(args.timeframes, market_data_only=args.market_data_only)
        return 0
    except Exception as exc:
        logging.exception("Live trade batch failed")
        print(f"Live trade batch failed: {type(exc).__name__}")
        if not args.market_data_only:
            live_main._send_fatal_notification(
                "[FATAL] Live trade batch failed. Check main.log.",
                "live trade batch",
            )
        return 1


if __name__ == "__main__":
    raise SystemExit(cli_main())
