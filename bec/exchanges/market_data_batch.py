"""Exchange-neutral, in-memory OHLCV batch acquisition for live trading."""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Mapping

import pandas as pd

from bec.exchanges.base import ExchangeAdapter
from bec.exchanges.registry import get_public_adapter_for_exchange
from bec.utils import database


logger = logging.getLogger(__name__)

_TIMEFRAME_SECONDS = {
    "1m": 60,
    "15m": 15 * 60,
    "30m": 30 * 60,
    "1h": 60 * 60,
    "4h": 4 * 60 * 60,
    "1d": 24 * 60 * 60,
    "1w": 7 * 24 * 60 * 60,
}


@dataclass(frozen=True)
class OhlcvBatchMetrics:
    exchange_code: str
    logical_requests: int
    symbols: int
    workers: int
    elapsed_seconds: float
    failures: int
    rate_limit_failures: int = 0


@dataclass
class OhlcvBatchResult:
    frames: dict[tuple[str, str], pd.DataFrame] = field(default_factory=dict)
    failures: dict[tuple[str, str], dict] = field(default_factory=dict)
    metrics: OhlcvBatchMetrics | None = None


def _key(symbol: str, timeframe: str) -> tuple[str, str]:
    return str(symbol).strip().upper(), str(timeframe).strip()


def merge_requirements(
    *requirement_sets: Mapping[tuple[str, str], int],
) -> dict[tuple[str, str], int]:
    """Deduplicate requests and retain the largest required candle count."""
    merged: dict[tuple[str, str], int] = {}
    for requirements in requirement_sets:
        for (symbol, timeframe), count in requirements.items():
            request_key = _key(symbol, timeframe)
            merged[request_key] = max(merged.get(request_key, 0), int(count))
    return merged


def _recent_frame(
    adapter: ExchangeAdapter,
    *,
    symbol: str,
    timeframe: str,
    required: int,
    now_utc: datetime,
) -> pd.DataFrame:
    timeframe_seconds = _TIMEFRAME_SECONDS.get(timeframe)
    if timeframe_seconds is None:
        raise ValueError(f"Unsupported recent OHLCV interval: {timeframe}")
    start_date = now_utc - timedelta(seconds=timeframe_seconds * (required + 2))
    frame = adapter.fetch_ohlcv(
        symbol,
        timeframe,
        start_date=start_date,
        end_date=now_utc,
        drop_incomplete=True,
        keep_time_col=False,
    )
    if frame.empty or len(frame) <= required:
        return frame
    return frame.tail(required).copy()


def _partition_symbol_groups(
    requirements: Mapping[tuple[str, str], int],
    workers: int,
) -> list[list[tuple[str, list[tuple[str, int]]]]]:
    by_symbol: dict[str, list[tuple[str, int]]] = {}
    for (symbol, timeframe), required in sorted(requirements.items()):
        by_symbol.setdefault(symbol, []).append((timeframe, int(required)))

    partitions: list[list[tuple[str, list[tuple[str, int]]]]] = [
        [] for _ in range(workers)
    ]
    loads = [0 for _ in range(workers)]
    groups = sorted(by_symbol.items(), key=lambda item: (-len(item[1]), item[0]))
    for symbol, requests in groups:
        target = min(range(workers), key=lambda index: (loads[index], index))
        partitions[target].append((symbol, requests))
        loads[target] += len(requests)
    return partitions


def _fetch_partition(
    adapter: ExchangeAdapter,
    partition: list[tuple[str, list[tuple[str, int]]]],
    now_utc: datetime,
) -> tuple[dict[tuple[str, str], pd.DataFrame], dict[tuple[str, str], dict]]:
    frames: dict[tuple[str, str], pd.DataFrame] = {}
    failures: dict[tuple[str, str], dict] = {}
    # A symbol group stays on one worker, so two timeframes for the same pair
    # can never be in flight concurrently.
    for symbol, requests in partition:
        for timeframe, required in requests:
            request_key = _key(symbol, timeframe)
            try:
                frame = _recent_frame(
                    adapter,
                    symbol=symbol,
                    timeframe=timeframe,
                    required=required,
                    now_utc=now_utc,
                )
            except Exception as exc:  # fail closed per market, keep the batch alive
                cause = exc
                while getattr(cause, "__cause__", None) is not None:
                    cause = cause.__cause__
                logger.warning(
                    "Public OHLCV batch failure for %s %s: %s",
                    symbol,
                    timeframe,
                    type(exc).__name__,
                )
                frame = pd.DataFrame()
                failures[request_key] = {
                    "reason": "public market-data request failed",
                    "required": required,
                    "available": 0,
                    "error_type": type(exc).__name__,
                    "cause_type": type(cause).__name__,
                    "status_code": getattr(cause, "status_code", None),
                }
            if frame.empty and request_key not in failures:
                failures[request_key] = {
                    "reason": "exchange returned no closed candles",
                    "required": required,
                    "available": 0,
                    "error_type": "",
                }
            elif not frame.empty and len(frame) < required:
                failures[request_key] = {
                    "reason": "insufficient closed-candle history",
                    "required": required,
                    "available": len(frame),
                    "error_type": "",
                }
                frame = pd.DataFrame()
            frames[request_key] = frame.copy()
    return frames, failures


def _is_rate_limit_failure(failure: dict) -> bool:
    value = (
        f"{failure.get('reason', '')} {failure.get('error_type', '')} "
        f"{failure.get('cause_type', '')} {failure.get('status_code', '')}"
    ).lower()
    return any(token in value for token in ("rate", "ddos", "429", "418", "50011"))


def fetch_recent_ohlcv_batch(
    requirements: Mapping[tuple[str, str], int],
    *,
    now_utc: datetime | None = None,
    exchange: dict | None = None,
    adapter_factory: Callable[[], ExchangeAdapter] | None = None,
) -> OhlcvBatchResult:
    """Fetch all unique live requirements with bounded public concurrency."""
    started = time.perf_counter()
    merged = merge_requirements(requirements)
    exchange = exchange or database.get_active_exchange(required=True)
    exchange_code = str(exchange.get("code") or "").strip().lower()
    now = now_utc or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)

    if not merged:
        return OhlcvBatchResult(
            metrics=OhlcvBatchMetrics(
                exchange_code=exchange_code,
                logical_requests=0,
                symbols=0,
                workers=0,
                elapsed_seconds=time.perf_counter() - started,
                failures=0,
            )
        )

    factory = adapter_factory or (
        lambda: get_public_adapter_for_exchange(exchange)
    )
    primary = factory()
    worker_count = max(
        1,
        min(
            int(primary.ohlcv_batch_policy.max_workers),
            len({symbol for symbol, _timeframe in merged}),
            3,
        ),
    )
    adapters = [primary]
    try:
        primary.load_markets()
        for _ in range(1, worker_count):
            worker = factory()
            worker.share_market_catalog_from(primary)
            adapters.append(worker)
    except Exception as exc:
        logger.warning(
            "Unable to prepare %s public market workers: %s",
            exchange_code,
            type(exc).__name__,
        )
        failures = {
            request_key: {
                "reason": "public market catalogue unavailable",
                "required": required,
                "available": 0,
                "error_type": type(exc).__name__,
            }
            for request_key, required in merged.items()
        }
        frames = {key: pd.DataFrame() for key in merged}
        elapsed = time.perf_counter() - started
        return OhlcvBatchResult(
            frames=frames,
            failures=failures,
            metrics=OhlcvBatchMetrics(
                exchange_code=exchange_code,
                logical_requests=len(merged),
                symbols=len({symbol for symbol, _timeframe in merged}),
                workers=len(adapters),
                elapsed_seconds=elapsed,
                failures=len(failures),
                rate_limit_failures=sum(
                    _is_rate_limit_failure(failure) for failure in failures.values()
                ),
            ),
        )

    partitions = _partition_symbol_groups(merged, worker_count)
    frames: dict[tuple[str, str], pd.DataFrame] = {}
    failures: dict[tuple[str, str], dict] = {}
    with ThreadPoolExecutor(
        max_workers=worker_count,
        thread_name_prefix=f"{exchange_code}-ohlcv",
    ) as executor:
        futures = [
            executor.submit(_fetch_partition, adapter, partition, now)
            for adapter, partition in zip(adapters, partitions)
            if partition
        ]
        for future in futures:
            partition_frames, partition_failures = future.result()
            frames.update(partition_frames)
            failures.update(partition_failures)

    elapsed = time.perf_counter() - started
    metrics = OhlcvBatchMetrics(
        exchange_code=exchange_code,
        logical_requests=len(merged),
        symbols=len({symbol for symbol, _timeframe in merged}),
        workers=worker_count,
        elapsed_seconds=elapsed,
        failures=len(failures),
        rate_limit_failures=sum(
            _is_rate_limit_failure(failure) for failure in failures.values()
        ),
    )
    logger.info(
        "OHLCV batch exchange=%s symbols=%s requests=%s workers=%s "
        "elapsed=%.3fs failures=%s rate_limit_failures=%s",
        metrics.exchange_code,
        metrics.symbols,
        metrics.logical_requests,
        metrics.workers,
        metrics.elapsed_seconds,
        metrics.failures,
        metrics.rate_limit_failures,
    )
    return OhlcvBatchResult(frames=frames, failures=failures, metrics=metrics)
