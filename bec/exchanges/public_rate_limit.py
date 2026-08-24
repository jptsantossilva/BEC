"""Cross-process coordination for public exchange API requests."""

from __future__ import annotations

import fcntl
import json
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterator


class SharedRequestLease:
    """One serialized request slot with an optional shared cooldown."""

    def __init__(self) -> None:
        self._cooldown_seconds = 0.0

    def defer(self, seconds: float) -> None:
        self._cooldown_seconds = max(self._cooldown_seconds, float(seconds), 0.0)

    @property
    def cooldown_seconds(self) -> float:
        return self._cooldown_seconds


class SharedPublicRequestThrottle:
    """Reserve paced request starts across processes without locking I/O."""

    def __init__(
        self,
        state_path: str | Path,
        *,
        min_interval_seconds: float,
        keyed: bool = False,
        clock: Callable[[], float] = time.time,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.state_path = Path(state_path)
        self.min_interval_seconds = max(float(min_interval_seconds), 0.0)
        self.keyed = bool(keyed)
        self._clock = clock
        self._sleep = sleeper

    @staticmethod
    def _read_state(state_file) -> dict[str, float]:
        state_file.seek(0)
        raw = state_file.read().strip()
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
            scopes = payload.get("scopes")
            if isinstance(scopes, dict):
                return {
                    str(key): max(float(value), 0.0)
                    for key, value in scopes.items()
                }
            # Backward compatibility with the original single-value state.
            return {
                "__global__": max(float(payload.get("next_allowed_at", 0.0)), 0.0)
            }
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}

    @staticmethod
    def _write_state(state_file, scopes: dict[str, float]) -> None:
        state_file.seek(0)
        state_file.truncate()
        json.dump(
            {"scopes": scopes},
            state_file,
            separators=(",", ":"),
        )
        state_file.flush()

    def _scope_key(self, scope_key: str | None) -> str:
        if not self.keyed:
            return "__global__"
        value = str(scope_key or "").strip().upper()
        return value or "__global__"

    def _reserve_start(self, scope_key: str) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        while True:
            with self.state_path.open("a+", encoding="utf-8") as state_file:
                fcntl.flock(state_file.fileno(), fcntl.LOCK_EX)
                try:
                    now = float(self._clock())
                    scopes = self._read_state(state_file)
                    next_allowed_at = scopes.get(scope_key, 0.0)
                    wait_seconds = max(next_allowed_at - now, 0.0)
                    if wait_seconds <= 0:
                        cutoff = now - 86400.0
                        scopes = {
                            key: value
                            for key, value in scopes.items()
                            if value >= cutoff or key == scope_key
                        }
                        scopes[scope_key] = now + self.min_interval_seconds
                        self._write_state(state_file, scopes)
                        return
                finally:
                    fcntl.flock(state_file.fileno(), fcntl.LOCK_UN)
            # Do not hold the shared file lock while waiting.  This is
            # essential for Kraken, where unrelated currency pairs have
            # independent public OHLC limits.
            self._sleep(wait_seconds)

    def _apply_cooldown(self, scope_key: str, seconds: float) -> None:
        delay = max(float(seconds), 0.0)
        if delay <= 0:
            return
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        with self.state_path.open("a+", encoding="utf-8") as state_file:
            fcntl.flock(state_file.fileno(), fcntl.LOCK_EX)
            try:
                now = float(self._clock())
                scopes = self._read_state(state_file)
                scopes[scope_key] = max(
                    scopes.get(scope_key, 0.0),
                    now + delay,
                )
                self._write_state(state_file, scopes)
            finally:
                fcntl.flock(state_file.fileno(), fcntl.LOCK_UN)

    @contextmanager
    def request_slot(self, scope_key: str | None = None) -> Iterator[SharedRequestLease]:
        resolved_scope = self._scope_key(scope_key)
        self._reserve_start(resolved_scope)
        lease = SharedRequestLease()
        try:
            yield lease
        finally:
            self._apply_cooldown(resolved_scope, lease.cooldown_seconds)
