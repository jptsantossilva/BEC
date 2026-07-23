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
    """Serialize and pace requests made by independent application processes."""

    def __init__(
        self,
        state_path: str | Path,
        *,
        min_interval_seconds: float,
        clock: Callable[[], float] = time.time,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.state_path = Path(state_path)
        self.min_interval_seconds = max(float(min_interval_seconds), 0.0)
        self._clock = clock
        self._sleep = sleeper

    @staticmethod
    def _read_next_allowed(state_file) -> float:
        state_file.seek(0)
        raw = state_file.read().strip()
        if not raw:
            return 0.0
        try:
            payload = json.loads(raw)
            return max(float(payload.get("next_allowed_at", 0.0)), 0.0)
        except (TypeError, ValueError, json.JSONDecodeError):
            return 0.0

    @staticmethod
    def _write_next_allowed(state_file, next_allowed_at: float) -> None:
        state_file.seek(0)
        state_file.truncate()
        json.dump(
            {"next_allowed_at": float(next_allowed_at)},
            state_file,
            separators=(",", ":"),
        )
        state_file.flush()

    @contextmanager
    def request_slot(self) -> Iterator[SharedRequestLease]:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        lease = SharedRequestLease()
        with self.state_path.open("a+", encoding="utf-8") as state_file:
            fcntl.flock(state_file.fileno(), fcntl.LOCK_EX)
            try:
                now = float(self._clock())
                next_allowed_at = self._read_next_allowed(state_file)
                wait_seconds = max(next_allowed_at - now, 0.0)
                if wait_seconds:
                    self._sleep(wait_seconds)

                try:
                    yield lease
                finally:
                    completed_at = float(self._clock())
                    delay = max(
                        self.min_interval_seconds,
                        lease.cooldown_seconds,
                    )
                    self._write_next_allowed(state_file, completed_at + delay)
            finally:
                fcntl.flock(state_file.fileno(), fcntl.LOCK_UN)
