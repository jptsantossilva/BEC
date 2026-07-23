import multiprocessing
import time

import pytest

from bec.exchanges.public_rate_limit import SharedPublicRequestThrottle


def _record_shared_request(state_path, output):
    throttle = SharedPublicRequestThrottle(
        state_path,
        min_interval_seconds=0.04,
    )
    with throttle.request_slot():
        output.put(time.monotonic())
        time.sleep(0.01)


def test_shared_throttle_honors_interval_and_cooldown(tmp_path):
    now = [100.0]
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        now[0] += seconds

    throttle = SharedPublicRequestThrottle(
        tmp_path / "kraken-public.lock",
        min_interval_seconds=1.1,
        clock=lambda: now[0],
        sleeper=sleep,
    )

    with throttle.request_slot():
        pass
    with throttle.request_slot() as lease:
        lease.defer(5)
    with throttle.request_slot():
        pass

    assert sleeps == pytest.approx([1.1, 5.0])


def test_shared_throttle_serializes_independent_processes(tmp_path):
    state_path = tmp_path / "kraken-public.lock"
    process_context = multiprocessing.get_context("spawn")
    output = process_context.Queue()
    processes = [
        process_context.Process(
            target=_record_shared_request,
            args=(state_path, output),
        )
        for _ in range(3)
    ]

    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=5)
        assert process.exitcode == 0

    starts = sorted(output.get(timeout=1) for _ in processes)
    gaps = [later - earlier for earlier, later in zip(starts, starts[1:])]
    assert gaps[0] >= 0.045
    assert gaps[1] >= 0.045
