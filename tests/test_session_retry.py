"""Unit tests for append-path back-pressure retry — no Aerospike server.

These exercise ``AerospikeSessionService._write_retrying_overload`` in isolation
with a dummy client. The retry is what makes the (now-idempotent) write hot path
resilient to ``DeviceOverload`` and ambiguous ``TimeoutError`` back-pressure.
"""

from __future__ import annotations

import pytest
from aerospike import exception as ae

from adk_aerospike import AerospikeSessionService
from adk_aerospike.sessions.service import _OVERLOAD_MAX_RETRIES


def _service() -> AerospikeSessionService:
    # ensure_indexes=False means __init__ never touches the client.
    return AerospikeSessionService(object(), "test", ensure_indexes=False)


async def test_device_overload_retried_then_succeeds() -> None:
    svc = _service()
    calls = {"n": 0}

    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise ae.DeviceOverload()
        return "ok"

    assert await svc._write_retrying_overload(flaky) == "ok"
    assert calls["n"] == 3


async def test_device_overload_gives_up_after_max() -> None:
    svc = _service()
    calls = {"n": 0}

    def always() -> str:
        calls["n"] += 1
        raise ae.DeviceOverload()

    with pytest.raises(ae.DeviceOverload):
        await svc._write_retrying_overload(always)
    assert calls["n"] == _OVERLOAD_MAX_RETRIES + 1


async def test_timeout_retried_when_idempotent() -> None:
    svc = _service()
    calls = {"n": 0}

    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 2:
            raise ae.TimeoutError()
        return "ok"

    assert await svc._write_retrying_overload(flaky, retry_timeout=True) == "ok"
    assert calls["n"] == 2


async def test_timeout_not_retried_when_opted_out() -> None:
    svc = _service()
    calls = {"n": 0}

    def once() -> str:
        calls["n"] += 1
        raise ae.TimeoutError()

    with pytest.raises(ae.TimeoutError):
        await svc._write_retrying_overload(once, retry_timeout=False)
    assert calls["n"] == 1  # no retry


async def test_other_errors_propagate_without_retry() -> None:
    svc = _service()
    calls = {"n": 0}

    def boom() -> str:
        calls["n"] += 1
        raise ae.RecordTooBig()

    with pytest.raises(ae.RecordTooBig):
        await svc._write_retrying_overload(boom)
    assert calls["n"] == 1  # RecordTooBig is not transient back-pressure
