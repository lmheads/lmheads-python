"""Unit tests for the broker-pull transport adapter.

Covers the message-walk semantics (status.message pickup, dedup against
echoed /respond, multi-message ordering, and rollback on executor
failure). Network is fully mocked — no live broker required.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from lmheads.listen import _Consumer


@pytest.fixture
def consumer():
    """A Consumer with a mocked handler. Tests then mock _tasks_get and
    handler.on_message_send_stream individually as needed."""
    c = _Consumer(http=None, base="https://example", agent_id="a", handler=MagicMock())
    c.handler.task_store.save = AsyncMock()
    return c


async def test_status_message_picked_up_on_first_fetch(consumer):
    """Caller follow-ups land in `status.message` (not `history`) when
    the lmheads server's pushFollowupViaHub fast-path is taken. The
    consumer must surface them on the first tasks/get, not wait for the
    next message to push them into history."""
    full_task = {
        "id": "T1",
        "contextId": "C1",
        "status": {
            "state": "working",
            "message": {"messageId": "U2", "role": "user", "parts": [{"text": "follow-up"}]},
        },
        "history": [
            {"messageId": "U1", "role": "user", "parts": [{"text": "first"}]},
            {"messageId": "A1", "role": "agent", "parts": [{"text": "ack"}]},
        ],
    }
    consumer._tasks_get = AsyncMock(return_value=full_task)
    consumer._processed_user_msgs.add("U1")  # already processed first turn

    seen: list[str] = []

    async def fake_stream(req, ctx):
        seen.append(req.message.message_id)
        if False:
            yield  # async generator

    consumer.handler.on_message_send_stream = fake_stream
    await consumer._process("T1")
    assert seen == ["U2"]


async def test_status_message_already_in_history_not_doubled(consumer):
    """If the same message id is in both history and status.message
    (later-stage of the SDK lifecycle), don't double-process."""
    full_task = {
        "id": "T1", "contextId": "C1",
        "status": {
            "state": "input_required",
            "message": {"messageId": "A2", "role": "agent", "parts": [{"text": "ok"}]},
        },
        "history": [
            {"messageId": "U1", "role": "user", "parts": [{"text": "hi"}]},
            {"messageId": "A2", "role": "agent", "parts": [{"text": "ok"}]},
        ],
    }
    consumer._tasks_get = AsyncMock(return_value=full_task)
    seen: list[str] = []

    async def fake_stream(req, ctx):
        seen.append(req.message.message_id)
        if False:
            yield

    consumer.handler.on_message_send_stream = fake_stream
    await consumer._process("T1")
    assert seen == ["U1"]


async def test_serial_multi_message(consumer):
    """Process every unprocessed user message in chronological order.
    Earlier passes only responded to the latest, which silently dropped
    middle messages when the caller sent rapid-fire."""
    full_task = {
        "id": "T1", "contextId": "C1",
        "status": {"state": "submitted"},
        "history": [
            {"messageId": "U1", "role": "user", "parts": [{"text": "first"}]},
            {"messageId": "U2", "role": "user", "parts": [{"text": "second"}]},
            {"messageId": "U3", "role": "user", "parts": [{"text": "third"}]},
        ],
    }
    consumer._tasks_get = AsyncMock(return_value=full_task)
    seen: list[str] = []

    async def fake_stream(req, ctx):
        seen.append(req.message.message_id)
        if False:
            yield

    consumer.handler.on_message_send_stream = fake_stream
    await consumer._process("T1")
    assert seen == ["U1", "U2", "U3"]


async def test_dedup_on_echo(consumer):
    """The broker echoes our own /respond as a task.update event. Re-
    invoking _process for the same task must NOT re-fire the executor
    on already-processed user messages."""
    full_task = {
        "id": "T1", "contextId": "C1",
        "status": {"state": "submitted"},
        "history": [
            {"messageId": "U1", "role": "user", "parts": [{"text": "hi"}]},
            {"messageId": "A1", "role": "agent", "parts": [{"text": "ok"}]},
        ],
    }
    consumer._tasks_get = AsyncMock(return_value=full_task)
    seen: list[str] = []

    async def fake_stream(req, ctx):
        seen.append(req.message.message_id)
        if False:
            yield

    consumer.handler.on_message_send_stream = fake_stream

    # First pass — process U1.
    await consumer._process("T1")
    assert seen == ["U1"]

    # Second pass (echo of our /respond) — same history, should NOT re-fire.
    await consumer._process("T1")
    assert seen == ["U1"]

    # New user message arrives — should fire on U2 only.
    full_task["history"].append(
        {"messageId": "U2", "role": "user", "parts": [{"text": "more"}]}
    )
    await consumer._process("T1")
    assert seen == ["U1", "U2"]


async def test_rollback_dedup_marker_on_failure(consumer):
    """If the executor raises, remove the message id from the dedup set
    so a retry (resend, reconnect, or later SSE echo) can re-process."""
    full_task = {
        "id": "T1", "contextId": "C1",
        "status": {"state": "submitted"},
        "history": [{"messageId": "U1", "role": "user", "parts": [{"text": "hi"}]}],
    }
    consumer._tasks_get = AsyncMock(return_value=full_task)

    async def fail_stream(req, ctx):
        raise RuntimeError("boom")
        yield

    consumer.handler.on_message_send_stream = fail_stream

    with pytest.raises(RuntimeError, match="boom"):
        await consumer._process("T1")
    assert "U1" not in consumer._processed_user_msgs


def test_dispatch_routes_event_kinds():
    """SSE event kinds → spawn / cancel routing matrix."""
    consumer = _Consumer(http=None, base="", agent_id="", handler=MagicMock())
    spawned: list[str] = []
    cancelled: list[str] = []
    consumer._spawn_processing = lambda tid: spawned.append(tid)  # type: ignore[method-assign]
    consumer._cancel_remote = lambda tid: cancelled.append(tid)  # type: ignore[method-assign]

    consumer._dispatch({"kind": "task.created", "task_id": "T1", "state": "submitted"})
    consumer._dispatch({"kind": "task.update", "task_id": "T2", "state": "working"})
    consumer._dispatch({"kind": "task.update", "task_id": "T3", "state": "canceled"})
    consumer._dispatch({"kind": "task.expired", "task_id": "T4"})
    consumer._dispatch({"kind": "task.update", "task_id": "T5", "state": "completed"})  # skip
    consumer._dispatch({"kind": "snapshot", "tasks": [
        {"ID": "T6", "State": "submitted"},
        {"ID": "T7", "State": "completed"},  # skip
    ]})

    assert spawned == ["T1", "T2", "T6"]
    assert cancelled == ["T3", "T4"]
