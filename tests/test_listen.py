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


async def test_executor_failure_posts_rejection_and_keeps_consumer_alive(consumer):
    """If the executor raises (proto validation, executor bug, etc.) the
    consumer must NOT re-raise — that would crash the event loop and stop
    processing other tasks. Instead, post a rejected-state update to the
    broker so the caller sees a clean failure, log the exception, and
    keep going. The dedup marker stays set: re-running the same payload
    would just hit the same exception, so retrying is the caller's call.
    """
    full_task = {
        "id": "T1", "contextId": "C1",
        "status": {"state": "submitted"},
        "history": [{"messageId": "U1", "role": "user", "parts": [{"text": "hi"}]}],
    }
    consumer._tasks_get = AsyncMock(return_value=full_task)
    # Stand in for the http client so _send_rejection can POST.
    response_mock = MagicMock()
    response_mock.raise_for_status = MagicMock(return_value=None)
    consumer.http = MagicMock()
    consumer.http.post = AsyncMock(return_value=response_mock)

    async def fail_stream(req, ctx):
        raise RuntimeError("boom")
        yield

    consumer.handler.on_message_send_stream = fail_stream

    # Should NOT raise — the consumer translates the exception into a
    # rejection POST and continues.
    await consumer._process("T1")

    # Rejection was posted to the broker.
    assert consumer.http.post.await_count == 1
    call = consumer.http.post.await_args
    assert "/respond" in call.args[0]
    body = call.kwargs["json"]
    assert body["state"] == "rejected"
    assert body["parts"][0]["kind"] == "text"
    assert "boom" in body["parts"][0]["text"]

    # Dedup marker stays set — same payload would just fail again.
    assert "U1" in consumer._processed_user_msgs


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


# ── _message_proto: data parts ──────────────────────────────────────


def test_message_proto_decodes_data_parts_to_struct_value():
    """Data parts (kind="data") arriving over SSE must round-trip into a
    Part proto with `data` set so the executor can read structured
    input. Earlier the helper only handled "text" parts and silently
    dropped data parts, which left the proto with parts=[] and the
    SDK's required-field validator rejected the request before the
    executor saw it."""
    from google.protobuf.json_format import MessageToDict

    from lmheads.listen import _message_proto

    payload = {
        "messageId": "M1",
        "role": "user",
        "parts": [
            {"kind": "data", "data": {"target": "https://x", "intensity": "passive"}},
        ],
    }

    msg = _message_proto(payload, task_id="T1", context_id="C1", role=2)

    assert len(msg.parts) == 1
    # MessageToDict on a Value flattens the struct_value oneof — the
    # outer wrapper isn't visible in the JSON shape, so we get the
    # inner dict directly. That's the same shape the executor sees
    # when it round-trips part.data, which is what callers depend on.
    decoded = MessageToDict(msg.parts[0].data)
    assert decoded == {
        "target": "https://x",
        "intensity": "passive",
    }


def test_message_proto_handles_mixed_text_and_data_parts():
    """Some callers ship a Markdown explanation alongside the structured
    input. Both must survive the proto round-trip."""
    from lmheads.listen import _message_proto

    payload = {
        "messageId": "M1",
        "role": "user",
        "parts": [
            {"kind": "text", "text": "Audit my staging environment."},
            {"kind": "data", "data": {"target": "https://staging"}},
        ],
    }
    msg = _message_proto(payload, task_id="T1", context_id="C1", role=2)
    assert len(msg.parts) == 2
    assert msg.parts[0].text == "Audit my staging environment."
    assert msg.parts[1].HasField("data")


# ── _format_executor_error ─────────────────────────────────────────


def test_format_executor_error_unfolds_invalid_params_errors():
    """InvalidParamsError carries a structured `data['errors']` list of
    field-level violations; the formatter renders them bullet-by-
    bullet so an LLM caller has something concrete to fix."""
    from lmheads.listen import _format_executor_error

    class FakeInvalidParamsError(Exception):
        message = "Validation failed"
        data = {
            "errors": [
                {"field": "message.parts", "message": "required"},
                {"field": "message.role", "message": "required"},
            ],
        }

    out = _format_executor_error(FakeInvalidParamsError())
    assert "message.parts: required" in out
    assert "message.role: required" in out
    assert "get_agent_card" in out  # caller hint


def test_format_executor_error_falls_back_for_generic_exceptions():
    from lmheads.listen import _format_executor_error

    out = _format_executor_error(RuntimeError("boom"))
    assert out == "RuntimeError: boom"
