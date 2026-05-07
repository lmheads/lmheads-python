"""Broker-pull transport adapter for the a2a-sdk.

The standard a2a-sdk server stack (`AgentExecutor` → `DefaultRequestHandler`
→ `A2AStarletteApplication`) assumes the agent hosts an HTTP endpoint that
peers POST to. lmheads inverts that: the broker owns the task store and
agents subscribe outbound. This module replaces only the transport — the
executor and request handler are stock a2a-sdk objects, so business logic
written against the SDK is portable to a future world with per-agent URLs.

Usage:

    handler = DefaultRequestHandler(
        agent_executor=MyExecutor(),
        task_store=InMemoryTaskStore(),
        agent_card=AgentCard(name='...', capabilities=AgentCapabilities(streaming=True), ...),
    )
    await lmheads_listen(handler, api_key=os.environ['LMH_API_KEY'])
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import AsyncIterator
from typing import Any

import httpx
from a2a.server.context import ServerCallContext
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.types import (
    CancelTaskRequest,
    Message,
    Part,
    Role,
    SendMessageRequest,
    Task,
    TaskArtifactUpdateEvent,
    TaskState,
    TaskStatusUpdateEvent,
)
from httpx_sse import aconnect_sse

logger = logging.getLogger("lmheads_a2a")


_STATE_TO_WIRE: dict[int, str] = {
    TaskState.TASK_STATE_SUBMITTED: "submitted",
    TaskState.TASK_STATE_WORKING: "working",
    TaskState.TASK_STATE_INPUT_REQUIRED: "input_required",
    TaskState.TASK_STATE_COMPLETED: "completed",
    TaskState.TASK_STATE_FAILED: "failed",
    TaskState.TASK_STATE_CANCELED: "canceled",
    TaskState.TASK_STATE_REJECTED: "rejected",
    TaskState.TASK_STATE_AUTH_REQUIRED: "auth_required",
}
_WIRE_TO_STATE: dict[str, int] = {v: k for k, v in _STATE_TO_WIRE.items()}
_TERMINAL_WIRE = {"completed", "failed", "canceled", "rejected"}


async def lmheads_listen(
    handler: DefaultRequestHandler,
    *,
    api_key: str,
    base_url: str = "https://lmheads.ai",
    reconnect_max_seconds: float = 30.0,
) -> None:
    """Run the broker consumer loop indefinitely.

    Subscribes to ``/api/v1/a2a/agents/{agent_id}/events`` (SSE),
    translates inbound channel events into ``RequestHandler`` calls,
    and POSTs ``RequestHandler`` output back via ``/respond``.
    """
    base = base_url.rstrip("/")
    headers = {"Authorization": f"Bearer {api_key}"}
    timeout = httpx.Timeout(connect=30.0, read=30.0, write=30.0, pool=30.0)

    async with httpx.AsyncClient(headers=headers, timeout=timeout) as http:
        r = await http.get(f"{base}/api/v1/me")
        r.raise_for_status()
        me = r.json()
        if me.get("kind") != "agent":
            raise ValueError(
                "API key must be agent-scoped. Generate one under "
                "Account → Agents → API Keys on lmheads.ai."
            )
        agent_id = me["agent_id"]
        agent_name = me.get("agent_name", "?")
        logger.info("listening as %s (%s) on %s", agent_name, agent_id, base)

        consumer = _Consumer(http=http, base=base, agent_id=agent_id, handler=handler)
        await consumer.run(reconnect_max=reconnect_max_seconds)


class _Consumer:
    def __init__(
        self,
        *,
        http: httpx.AsyncClient,
        base: str,
        agent_id: str,
        handler: DefaultRequestHandler,
    ) -> None:
        self.http = http
        self.base = base
        self.agent_id = agent_id
        self.handler = handler
        self._workers: dict[str, asyncio.Task] = {}
        # message_ids of inbound user messages we've already started
        # processing. The broker fires a task.update both when a new
        # caller message lands AND when we POST our own /respond — without
        # a per-message dedup the echo would re-trigger the executor.
        self._processed_user_msgs: set[str] = set()

    async def run(self, *, reconnect_max: float) -> None:
        backoff = 1.0
        while True:
            try:
                async for event in self._sse_events():
                    self._dispatch(event)
                # Stream closed cleanly (server shutdown / TTL?). Reconnect.
                logger.info("SSE stream closed; reconnecting")
                backoff = 1.0
            except asyncio.CancelledError:
                for task in self._workers.values():
                    task.cancel()
                raise
            except Exception as e:  # noqa: BLE001 — reconnect on any transport hiccup
                logger.warning("SSE error: %s; reconnecting in %.1fs", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, reconnect_max)

    async def _sse_events(self) -> AsyncIterator[dict]:
        url = f"{self.base}/api/v1/a2a/agents/{self.agent_id}/events"
        # SSE stream needs an indefinite read timeout — events are
        # heartbeated every 25s but data arrives only when there's work.
        sse_timeout = httpx.Timeout(connect=30.0, read=None, write=30.0, pool=30.0)
        async with aconnect_sse(self.http, "GET", url, timeout=sse_timeout) as source:
            async for sse in source.aiter_sse():
                if not sse.data:
                    continue
                try:
                    yield json.loads(sse.data)
                except json.JSONDecodeError:
                    logger.warning("dropped malformed SSE payload: %r", sse.data[:200])

    def _dispatch(self, evt: dict) -> None:
        kind = evt.get("kind")
        # Visibility into what the broker actually pushed and when. If
        # caller-side buffering is the culprit, inbound events on the
        # agent side will look prompt; if agent-side buffering is the
        # culprit, gaps between caller send and this log will show it.
        logger.info(
            "sse <- kind=%s task=%s state=%s",
            kind,
            (evt.get("task_id") or "")[:8] or "?",
            evt.get("state", ""),
        )
        if kind == "snapshot":
            for summary in evt.get("tasks") or []:
                state = (summary.get("State") or summary.get("state") or "").lower()
                if state in _TERMINAL_WIRE:
                    continue
                tid = summary.get("ID") or summary.get("id")
                if tid:
                    self._spawn_processing(tid)
        elif kind in ("task.created", "task.update"):
            tid = evt.get("task_id")
            if not tid:
                return
            state = (evt.get("state") or "").lower()
            if state == "canceled":
                self._cancel_remote(tid)
                return
            if state in _TERMINAL_WIRE:
                return  # remote already done
            self._spawn_processing(tid)
        elif kind == "task.expired":
            tid = evt.get("task_id")
            if tid:
                self._cancel_remote(tid)

    def _spawn_processing(self, task_id: str) -> None:
        running = self._workers.get(task_id)
        if running is not None and not running.done():
            # Already busy on this task — the executor will run to completion
            # before another invocation. The new event will trigger a fresh
            # invocation when this one finishes (next SSE event re-checks).
            return

        async def runner() -> None:
            try:
                await self._process(task_id)
            except asyncio.CancelledError:
                logger.info("worker for %s cancelled", task_id[:8])
                raise
            except Exception:
                logger.exception("worker for %s failed", task_id[:8])
            finally:
                self._workers.pop(task_id, None)

        self._workers[task_id] = asyncio.create_task(
            runner(), name=f"lmh-task-{task_id[:8]}"
        )

    def _cancel_remote(self, task_id: str) -> None:
        worker = self._workers.get(task_id)
        if worker is not None and not worker.done():
            worker.cancel()

        async def notify_handler() -> None:
            try:
                await self.handler.on_cancel_task(
                    CancelTaskRequest(id=task_id), _ctx()
                )
            except Exception as e:  # noqa: BLE001
                logger.debug("handler.on_cancel_task(%s) raised: %s", task_id[:8], e)

        asyncio.create_task(notify_handler(), name=f"lmh-cancel-{task_id[:8]}")

    async def _process(self, task_id: str) -> None:
        full = await self._tasks_get(task_id)
        if full is None:
            return

        history = list(full.get("history") or [])
        context_id = full.get("contextId") or full.get("context_id") or ""

        # The lmheads server stores follow-up caller messages as the
        # task's `status.message` (via pushFollowupViaHub → SDK
        # TaskStatusUpdateEvent), NOT as a History entry. The SDK's
        # consumer only moves status.message into history on the NEXT
        # status update — so a fresh tasks/get after caller sends N
        # has the new message in status.message and history still ends
        # at N-1. If we only walk history, we miss N until N+1 arrives
        # and shifts it back. Treat status.message as the chronological
        # tail so the latest user turn is always visible.
        status = full.get("status") or {}
        latest = status.get("message")
        if latest:
            latest_id = latest.get("messageId") or latest.get("message_id") or ""
            already_in_history = any(
                (m.get("messageId") or m.get("message_id") or "") == latest_id
                for m in history
            )
            if not already_in_history:
                history.append(latest)

        # Walk history in chronological order and process every user
        # message we haven't seen yet. Earlier passes only responded
        # to the latest, which silently dropped middle messages when
        # the caller sent N+1 before the executor finished N.
        for i, m in enumerate(history):
            if (m.get("role") or "").lower() != "user":
                continue
            msg_id = m.get("messageId") or m.get("message_id") or ""
            if not msg_id or msg_id in self._processed_user_msgs:
                continue

            self._processed_user_msgs.add(msg_id)
            prior_history = history[:i]
            prior_task = _build_task_proto(full, prior_history, context_id)
            await self.handler.task_store.save(prior_task, _ctx())

            send_req = SendMessageRequest(
                message=_message_proto(m, task_id, context_id, Role.ROLE_USER),
            )

            try:
                async for event in self.handler.on_message_send_stream(send_req, _ctx()):
                    await self._on_executor_event(event, task_id)
            except BaseException:
                # Roll back the dedup marker so a retry (resend,
                # reconnect, or a later SSE echo) can pick up this
                # turn. We don't want a transient error to silently
                # drop a message.
                self._processed_user_msgs.discard(msg_id)
                raise

    async def _on_executor_event(self, event: Any, task_id: str) -> None:
        if isinstance(event, Task):
            return  # broker is the source of truth for the Task object
        if isinstance(event, Message):
            # lmheads represents agent messages as state-update payloads;
            # a bare Message has no slot in the broker model. The scripted
            # demo doesn't emit these.
            logger.debug("ignoring bare Message event for %s", task_id[:8])
            return
        if isinstance(event, TaskStatusUpdateEvent):
            await self._respond(task_id, event)
            return
        if isinstance(event, TaskArtifactUpdateEvent):
            logger.debug("artifact update event ignored (not yet wired): %s", task_id[:8])
            return

    async def _respond(self, task_id: str, evt: TaskStatusUpdateEvent) -> None:
        wire_state = _STATE_TO_WIRE.get(evt.status.state, "working")
        body: dict[str, Any] = {"state": wire_state}
        if evt.status.HasField("message"):
            parts: list[dict[str, Any]] = []
            for p in evt.status.message.parts:
                if p.text:
                    parts.append({"kind": "text", "text": p.text})
            if parts:
                body["parts"] = parts
        url = f"{self.base}/api/v1/a2a/tasks/{task_id}/respond"
        r = await self.http.post(url, json=body)
        r.raise_for_status()

    async def _tasks_get(self, task_id: str) -> dict | None:
        body = {
            "jsonrpc": "2.0",
            "id": uuid.uuid4().hex,
            "method": "tasks/get",
            "params": {"id": task_id},
        }
        r = await self.http.post(
            f"{self.base}/api/v1/a2a/{self.agent_id}/rpc", json=body
        )
        r.raise_for_status()
        env = r.json()
        if "error" in env:
            logger.warning("tasks/get %s: %s", task_id[:8], env["error"])
            return None
        return env.get("result")


def _ctx() -> ServerCallContext:
    return ServerCallContext()


def _message_proto(
    d: dict, task_id: str, context_id: str, role: int
) -> Message:
    parts: list[Part] = []
    for p in d.get("parts") or []:
        if "text" in p:
            parts.append(Part(text=p["text"]))
    return Message(
        message_id=d.get("messageId") or d.get("message_id") or uuid.uuid4().hex,
        task_id=task_id,
        context_id=context_id,
        role=role,
        parts=parts,
    )


def _build_task_proto(
    d: dict, history: list[dict], context_id: str
) -> Task:
    task = Task(
        id=d.get("id") or "",
        context_id=context_id,
    )
    state_str = ((d.get("status") or {}).get("state") or "").lower()
    task.status.state = _WIRE_TO_STATE.get(state_str, TaskState.TASK_STATE_SUBMITTED)
    for m in history:
        msg = task.history.add()
        msg.message_id = m.get("messageId") or m.get("message_id") or uuid.uuid4().hex
        msg.task_id = task.id
        msg.context_id = context_id
        role_str = (m.get("role") or "").lower()
        msg.role = (
            Role.ROLE_AGENT if role_str == "agent" else Role.ROLE_USER
        )
        for p in m.get("parts") or []:
            pp = msg.parts.add()
            if "text" in p:
                pp.text = p["text"]
    return task
