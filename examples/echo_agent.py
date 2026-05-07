#!/usr/bin/env python3
"""LmHeads demo — echo agent for diagnosing message propagation.

Replies to every inbound user message with `ok, got message "<text>"`
and keeps the task open (state=input_required) so the caller can send
multiple follow-ups. Completes the task only when the user message
contains the literal phrase `finish the task` (case-insensitive).

Useful for stress-testing SSE delivery: send N messages rapidly and
watch whether replies arrive in order with low latency, or whether
they show up only after the next caller message forces a buffer flush.

Setup:
  uv sync
  export LMH_API_KEY=lmh_xxxxxxxx     # agent-scoped key (different agent
                                      # than the security expert; create
                                      # an "echo-agent" on lmheads.ai and
                                      # generate a key for it)
  uv run python echo_agent.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import uuid

from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore
from a2a.types import (
    AgentCapabilities,
    AgentCard,
    Message,
    Part,
    Role,
    TaskState,
    TaskStatus,
    TaskStatusUpdateEvent,
)

from lmheads import lmheads_listen

log = logging.getLogger("echo_agent")
FINISH_PHRASE = "finish the task"


class EchoExecutor(AgentExecutor):
    """One reply per inbound user message; close on `finish the task`."""

    async def execute(self, ctx: RequestContext, queue: EventQueue) -> None:
        incoming = ""
        if ctx.message is not None:
            incoming = " ".join(
                p.text for p in ctx.message.parts if getattr(p, "text", None)
            ).strip()
        log.info("got message: %r", incoming[:200])

        if FINISH_PHRASE in incoming.lower():
            text = (
                f'ok, got message "{incoming}". '
                f"closing the task — bye!"
            )
            await self._emit(queue, ctx, TaskState.TASK_STATE_COMPLETED, text)
        else:
            text = f'ok, got message "{incoming}"'
            await self._emit(
                queue, ctx, TaskState.TASK_STATE_INPUT_REQUIRED, text
            )

    async def cancel(self, ctx: RequestContext, queue: EventQueue) -> None:
        return

    @staticmethod
    async def _emit(
        queue: EventQueue, ctx: RequestContext, state: int, text: str
    ) -> None:
        msg = Message(
            message_id=uuid.uuid4().hex,
            task_id=ctx.task_id or "",
            context_id=ctx.context_id or "",
            role=Role.ROLE_AGENT,
            parts=[Part(text=text)],
        )
        await queue.enqueue_event(
            TaskStatusUpdateEvent(
                task_id=ctx.task_id or "",
                context_id=ctx.context_id or "",
                status=TaskStatus(state=state, message=msg),
            )
        )


async def _amain() -> None:
    api_key = os.environ.get("LMH_API_KEY")
    if not api_key:
        sys.exit("LMH_API_KEY env var required (agent-scoped lmh_ key)")

    base_url = os.environ.get("LMH_BASE_URL", "https://lmheads.ai")
    handler = DefaultRequestHandler(
        agent_executor=EchoExecutor(),
        task_store=InMemoryTaskStore(),
        agent_card=AgentCard(
            name="echo-agent",
            description=(
                "Demo echo agent — replies to every message and stays open "
                "until told 'finish the task'. Used to test SSE propagation."
            ),
            version="0.1.0",
            capabilities=AgentCapabilities(streaming=True),
        ),
    )
    await lmheads_listen(handler, api_key=api_key, base_url=base_url)


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LMH_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
