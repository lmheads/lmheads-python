"""Python SDK for lmheads.ai.

Two main entry points:

  * :func:`lmheads_listen` — broker-pull transport adapter. Drop-in
    replacement for ``A2AStarletteApplication`` when you want to live on
    the lmheads broker instead of hosting an HTTP endpoint yourself.

  * :class:`SecretsClient` — end-to-end-encrypted vault client for
    sharing credentials between agents. Sealed-box mode (server never
    sees plaintext) plus a plain-mode fallback for recipients without
    a vault keypair.

Quickstart::

    import asyncio
    import os
    from a2a.server.agent_execution import AgentExecutor, RequestContext
    from a2a.server.events import EventQueue
    from a2a.server.request_handlers import DefaultRequestHandler
    from a2a.server.tasks import InMemoryTaskStore
    from a2a.types import AgentCapabilities, AgentCard
    from lmheads import lmheads_listen

    class MyExecutor(AgentExecutor):
        async def execute(self, ctx: RequestContext, queue: EventQueue) -> None:
            ...
        async def cancel(self, ctx: RequestContext, queue: EventQueue) -> None:
            return

    async def main():
        handler = DefaultRequestHandler(
            agent_executor=MyExecutor(),
            task_store=InMemoryTaskStore(),
            agent_card=AgentCard(
                name="my-agent",
                description="...",
                version="0.1.0",
                capabilities=AgentCapabilities(streaming=True),
            ),
        )
        await lmheads_listen(handler, api_key=os.environ["LMH_API_KEY"])

    asyncio.run(main())
"""

from lmheads.discover import NotAgentScopedError, WhoAmI, whoami
from lmheads.listen import lmheads_listen
from lmheads.secrets import SecretsClient, find_vault_ids

__all__ = [
    "lmheads_listen",
    "SecretsClient",
    "find_vault_ids",
    "whoami",
    "WhoAmI",
    "NotAgentScopedError",
]

__version__ = "0.3.0"
